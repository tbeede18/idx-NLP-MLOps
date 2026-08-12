"""
CRMLS/Trestle NLP enrichment + semantic search pipeline -- AWS deployment version.

Reads raw listings from S3, enriches with tiered sentiment/regex/semantic signals,
writes enriched Parquet back to S3 (partitioned), and maintains an incremental
FAISS semantic search index in S3.

Runs as a container on AWS Batch or ECS Fargate (see Dockerfile), triggered on a
schedule (e.g. weekly via EventBridge). Authenticates via the IAM role attached
to the compute environment -- no credentials are read or stored in this script.

Required environment variables (set in the Batch job definition / Fargate task definition):
    LISTINGS_S3_BUCKET   S3 bucket holding raw listings + pipeline outputs
    LISTINGS_S3_PREFIX   Prefix for semantic search artifacts (default: semantic_search)
    RAW_LISTINGS_KEY     S3 key for the raw listings CSV (default: raw/synthetic_listings.csv)
    AWS_REGION           AWS region (default: us-east-1)
"""

import io
import re
import hashlib
import numpy as np
import pandas as pd
import boto3
import faiss
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
from sentence_transformers import SentenceTransformer, CrossEncoder

import os

COL = "PublicRemarks"
ID_COL = "ListingKey"
MOD_COL = "ModificationTimestamp"        # adjust to whatever your Trestle schema calls "last updated"
AMBIGUITY_THRESHOLD = 0.2                # VADER compound scores within +/- this trigger DistilBERT

# Config comes from environment variables in production (set via the Batch job definition /
# Fargate task definition) with sensible fallbacks for local/notebook testing.
S3_BUCKET = os.environ.get("LISTINGS_S3_BUCKET", "your-bucket-name")
S3_PREFIX = os.environ.get("LISTINGS_S3_PREFIX", "semantic_search")
RAW_LISTINGS_KEY = os.environ.get("RAW_LISTINGS_KEY", "raw/synthetic_listings.csv")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

EMBED_MODEL_NAME = "all-MiniLM-L6-v2"    # 384-dim, CPU-friendly, no per-call API cost
EMBED_DIM = 384
RERANKER_MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"  # small, only scores a few dozen pairs per query

# How many rows to embed between S3 checkpoint saves. If a Fargate Spot task gets
# reclaimed mid-run, work since the last checkpoint is lost -- not the whole run.
CHECKPOINT_INTERVAL = 2000

# Match spaCy's process count to what the container actually has, instead of a
# hardcoded number that could oversubscribe (and slow down) a smaller task.
NER_N_PROCESS = max(1, os.cpu_count() or 1)

s3 = boto3.client("s3", region_name=AWS_REGION)

if S3_BUCKET == "your-bucket-name":
    raise RuntimeError(
        "LISTINGS_S3_BUCKET is not set -- refusing to run with the placeholder bucket name. "
        "Set it in the Batch job definition / Fargate task definition before deploying."
    )

embed_model = SentenceTransformer(EMBED_MODEL_NAME)
reranker = CrossEncoder(RERANKER_MODEL_NAME)
analyzer = SentimentIntensityAnalyzer()
nlp = spacy.load("en_core_web_sm", disable=["tagger", "parser", "lemmatizer"])

RISK_PATTERNS = [
    r"sold\s+as.is", r"as.is", r"foundation\s+issue", r"water\s+damage",
    r"unpermitted", r"flood\s+zone", r"estate\s+sale", r"needs?\s+tlc",
    r"needs?\s+work", r"fixer", r"short\s+sale", r"reo", r"bank.owned",
    r"mold", r"structural", r"probate",
]
VALUE_PATTERNS = [
    r"recently\s+renovated", r"newly\s+renovated", r"updated\s+kitchen",
    r"remodeled", r"adu", r"accessory\s+dwelling", r"solar\s+panel",
    r"permitted\s+addition", r"new\s+roof", r"new\s+hvac",
    r"move.in\s+ready", r"turnkey", r"smart\s+home",
]
RISK_RE = re.compile("|".join(RISK_PATTERNS), flags=re.IGNORECASE)
VALUE_RE = re.compile("|".join(VALUE_PATTERNS), flags=re.IGNORECASE)

# Negation words checked in a small window before a regex match -- "no mold"
# or "not a fixer" should not count as a risk signal.
NEGATION_WORDS = {"no", "not", "without", "never", "isn't", "wasn't", "n't"}
NEGATION_WINDOW = 3  # words to look back from the start of a match

# Anchor sentences for semantic (embedding-based) risk/value detection.
# These catch paraphrases the regex lexicon misses (e.g. "could use some love"
# instead of "needs work") by comparing cosine similarity against the same
# MiniLM embeddings already computed for semantic search -- no extra model,
# no extra inference cost beyond a handful of one-time anchor embeddings.
RISK_ANCHORS = [
    "this home needs significant repairs and updates",
    "sold as-is with unknown condition",
    "signs of water damage or structural issues",
    "outdated and in need of major renovation",
]
VALUE_ANCHORS = [
    "recently renovated with modern updated finishes",
    "move-in ready turnkey home in excellent condition",
    "features solar panels and energy efficient upgrades",
    "beautifully maintained with high end upgrades",
]
SEMANTIC_SIGNAL_THRESHOLD = 0.45  # cosine similarity above which a semantic match counts

def dedup_remarks(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run every model on unique text only, then join back by hash."""
    df["remark_hash"] = df[COL].fillna("").apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )
    unique_remarks = df.drop_duplicates(subset="remark_hash")[["remark_hash", COL]].reset_index(drop=True)
    print(f"{len(df)} total rows -> {len(unique_remarks)} unique remarks "
          f"({100 * (1 - len(unique_remarks) / len(df)):.1f}% reduction in model work)")
    return df, unique_remarks

def _unnegated_matches(text: str, compiled_re: re.Pattern, window: int = NEGATION_WINDOW) -> int:
    """
    Counts regex matches that aren't preceded by a negation word within
    `window` words. Cheap: still pure string ops on already-deduped text.
    """
    lower = text.lower()
    count = 0
    for m in compiled_re.finditer(lower):
        preceding = lower[:m.start()].split()[-window:]
        if not any(neg in preceding for neg in NEGATION_WORDS):
            count += 1
    return count


def apply_regex_lexicon(unique_remarks: pd.DataFrame) -> pd.DataFrame:
    texts = unique_remarks[COL].fillna("")

    # Severity counts: how many times each pattern type fires, not just whether it fired.
    unique_remarks["risk_count_raw"] = texts.apply(lambda x: _unnegated_matches(x, RISK_RE))
    unique_remarks["value_count_raw"] = texts.apply(lambda x: _unnegated_matches(x, VALUE_RE))

    # Boolean flags derived from the negation-aware counts instead of raw str.contains,
    # so "no mold" / "not a fixer" no longer register as risk signals.
    unique_remarks["risk_signals"] = unique_remarks["risk_count_raw"] > 0
    unique_remarks["value_signals"] = unique_remarks["value_count_raw"] > 0
    return unique_remarks

def apply_semantic_signals(unique_remarks: pd.DataFrame) -> pd.DataFrame:
    """
    Catches paraphrased risk/value language the regex lexicon misses, by
    comparing each remark's embedding to a small set of anchor sentences.
    Reuses the same `embed_model` already loaded for semantic search --
    the only new cost is embedding the anchors once (a handful of sentences)
    and embedding the deduped unique remarks (which you'd likely want
    cached/reused for the search index anyway).
    """
    texts = unique_remarks[COL].fillna("").tolist()
    remark_vecs = embed_model.encode(texts, batch_size=256, convert_to_numpy=True, show_progress_bar=True)

    risk_anchor_vecs = embed_model.encode(RISK_ANCHORS, convert_to_numpy=True)
    value_anchor_vecs = embed_model.encode(VALUE_ANCHORS, convert_to_numpy=True)

    def cosine_sim_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        a_norm = a / np.linalg.norm(a, axis=1, keepdims=True)
        b_norm = b / np.linalg.norm(b, axis=1, keepdims=True)
        return a_norm @ b_norm.T

    risk_sims = cosine_sim_matrix(remark_vecs, risk_anchor_vecs)   # (n_remarks, n_risk_anchors)
    value_sims = cosine_sim_matrix(remark_vecs, value_anchor_vecs)

    unique_remarks["semantic_risk_score"] = risk_sims.max(axis=1).round(4)
    unique_remarks["semantic_value_score"] = value_sims.max(axis=1).round(4)
    unique_remarks["semantic_risk_signal"] = unique_remarks["semantic_risk_score"] > SEMANTIC_SIGNAL_THRESHOLD
    unique_remarks["semantic_value_signal"] = unique_remarks["semantic_value_score"] > SEMANTIC_SIGNAL_THRESHOLD

    # Cache remark_vecs for reuse by the search section instead of re-encoding
    # the same unique remarks a second time.
    unique_remarks.attrs["remark_vecs"] = remark_vecs
    return unique_remarks

def build_sentiment_pipe(quantized: bool = False):
    if not quantized:
        return pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            truncation=True,
            max_length=512,
        )
    # Optional ONNX + INT8 quantized path -- faster CPU inference for tier-2 volume.
    from optimum.onnxruntime import ORTModelForSequenceClassification, ORTQuantizer
    from optimum.onnxruntime.configuration import AutoQuantizationConfig
    from transformers import AutoTokenizer

    model_id = "distilbert-base-uncased-finetuned-sst-2-english"
    onnx_path = "./distilbert_onnx"
    model = ORTModelForSequenceClassification.from_pretrained(model_id, export=True)
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model.save_pretrained(onnx_path)
    tokenizer.save_pretrained(onnx_path)

    quantizer = ORTQuantizer.from_pretrained(onnx_path)
    qconfig = AutoQuantizationConfig.avx512_vnni(is_static=False, per_channel=False)
    quantizer.quantize(save_dir=onnx_path, quantization_config=qconfig)

    quantized_model = ORTModelForSequenceClassification.from_pretrained(onnx_path, file_name="model_quantized.onnx")
    return pipeline("sentiment-analysis", model=quantized_model, tokenizer=tokenizer, truncation=True, max_length=512)


def apply_tiered_sentiment(unique_remarks: pd.DataFrame, sentiment_pipe) -> pd.DataFrame:
    """VADER on everything (cheap); DistilBERT only where VADER is ambiguous."""
    unique_remarks["vader_compound"] = unique_remarks[COL].fillna("").apply(
        lambda x: analyzer.polarity_scores(x)["compound"]
    )

    ambiguous_mask = unique_remarks["vader_compound"].abs() < AMBIGUITY_THRESHOLD
    ambiguous_rows = unique_remarks[ambiguous_mask]
    print(f"Running DistilBERT on {len(ambiguous_rows)}/{len(unique_remarks)} "
          f"ambiguous rows ({100 * len(ambiguous_rows) / max(len(unique_remarks), 1):.1f}%).")

    unique_remarks["distilbert_label"] = None
    unique_remarks["distilbert_score"] = np.nan
    if len(ambiguous_rows) > 0:
        bert_results = sentiment_pipe(ambiguous_rows[COL].fillna("").tolist(), batch_size=64)
        unique_remarks.loc[ambiguous_mask, "distilbert_label"] = [r["label"] for r in bert_results]
        unique_remarks.loc[ambiguous_mask, "distilbert_score"] = [r["score"] for r in bert_results]
    return unique_remarks

def apply_entity_extraction(unique_remarks: pd.DataFrame) -> pd.DataFrame:
    texts = unique_remarks[COL].fillna("").tolist()
    all_entities = []
    for doc in nlp.pipe(texts, batch_size=200, n_process=NER_N_PROCESS):
        entities = {}
        for ent in doc.ents:
            entities.setdefault(ent.label_, []).append(ent.text)
        all_entities.append(entities)
    unique_remarks["entities"] = all_entities
    return unique_remarks

def bucket_sentiment(score: float) -> str:
    if score >= 0.5:
        return "very_positive"
    elif score >= 0.2:
        return "positive"
    elif score > -0.2:
        return "neutral"
    elif score > -0.5:
        return "negative"
    else:
        return "very_negative"


def feature_engineer_undervalue(df: pd.DataFrame) -> pd.DataFrame:
    # Combine symbolic (regex) counts with semantic (embedding) signals -- a
    # remark can now register as risky/valuable even if it uses phrasing the
    # regex lexicon doesn't literally contain.
    df["risk_count"] = df["risk_count_raw"].fillna(0) + df["semantic_risk_signal"].fillna(False).astype(int)
    df["value_count"] = df["value_count_raw"].fillna(0) + df["semantic_value_signal"].fillna(False).astype(int)
    df["signal_net"] = df["value_count"] - df["risk_count"]
    df["has_risk"] = df["risk_count"] > 0
    df["has_value_add"] = df["value_count"] > 0
    df["positive_with_risk"] = (df["vader_compound"] > 0.2) & (df["risk_count"] > 0)
    df["sentiment_bucket"] = df["vader_compound"].apply(bucket_sentiment)
    df["remark_word_count"] = df[COL].str.split().str.len()
    df["exclamation_count"] = df[COL].str.count(r"!")
    df["all_caps_word_count"] = df[COL].apply(
        lambda x: sum(1 for w in str(x).split() if w.isupper() and len(w) > 2)
    )

    # Guard against missing/zero LivingArea, which real CRMLS data will have --
    # unguarded division produces inf/-inf that silently corrupts every score
    # downstream (zscore, percentile, undervalue_score).
    df["price_per_sqft"] = np.where(
        df["LivingArea"].fillna(0) > 0, df["ListPrice"] / df["LivingArea"], np.nan
    )
    df["peer_group"] = df["PostalCode"].astype(str) + "_" + df["PropertySubType"]

    # Built-in vectorized transforms ("mean"/"std") instead of a Python lambda per
    # group -- pandas runs these at the C level, meaningfully faster once you're
    # grouping hundreds of thousands of rows into thousands of peer groups.
    grp = df.groupby("peer_group")["price_per_sqft"]
    group_mean = grp.transform("mean")
    group_std = grp.transform("std")
    df["price_per_sqft_zscore"] = ((df["price_per_sqft"] - group_mean) / group_std).fillna(0)
    df["price_percentile"] = grp.rank(pct=True)

    value_norm = df["value_count"].clip(0, 5) / 5
    risk_penalty = df["risk_count"].clip(0, 5) / 5
    sentiment_norm = (df["vader_compound"] + 1) / 2

    df["listing_quality_score_v2"] = (
        (0.35 * sentiment_norm) + (0.45 * value_norm) - (0.20 * risk_penalty)
    ).clip(0, 1).round(4)

    df["price_discount"] = 1 - df["price_percentile"]
    df["undervalue_score"] = (df["listing_quality_score_v2"] * df["price_discount"]).round(4)

    df["undervalue_candidate"] = (
        (df["undervalue_score"] > df["undervalue_score"].quantile(0.75)) &
        (df["risk_count"] == 0)
    )
    df["hidden_value_candidate"] = (
        (df["vader_compound"] > 0.3) &
        (df["price_percentile"] < 0.35) &
        (df["value_count"] == 0) &
        (df["risk_count"] == 0)
    )
    return df

def downcast_for_storage(df: pd.DataFrame) -> pd.DataFrame:
    float_cols = df.select_dtypes(include=["float64"]).columns
    df[float_cols] = df[float_cols].astype("float32")
    int_cols = df.select_dtypes(include=["int64"]).columns
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], downcast="integer")
    return df


def write_partitioned_parquet(df: pd.DataFrame, base_path: str, partition_col: str = "peer_group"):
    df.to_parquet(
        base_path,
        engine="pyarrow",
        partition_cols=[partition_col],
        compression="snappy",
        index=False,
    )

def load_manifest() -> pd.DataFrame:
    key = f"{S3_PREFIX}/manifest.parquet"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame(columns=[ID_COL, MOD_COL, "row_idx"])


def load_embedding_matrix() -> np.ndarray:
    key = f"{S3_PREFIX}/embeddings.npy"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return np.load(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return np.empty((0, EMBED_DIM), dtype=np.float16)


def save_embedding_matrix(embeddings: np.ndarray):
    buf = io.BytesIO()
    np.save(buf, embeddings.astype(np.float16))
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/embeddings.npy", Body=buf.getvalue())


def save_manifest(manifest: pd.DataFrame):
    buf = io.BytesIO()
    manifest.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/manifest.parquet", Body=buf.getvalue())

def embed_incremental(df: pd.DataFrame, batch_size: int = 256) -> tuple[np.ndarray, pd.DataFrame, np.ndarray]:
    """
    Only embeds rows that are new or changed since the last run. Processes in
    CHECKPOINT_INTERVAL-sized chunks, updating the embeddings matrix and
    manifest with vectorized array/pandas operations rather than a per-row
    Python loop -- the previous per-row version filtered the entire manifest
    and copied the entire embeddings array on every single row, which is O(n^2)
    and gets very expensive as the corpus grows (e.g. a 25k-row weekly delta
    against a 500k-row manifest was doing tens of billions of comparisons).

    Checkpoints to S3 after each chunk so a Spot interruption loses at most one
    chunk's worth of work.

    Returns (full_embeddings_matrix, manifest, new_vecs) where new_vecs is the
    array of vectors for genuinely new rows only (not updated ones -- HNSW
    can't update in place, so changed listings get their new embedding stored
    in the matrix but only pick up the fresh vector in the search index at the
    next full rebuild; see the note on periodic FAISS rebuilds).
    """
    manifest = load_manifest()
    embeddings = load_embedding_matrix()

    if manifest.empty:
        to_embed = df.copy()
        to_embed["row_idx"] = -1
    else:
        merged = df.merge(
            manifest[[ID_COL, MOD_COL, "row_idx"]],
            on=ID_COL, how="left", suffixes=("", "_prev")
        )
        is_new = merged["row_idx"].isna()
        is_changed = (~is_new) & (merged[MOD_COL] > merged[f"{MOD_COL}_prev"])
        to_embed = merged[is_new | is_changed].copy()
        to_embed["row_idx"] = to_embed["row_idx"].fillna(-1)

    if len(to_embed) == 0:
        print("No new or changed listings -- skipping embedding step entirely.")
        return embeddings, manifest, np.empty((0, EMBED_DIM), dtype=np.float16)

    to_embed = to_embed.reset_index(drop=True)
    print(f"Embedding {len(to_embed)} new/changed rows out of {len(df)} total.")
    all_new_vecs = embed_model.encode(
        to_embed[COL].fillna("").tolist(),
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    ).astype(np.float16)

    genuinely_new_vecs = []

    for chunk_start in range(0, len(to_embed), CHECKPOINT_INTERVAL):
        chunk = to_embed.iloc[chunk_start: chunk_start + CHECKPOINT_INTERVAL]
        chunk_vecs = all_new_vecs[chunk_start: chunk_start + CHECKPOINT_INTERVAL]

        row_idx_vals = chunk["row_idx"].to_numpy()
        update_mask = row_idx_vals != -1
        new_mask = ~update_mask

        # Vectorized in-place update for changed listings -- single fancy-index
        # assignment instead of one assignment per row.
        if update_mask.any():
            embeddings[row_idx_vals[update_mask].astype(int)] = chunk_vecs[update_mask]

        # Single vstack per chunk (not per row) for brand-new listings.
        n_new = int(new_mask.sum())
        if n_new:
            start_idx = embeddings.shape[0]
            embeddings = np.vstack([embeddings, chunk_vecs[new_mask]])
            new_indices = np.arange(start_idx, start_idx + n_new)
            genuinely_new_vecs.append(chunk_vecs[new_mask])
        else:
            new_indices = np.array([], dtype=int)

        row_idx_assignment = np.empty(len(chunk), dtype=int)
        row_idx_assignment[update_mask] = row_idx_vals[update_mask].astype(int)
        row_idx_assignment[new_mask] = new_indices

        chunk_manifest = pd.DataFrame({
            ID_COL: chunk[ID_COL].to_numpy(),
            MOD_COL: chunk[MOD_COL].to_numpy(),
            "row_idx": row_idx_assignment,
        })
        # One vectorized filter for the whole chunk instead of one filter per row.
        manifest = manifest[~manifest[ID_COL].isin(chunk_manifest[ID_COL])]
        manifest = pd.concat([manifest, chunk_manifest], ignore_index=True)

        print(f"Checkpoint: {min(chunk_start + CHECKPOINT_INTERVAL, len(to_embed))}/{len(to_embed)} rows processed")
        save_embedding_matrix(embeddings)
        save_manifest(manifest)

    new_vecs = (
        np.concatenate(genuinely_new_vecs) if genuinely_new_vecs
        else np.empty((0, EMBED_DIM), dtype=np.float16)
    )
    return embeddings, manifest, new_vecs


def build_faiss_index(embeddings: np.ndarray) -> faiss.Index:
    """Full (re)build from scratch -- used only on cold start or a deliberate periodic rebuild."""
    vecs = np.ascontiguousarray(embeddings.astype(np.float32))
    faiss.normalize_L2(vecs)
    index = faiss.IndexHNSWFlat(EMBED_DIM, 32)
    index.hnsw.efConstruction = 100
    index.add(vecs)
    return index


def update_or_build_faiss_index(new_vecs: np.ndarray, embeddings: np.ndarray) -> faiss.Index:
    """
    Loads the persisted index and adds only the newly embedded vectors, instead
    of rebuilding from the full embedding matrix every run -- cost and runtime
    then scale with the weekly delta, not the size of the whole corpus.

    Falls back to a full rebuild if no index exists yet (first run). HNSW
    doesn't support efficient deletion, so if listings are ever removed (not
    just added/updated) or the graph has been incrementally added to for many
    months, periodically force a full rebuild via build_faiss_index(embeddings)
    to keep search quality from drifting -- e.g. on a monthly schedule.
    """
    try:
        index = load_faiss_index()
        print("Loaded existing FAISS index from S3 -- adding incremental vectors only.")
    except Exception:
        print("No existing index found in S3 -- building from the full embedding matrix (cold start).")
        return build_faiss_index(embeddings)

    if len(new_vecs) > 0:
        vecs = np.ascontiguousarray(new_vecs.astype(np.float32))
        faiss.normalize_L2(vecs)
        index.add(vecs)
    return index


def save_faiss_index(index: faiss.Index):
    faiss.write_index(index, "/tmp/listings.index")
    s3.upload_file("/tmp/listings.index", S3_BUCKET, f"{S3_PREFIX}/listings.index")


def load_faiss_index() -> faiss.Index:
    s3.download_file(S3_BUCKET, f"{S3_PREFIX}/listings.index", "/tmp/listings.index")
    return faiss.read_index("/tmp/listings.index")


def search(query: str, df: pd.DataFrame, index: faiss.Index, top_k: int = 5, rerank_candidates: int = 25) -> pd.DataFrame:
    """
    Retrieve-then-rerank: FAISS pulls a wider candidate pool cheaply (cosine
    similarity over the whole corpus), then a cross-encoder -- which scores
    the query against each candidate directly and is meaningfully more
    accurate than embedding similarity alone -- reranks just that small pool.
    Cost stays flat regardless of corpus size since the cross-encoder only
    ever sees `rerank_candidates` pairs, never the full listing set.
    """
    query_vec = embed_model.encode([query]).astype(np.float32)
    faiss.normalize_L2(query_vec)
    scores, indices = index.search(query_vec, rerank_candidates)
    indices, scores = indices[0], scores[0]
    valid = indices >= 0

    candidates = df.iloc[indices[valid]][[ID_COL, COL, "ListPrice", "MLSAreaMajor"]].copy()
    candidates["faiss_score"] = scores[valid].round(4)

    pairs = [[query, remark] for remark in candidates[COL].fillna("").tolist()]
    candidates["rerank_score"] = reranker.predict(pairs)

    return candidates.sort_values("rerank_score", ascending=False).head(top_k)


def run_pipeline():

    # Reads the raw listings pull from S3. In production this is your full CRMLS/Trestle extract,
    # landed by an upstream ingestion job -- not the 200-row synthetic set.
    obj = s3.get_object(Bucket=S3_BUCKET, Key=RAW_LISTINGS_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    # --- NLP enrichment ---
    df, unique_remarks = dedup_remarks(df)
    unique_remarks = apply_regex_lexicon(unique_remarks)
    unique_remarks = apply_semantic_signals(unique_remarks)

    sentiment_pipe = build_sentiment_pipe(quantized=False)  # set True once you have validated ONNX output
    unique_remarks = apply_tiered_sentiment(unique_remarks, sentiment_pipe)
    unique_remarks = apply_entity_extraction(unique_remarks)

    model_cols = ["remark_hash", "risk_signals", "value_signals",
                  "risk_count_raw", "value_count_raw",
                  "semantic_risk_score", "semantic_value_score",
                  "semantic_risk_signal", "semantic_value_signal",
                  "vader_compound", "distilbert_label", "distilbert_score", "entities"]
    df = df.merge(unique_remarks[model_cols], on="remark_hash", how="left")

    df = feature_engineer_undervalue(df)
    df = downcast_for_storage(df)

    # Writes enriched output to S3, partitioned by peer_group for cheaper Athena scans.
    write_partitioned_parquet(df, f"s3://{S3_BUCKET}/enriched_listings")

    print(df[df["undervalue_candidate"]][
        ["ListingKey", "ListPrice", "price_per_sqft", "price_percentile",
         "listing_quality_score_v2", "undervalue_score",
         "undervalue_candidate", "value_signals"]
    ].sort_values("undervalue_score", ascending=False).head(10))

    # --- Semantic search (AWS-scaled) ---
    embeddings, manifest, new_vecs = embed_incremental(df)
    index = update_or_build_faiss_index(new_vecs, embeddings)
    save_faiss_index(index)

    # Test queries only run when explicitly requested (e.g. during manual validation) --
    # left unguarded, this was running three reranked searches on every scheduled
    # production run for no operational purpose.
    if os.environ.get("RUN_TEST_QUERIES", "false").lower() == "true":
        test_queries = [
            "move-in ready condo with updated kitchen",
            "fixer upper with foundation issues sold as-is",
            "ADU or in-law suite with separate entrance",
        ]
        for q in test_queries:
            print(f"\nQuery: '{q}'")
            print(search(q, df, index).to_string(index=False))

def main():
    run_pipeline()


if __name__ == "__main__":
    main()
