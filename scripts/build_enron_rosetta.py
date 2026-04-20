#!/usr/bin/env python3
"""Build full Rosetta artifacts from the fetched Enron source data."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import tarfile
from email.parser import BytesHeaderParser
from email.utils import parseaddr, parsedate_to_datetime
from datetime import UTC
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from vei.whatif._enron_dataset import load_enron_full_dataset_release

HF_PARQUET_URLS = [
    "https://huggingface.co/datasets/corbt/enron-emails/resolve/main/data/train-00000-of-00003.parquet",
    "https://huggingface.co/datasets/corbt/enron-emails/resolve/main/data/train-00001-of-00003.parquet",
    "https://huggingface.co/datasets/corbt/enron-emails/resolve/main/data/train-00002-of-00003.parquet",
]

RE_LIST_TAG = re.compile(r"^\s*\[[^\]]+\]\s*")
RE_REPLY_PREFIX = re.compile(r"^\s*(re)\s*:\s*", re.IGNORECASE)
RE_FORWARD_PREFIX = re.compile(r"^\s*(fw|fwd)\s*:\s*", re.IGNORECASE)
RE_WHITESPACE = re.compile(r"\s+")
RE_EMAIL_IN_BRACKETS = re.compile(r"<([^>]+)>")
RE_SPLIT_RECIP = re.compile(r"[;,]")

RE_ASSIGNMENT = re.compile(
    r"\b(?:please|can you|could you|need you to|action required|assign(?:ed|ment)?|take care of|follow up)\b",
    re.IGNORECASE,
)
RE_APPROVAL = re.compile(
    r"\b(?:approve|approval|sign[- ]off|authorize|ok(?:ay)? to|consent)\b",
    re.IGNORECASE,
)
RE_LEGAL = re.compile(
    r"\b(?:legal|counsel|attorney|litigation|contract|compliance|ferc|regulatory)\b",
    re.IGNORECASE,
)
RE_TRADING = re.compile(
    r"\b(?:trade|trading|market|bid|offer|position|hedge|gas|power|enron online)\b",
    re.IGNORECASE,
)
RE_ATTACHMENT = re.compile(
    r"\b(?:attachment|attached|see attached|enclosed)\b", re.IGNORECASE
)

STATE_MAP = {
    "message": "opened",
    "message_reply": "active",
    "message_forward": "active",
    "assignment": "assigned",
    "approval": "approved",
    "escalation": "escalated",
}


def parse_args() -> argparse.Namespace:
    release = load_enron_full_dataset_release()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-cache",
        type=Path,
        default=release.resolved_source_dir / "enron_rosetta_source.parquet",
        help="Local source cache with recipient metadata + body snippet.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=release.resolved_rosetta_dir,
        help="Output directory for Rosetta artifacts.",
    )
    parser.add_argument("--start-year", type=int, default=1997)
    parser.add_argument("--end-year", type=int, default=2003)
    parser.add_argument(
        "--body-snippet-chars",
        type=int,
        default=2000,
        help="Snippet length retained from body for classification and optional content field.",
    )
    parser.add_argument(
        "--include-content",
        action="store_true",
        help="Also export content-filled event table with body snippet in content.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap for debugging; 0 means all rows.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore source cache and pull from source parquet URLs again.",
    )
    parser.add_argument(
        "--source-max-attempts",
        type=int,
        default=3,
        help="Retry attempts for remote source pull (handles transient HTTP 429/5xx).",
    )
    parser.add_argument(
        "--local-tar-path",
        type=Path,
        default=release.resolved_raw_dir / "enron_mail_20150507.tar.gz",
        help="Local Enron tarball fallback when remote source is unavailable.",
    )
    parser.add_argument(
        "--prefer-local-source",
        action="store_true",
        help="Skip remote pull and build source cache from local tarball.",
    )
    return parser.parse_args()


def normalize_subject(subject: str) -> str:
    if not isinstance(subject, str):
        return ""
    s = subject.strip()
    if not s:
        return ""
    while True:
        updated = RE_LIST_TAG.sub("", s)
        updated = RE_REPLY_PREFIX.sub("", updated)
        updated = RE_FORWARD_PREFIX.sub("", updated)
        updated = RE_WHITESPACE.sub(" ", updated).strip().lower()
        if updated == s:
            break
        s = updated
    return s


def clean_email_token(value: str) -> str:
    token = value.strip().strip('"').strip("'")
    if not token:
        return ""
    if token.lower().startswith("mailto:"):
        token = token[7:]
    bracket_match = RE_EMAIL_IN_BRACKETS.search(token)
    if bracket_match:
        token = bracket_match.group(1)
    token = token.strip().lower()
    token = token.replace(" ", "")
    return token


def parse_recipient_list(value: Any) -> list[str]:
    if value is None:
        return []
    tokens: list[str] = []
    if isinstance(value, (list, tuple, np.ndarray, pd.Series)):
        for item in value:
            if item is None:
                continue
            item_str = str(item).strip()
            if not item_str:
                continue
            tokens.extend(RE_SPLIT_RECIP.split(item_str))
    else:
        tokens.extend(RE_SPLIT_RECIP.split(str(value)))
    cleaned = [clean_email_token(tok) for tok in tokens]
    cleaned = [tok for tok in cleaned if tok and tok not in {"", "none", "null", "[]"}]
    return sorted(set(cleaned))


def dedupe_targets(
    actor_id: str, to_list: list[str], cc_list: list[str], bcc_list: list[str]
) -> list[str]:
    merged = []
    seen: set[str] = set()
    for target in to_list + cc_list + bcc_list:
        if target == actor_id:
            continue
        if target in seen:
            continue
        seen.add(target)
        merged.append(target)
    return merged


def hash_short(value: str) -> str:
    return hashlib.sha1(  # noqa: S324
        value.encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()[:16]


def make_target_id(targets: list[str], fallback_thread_id: str) -> str:
    if len(targets) == 1:
        return targets[0]
    if len(targets) > 1:
        group_key = ";".join(sorted(targets))
        return f"group:{hash_short(group_key)}"
    return f"thread:{fallback_thread_id}"


def classify_event(row: pd.Series) -> dict[str, Any]:
    subject = str(row["subject"] or "")
    body = str(row["body_snippet"] or "")
    subject_lower = subject.lower()
    text = f"{subject}\n{body}"

    is_reply = bool(RE_REPLY_PREFIX.search(subject))
    is_forward = bool(RE_FORWARD_PREFIX.search(subject)) or (
        "forwarded by" in text.lower()
    )
    assignment_hit = bool(RE_ASSIGNMENT.search(text))
    approval_hit = bool(RE_APPROVAL.search(text))
    legal_hit = bool(RE_LEGAL.search(text))
    trading_hit = bool(RE_TRADING.search(text))

    cc_count = int(row["cc_count"])
    bcc_count = int(row["bcc_count"])
    to_count = int(row["to_count"])
    consult_legal = legal_hit and (cc_count > 0 or "please advise" in text.lower())
    consult_trading = trading_hit and (
        cc_count > 0 or "position" in text.lower() or "bid" in text.lower()
    )
    is_escalation = bool(
        is_forward
        or cc_count > 0
        or bcc_count > 0
        or (to_count >= 4)
        or consult_legal
        or consult_trading
    )

    if approval_hit:
        event_type = "approval"
    elif assignment_hit:
        event_type = "assignment"
    elif is_escalation:
        event_type = "escalation"
    elif is_reply:
        event_type = "message_reply"
    elif is_forward:
        event_type = "message_forward"
    else:
        event_type = "message"

    return {
        "event_type": event_type,
        "is_reply": is_reply,
        "is_forward": is_forward,
        "assignment_hit": assignment_hit,
        "approval_hit": approval_hit,
        "consult_legal_specialist": consult_legal,
        "consult_trading_specialist": consult_trading,
        "is_escalation": is_escalation,
        "has_attachment_reference": bool(RE_ATTACHMENT.search(text)),
        "subject_lower": subject_lower,
    }


def compute_thread_ids(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(index=df.index, dtype="object")

    work = df[["message_id", "norm_subject", "sent_ts"]].copy()
    work = work.sort_values(["norm_subject", "sent_ts", "message_id"]).reset_index()
    work["thread_segment"] = 0

    has_subject = work["norm_subject"] != ""
    if has_subject.any():
        subject_part = work.loc[has_subject, ["norm_subject", "sent_ts"]].copy()
        day_gap = (
            subject_part.groupby("norm_subject")["sent_ts"]
            .diff()
            .dt.total_seconds()
            .div(86400.0)
        )
        split = (day_gap.fillna(0.0) > 90.0).astype(int)
        work.loc[has_subject, "thread_segment"] = (
            split.groupby(subject_part["norm_subject"]).cumsum().to_numpy()
        )

    def build_thread_key(row: pd.Series) -> str:
        if row["norm_subject"]:
            base = f"{row['norm_subject']}|{int(row['thread_segment'])}"
        else:
            base = f"mid|{row['message_id']}"
        return f"thr_{hash_short(base)}"

    work["thread_task_id"] = work.apply(build_thread_key, axis=1)
    work = work.set_index("index")
    return work.loc[df.index, "thread_task_id"]


def load_source_df(args: argparse.Namespace) -> pd.DataFrame:
    if args.source_cache.exists() and not args.force_refresh:
        return pd.read_parquet(args.source_cache)

    if args.prefer_local_source:
        return build_source_from_local_tar(args)

    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "duckdb is required to build source cache. Use the project venv "
            "(e.g., ./.venv/bin/python) where duckdb is installed."
        ) from exc

    args.source_cache.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    urls_sql = ", ".join(f"'{u}'" for u in HF_PARQUET_URLS)
    limit_sql = f"LIMIT {int(args.max_rows)}" if args.max_rows > 0 else ""
    query_lines = [
        "WITH src AS (",
        "    SELECT",
        "        lower(trim(message_id)) AS message_id,",
        "        coalesce(subject, '') AS subject,",
        '        lower(trim("from")) AS from_email,',
        '        "to" AS to_raw,',
        "        cc AS cc_raw,",
        "        bcc AS bcc_raw,",
        "        CAST(date AS TIMESTAMP) AS sent_ts,",
        (
            "        left(coalesce(body, ''), "
            f"{int(args.body_snippet_chars)}) AS body_snippet,"
        ),
        "        file_name",
        f"    FROM read_parquet([{urls_sql}], union_by_name=true)",
        ")",
        "SELECT",
        "    message_id,",
        "    subject,",
        "    from_email,",
        "    to_raw,",
        "    cc_raw,",
        "    bcc_raw,",
        "    sent_ts,",
        "    body_snippet,",
        "    file_name,",
        "    lower(split_part(file_name, '/', 1)) AS custodian_id,",
        "    lower(split_part(file_name, '/', 2)) AS folder",
        "FROM src",
        "WHERE sent_ts IS NOT NULL",
        "  AND file_name IS NOT NULL",
        "  AND split_part(file_name, '/', 1) <> ''",
        f"  AND year(sent_ts) BETWEEN {int(args.start_year)} AND {int(args.end_year)}",
    ]
    if limit_sql:
        query_lines.append(limit_sql)
    query = "\n".join(query_lines)  # nosec B608 - fixed URLs plus bounded ints.
    # Writing directly to parquet avoids large in-memory transfers via fetch_df.
    copy_sql = f"COPY ({query}) TO '{args.source_cache.as_posix()}' (FORMAT PARQUET);"
    attempts = max(1, int(args.source_max_attempts))
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            con.execute(copy_sql)
            last_error = None
            break
        except Exception as exc:  # pragma: no cover - network-dependent path
            last_error = exc
            if attempt >= attempts:
                con.close()
                raise
            sleep_sec = (2**attempt) + float(np.random.uniform(0, 1))
            print(
                f"source_pull_retry attempt={attempt}/{attempts} sleep_sec={sleep_sec:.2f} err={str(exc)[:220]}"
            )
            time.sleep(sleep_sec)
    con.close()
    if last_error is not None:
        # Fallback for network/rate-limit failure when local tarball is available.
        print(f"source_pull_failed_remote fallback_local=1 err={str(last_error)[:220]}")
        return build_source_from_local_tar(args)
    return pd.read_parquet(args.source_cache)


def build_source_from_local_tar(args: argparse.Namespace) -> pd.DataFrame:
    tar_path = args.local_tar_path
    if not tar_path.exists():
        raise FileNotFoundError(
            f"Remote source failed and local tar fallback missing: {tar_path}"
        )

    print(f"building_source_from_local_tar path={tar_path}")
    parser = BytesHeaderParser()
    records: list[dict[str, Any]] = []
    max_rows = int(args.max_rows)
    snippet_chars = max(0, int(args.body_snippet_chars))
    scanned_files = 0

    try:
        with tarfile.open(tar_path, "r:gz") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                if not member.name.startswith("maildir/"):
                    continue

                file_obj = tf.extractfile(member)
                if file_obj is None:
                    continue
                raw = file_obj.read()
                if not raw:
                    continue
                scanned_files += 1
                if scanned_files % 50000 == 0:
                    print(f"local_tar_progress files={scanned_files}")

                if b"\n\n" in raw:
                    header_bytes, body_bytes = raw.split(b"\n\n", 1)
                else:
                    header_bytes, body_bytes = raw, b""

                try:
                    msg = parser.parsebytes(header_bytes + b"\n\n")
                except Exception:
                    continue

                file_name = member.name[len("maildir/") :]
                parts = file_name.split("/")
                if len(parts) < 2:
                    continue
                custodian_id = parts[0].strip().lower()
                folder = parts[1].strip().lower()

                from_field = str(msg.get("From", ""))
                parsed_from = parseaddr(from_field)[1]
                from_email = clean_email_token(parsed_from or from_field)
                if not from_email:
                    continue

                message_id = str(msg.get("Message-ID", "")).strip().lower()
                if not message_id:
                    message_id = f"<local-{hash_short(file_name)}@enron.local>"

                body_snippet = ""
                if snippet_chars > 0 and body_bytes:
                    body_text = body_bytes.decode("utf-8", errors="ignore")
                    body_snippet = body_text[:snippet_chars]

                sent_raw = str(msg.get("Date", ""))
                try:
                    parsed_dt = parsedate_to_datetime(sent_raw)
                    if parsed_dt is None:
                        sent_ts = pd.NaT
                    else:
                        if parsed_dt.tzinfo is None:
                            parsed_dt = parsed_dt.replace(tzinfo=UTC)
                        sent_ts = pd.Timestamp(parsed_dt).tz_convert("UTC")
                except Exception:
                    sent_ts = pd.NaT

                records.append(
                    {
                        "message_id": message_id,
                        "subject": str(msg.get("Subject", "")),
                        "from_email": from_email,
                        "to_raw": [str(x) for x in msg.get_all("To", [])],
                        "cc_raw": [str(x) for x in msg.get_all("Cc", [])],
                        "bcc_raw": [str(x) for x in msg.get_all("Bcc", [])],
                        "sent_ts": sent_ts,
                        "body_snippet": body_snippet,
                        "file_name": file_name,
                        "custodian_id": custodian_id,
                        "folder": folder,
                    }
                )
                if max_rows > 0 and len(records) >= max_rows:
                    break
    except EOFError:
        # Known local archive issue: truncated tar. Keep all parsed records.
        print("local_tar_warning truncated_archive=1 continuing_with_partial_records=1")

    source = pd.DataFrame.from_records(records)
    source = source.dropna(subset=["sent_ts"])
    source = source[
        source["sent_ts"].dt.year.between(int(args.start_year), int(args.end_year))
    ].copy()
    source = source[
        [
            "message_id",
            "subject",
            "from_email",
            "to_raw",
            "cc_raw",
            "bcc_raw",
            "sent_ts",
            "body_snippet",
            "file_name",
            "custodian_id",
            "folder",
        ]
    ]
    args.source_cache.parent.mkdir(parents=True, exist_ok=True)
    source.to_parquet(args.source_cache, index=False)
    print(f"local_tar_complete rows={len(source)} files_scanned={scanned_files}")
    return source


def build_rosetta_events(
    df: pd.DataFrame, include_content: bool
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    work = df.copy()
    work["subject"] = work["subject"].fillna("").astype(str)
    work["body_snippet"] = work["body_snippet"].fillna("").astype(str)
    work["from_email"] = (
        work["from_email"].fillna("").astype(str).str.strip().str.lower()
    )
    work["sent_ts"] = pd.to_datetime(work["sent_ts"], utc=True, errors="coerce")
    work = work.dropna(subset=["sent_ts"])
    work = work[work["from_email"] != ""].copy()

    work["norm_subject"] = work["subject"].map(normalize_subject)
    work["thread_task_id"] = compute_thread_ids(work)

    work["to_recipients"] = work["to_raw"].map(parse_recipient_list)
    work["cc_recipients"] = work["cc_raw"].map(parse_recipient_list)
    work["bcc_recipients"] = work["bcc_raw"].map(parse_recipient_list)
    work["all_targets"] = [
        dedupe_targets(actor, to_r, cc_r, bcc_r)
        for actor, to_r, cc_r, bcc_r in zip(
            work["from_email"],
            work["to_recipients"],
            work["cc_recipients"],
            work["bcc_recipients"],
            strict=False,
        )
    ]
    work["to_count"] = work["to_recipients"].map(len)
    work["cc_count"] = work["cc_recipients"].map(len)
    work["bcc_count"] = work["bcc_recipients"].map(len)

    class_rows = work.apply(classify_event, axis=1, result_type="expand")
    work = pd.concat([work, class_rows], axis=1)

    work["target_id"] = [
        make_target_id(targets, tid)
        for targets, tid in zip(
            work["all_targets"], work["thread_task_id"], strict=False
        )
    ]
    work["event_id"] = [
        f"enron_{hash_short(f'{mid}|{ts.isoformat()}|{actor}')}"
        for mid, ts, actor in zip(
            work["message_id"], work["sent_ts"], work["from_email"], strict=False
        )
    ]

    def build_artifacts_json(row: pd.Series) -> str:
        payload = {
            "source": "enron_email",
            "message_id": row["message_id"],
            "subject": row["subject"][:220],
            "norm_subject": row["norm_subject"][:220],
            "folder": row.get("folder", ""),
            "custodian_id": row.get("custodian_id", ""),
            "to_recipients": row["to_recipients"][:25],
            "cc_recipients": row["cc_recipients"][:25],
            "bcc_count": int(row["bcc_count"]),
            "to_count": int(row["to_count"]),
            "cc_count": int(row["cc_count"]),
            "is_reply": bool(row["is_reply"]),
            "is_forward": bool(row["is_forward"]),
            "is_escalation": bool(row["is_escalation"]),
            "consult_legal_specialist": bool(row["consult_legal_specialist"]),
            "consult_trading_specialist": bool(row["consult_trading_specialist"]),
            "has_attachment_reference": bool(row["has_attachment_reference"]),
            "body_sha1": hashlib.sha1(  # noqa: S324
                str(row["body_snippet"]).encode("utf-8"),
                usedforsecurity=False,
            ).hexdigest(),
        }
        return json.dumps(payload, ensure_ascii=True, sort_keys=True)

    work["artifacts"] = work.apply(build_artifacts_json, axis=1)

    event_cols = [
        "event_id",
        "sent_ts",
        "from_email",
        "target_id",
        "event_type",
        "thread_task_id",
        "artifacts",
    ]
    events_meta = work[event_cols].rename(
        columns={
            "sent_ts": "timestamp",
            "from_email": "actor_id",
        }
    )
    events_meta["content"] = ""
    events_meta = events_meta[
        [
            "event_id",
            "timestamp",
            "actor_id",
            "target_id",
            "event_type",
            "content",
            "thread_task_id",
            "artifacts",
        ]
    ].sort_values(["timestamp", "event_id"])

    events_with_content = events_meta.copy()
    if include_content:
        content_map = work.set_index("event_id")["body_snippet"].to_dict()
        events_with_content["content"] = (
            events_with_content["event_id"].map(content_map).fillna("")
        )

    return events_meta, events_with_content, work


def build_talk_graph(df: pd.DataFrame) -> pd.DataFrame:
    talk = df[["event_id", "sent_ts", "from_email", "all_targets", "event_type"]].copy()
    talk = talk.explode("all_targets")
    talk = talk.rename(
        columns={
            "from_email": "src_actor_id",
            "all_targets": "dst_actor_id",
            "sent_ts": "timestamp",
        }
    )
    talk["dst_actor_id"] = talk["dst_actor_id"].fillna("").astype(str)
    talk = talk[talk["dst_actor_id"] != ""].copy()
    talk = talk[talk["dst_actor_id"] != talk["src_actor_id"]].copy()

    out = (
        talk.groupby(["src_actor_id", "dst_actor_id"])
        .agg(
            interactions=("event_id", "count"),
            escalation_interactions=(
                "event_type",
                lambda s: int((s == "escalation").sum()),
            ),
            assignment_interactions=(
                "event_type",
                lambda s: int((s == "assignment").sum()),
            ),
            approval_interactions=(
                "event_type",
                lambda s: int((s == "approval").sum()),
            ),
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
        )
        .reset_index()
        .sort_values(
            ["interactions", "escalation_interactions"], ascending=[False, False]
        )
    )
    out["escalation_share"] = np.where(
        out["interactions"] > 0,
        out["escalation_interactions"] / out["interactions"],
        0.0,
    )
    return out


def build_work_graph(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = events[
        ["event_id", "timestamp", "actor_id", "thread_task_id", "event_type"]
    ].copy()
    work = work.sort_values(["thread_task_id", "timestamp", "event_id"])
    rows: list[dict[str, Any]] = []

    for thread_id, group in work.groupby("thread_task_id", sort=False):
        prev_state = "start"
        for _, row in group.iterrows():
            curr_state = STATE_MAP.get(str(row["event_type"]), "active")
            rows.append(
                {
                    "thread_task_id": thread_id,
                    "event_id": row["event_id"],
                    "timestamp": row["timestamp"],
                    "actor_id": row["actor_id"],
                    "from_state": prev_state,
                    "to_state": curr_state,
                    "event_type": row["event_type"],
                }
            )
            prev_state = curr_state

    transitions = pd.DataFrame(rows)
    edges = (
        transitions.groupby(["from_state", "to_state"])
        .agg(
            transitions=("event_id", "count"),
            unique_threads=("thread_task_id", "nunique"),
        )
        .reset_index()
        .sort_values("transitions", ascending=False)
    )
    return transitions, edges


def write_schema_json(output_dir: Path) -> None:
    schema = {
        "name": "enron_rosetta_events",
        "description": "Medium-agnostic event schema for Enron email (Rosetta representation).",
        "fields": [
            {"name": "event_id", "type": "string", "required": True},
            {"name": "timestamp", "type": "datetime", "required": True},
            {
                "name": "actor_id",
                "type": "string",
                "required": True,
                "description": "Sender (human)",
            },
            {
                "name": "target_id",
                "type": "string",
                "required": True,
                "description": "Primary target person/group/thread fallback",
            },
            {"name": "event_type", "type": "string", "required": True},
            {
                "name": "content",
                "type": "string",
                "required": False,
                "description": "Optional body snippet; blank in metadata-only export",
            },
            {"name": "thread_task_id", "type": "string", "required": True},
            {"name": "artifacts", "type": "json-string", "required": True},
        ],
        "event_type_values": [
            "message",
            "message_reply",
            "message_forward",
            "assignment",
            "approval",
            "escalation",
        ],
        "notes": [
            "Escalation includes forwards, cc/bcc broadcasts, and specialist-consult cues.",
            "thread_task_id is subject-time heuristic (90-day split), not RFC message-id threading.",
        ],
    }
    (output_dir / "enron_rosetta_schema.json").write_text(
        json.dumps(schema, indent=2) + "\n",
        encoding="utf-8",
    )


def write_summary(
    *,
    output_dir: Path,
    events: pd.DataFrame,
    talk_edges: pd.DataFrame,
    work_edges: pd.DataFrame,
    reference_total_rows: int | None,
) -> None:
    type_counts = (
        events["event_type"].value_counts(normalize=True).rename("share").mul(100.0)
    )
    top_talk = talk_edges.head(10)
    lines = [
        "# Enron Rosetta Export Summary",
        "",
        "## Event Table",
        f"- Rows: **{len(events):,}**",
        f"- Unique actors: **{events['actor_id'].nunique():,}**",
        f"- Unique thread/task IDs: **{events['thread_task_id'].nunique():,}**",
        f"- Escalation share: **{(events['event_type'] == 'escalation').mean() * 100.0:.2f}%**",
    ]
    if reference_total_rows is not None and reference_total_rows > 0:
        coverage = 100.0 * len(events) / float(reference_total_rows)
        lines.append(
            f"- Coverage vs full local header cache ({reference_total_rows:,} rows): **{coverage:.2f}%**"
        )
        if len(events) < reference_total_rows:
            lines.append(
                "- Note: source appears partial (for example, local tar truncation)."
            )
    lines.extend(["", "### Event Type Shares"])
    for event_type, share in type_counts.items():
        lines.append(f"- {event_type}: **{share:.2f}%**")

    lines.extend(
        [
            "",
            "## Talk Graph",
            f"- Directed edges: **{len(talk_edges):,}**",
            f"- Total interactions: **{int(talk_edges['interactions'].sum()):,}**",
            "",
            "### Top 10 Talk Edges",
        ]
    )
    for _, row in top_talk.iterrows():
        lines.append(
            f"- {row['src_actor_id']} -> {row['dst_actor_id']}: "
            f"n={int(row['interactions'])}, escalation_share={row['escalation_share']:.2f}"
        )

    lines.extend(
        [
            "",
            "## Work Graph",
            f"- State transitions logged: **{int(work_edges['transitions'].sum()):,}**",
            f"- Distinct transition edges: **{len(work_edges):,}**",
            "",
            "### Top State Transitions",
        ]
    )
    for _, row in work_edges.head(10).iterrows():
        lines.append(
            f"- {row['from_state']} -> {row['to_state']}: "
            f"{int(row['transitions'])} transitions across {int(row['unique_threads'])} threads"
        )

    (output_dir / "enron_rosetta_summary.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    source_df = load_source_df(args)
    events_meta, events_content, working = build_rosetta_events(
        source_df, include_content=args.include_content
    )

    talk_edges = build_talk_graph(working)
    work_transitions, work_edges = build_work_graph(events_meta)

    events_meta.to_parquet(
        args.output_dir / "enron_rosetta_events_metadata.parquet", index=False
    )
    if args.include_content:
        events_content.to_parquet(
            args.output_dir / "enron_rosetta_events_content.parquet", index=False
        )
    else:
        events_content.head(0).to_parquet(
            args.output_dir / "enron_rosetta_events_content.parquet", index=False
        )

    talk_edges.to_parquet(
        args.output_dir / "enron_talk_graph_edges.parquet", index=False
    )
    work_transitions.to_parquet(
        args.output_dir / "enron_work_graph_transitions.parquet", index=False
    )
    work_edges.to_parquet(
        args.output_dir / "enron_work_graph_edges.parquet", index=False
    )

    # Compact CSV samples stay human-readable and omit hash-heavy IDs.
    write_safe_samples(
        output_dir=args.output_dir,
        working=working,
        talk_edges=talk_edges,
        work_transitions=work_transitions,
    )

    write_schema_json(args.output_dir)
    reference_total_rows: int | None = None
    header_cache = Path("data/enron_headers_1997_2003.parquet")
    if header_cache.exists():
        try:
            reference_total_rows = len(
                pd.read_parquet(header_cache, columns=["message_id"])
            )
        except Exception:
            reference_total_rows = None
    write_summary(
        output_dir=args.output_dir,
        events=events_meta,
        talk_edges=talk_edges,
        work_edges=work_edges,
        reference_total_rows=reference_total_rows,
    )

    print(f"Wrote Rosetta artifacts to: {args.output_dir.resolve()}")
    print(f"Event rows: {len(events_meta)}")
    print(f"Talk edges: {len(talk_edges)}")
    print(f"Work transitions: {len(work_transitions)}")


def write_safe_samples(
    *,
    output_dir: Path,
    working: pd.DataFrame,
    talk_edges: pd.DataFrame,
    work_transitions: pd.DataFrame,
) -> None:
    event_sample = working.head(1000).copy()
    event_sample["timestamp"] = event_sample["sent_ts"].dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    event_sample["subject_preview"] = (
        event_sample["subject"]
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 120)
    )
    event_sample["target_count"] = event_sample["all_targets"].map(len)
    event_sample[
        [
            "timestamp",
            "from_email",
            "event_type",
            "subject_preview",
            "folder",
            "custodian_id",
            "target_count",
            "to_count",
            "cc_count",
            "bcc_count",
        ]
    ].rename(columns={"from_email": "actor_id"}).to_csv(
        output_dir / "enron_rosetta_events_metadata_sample.csv",
        index=False,
    )

    talk_edges.head(1000)[
        [
            "src_actor_id",
            "dst_actor_id",
            "interactions",
            "escalation_interactions",
            "assignment_interactions",
            "approval_interactions",
            "first_timestamp",
            "last_timestamp",
            "escalation_share",
        ]
    ].to_csv(output_dir / "enron_talk_graph_edges_sample.csv", index=False)

    work_transitions.head(1000)[
        [
            "timestamp",
            "actor_id",
            "from_state",
            "to_state",
            "event_type",
        ]
    ].to_csv(output_dir / "enron_work_graph_transitions_sample.csv", index=False)


if __name__ == "__main__":
    main()
