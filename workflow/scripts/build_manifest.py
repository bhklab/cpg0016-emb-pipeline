from collections import Counter, defaultdict
import json
import subprocess
from pathlib import Path

import polars as pl


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def parse_selection_list(values):
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]

    normalized = []
    seen = set()
    for value in values:
        if value is None:
            continue
        item = str(value).strip()
        if not item or item in seen:
            continue
        normalized.append(item)
        seen.add(item)

    return normalized


def record_skip(skip_counts, skip_examples, reason, key):
    skip_counts[reason] += 1
    examples = skip_examples[reason]
    if len(examples) < 5:
        examples.append(key)


def list_source_objects(bucket, prefix, no_sign_request):
    objects = []
    continuation_token = None

    while True:
        cmd = [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            prefix,
            "--output",
            "json",
        ]
        if no_sign_request:
            cmd.append("--no-sign-request")
        if continuation_token:
            cmd.extend(["--continuation-token", continuation_token])

        response = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(response.stdout)
        objects.extend(payload.get("Contents", []))

        continuation_token = payload.get("NextContinuationToken")
        if not payload.get("IsTruncated"):
            break

    return objects


def log_scan_summary(source, prefix, scanned_objects, rows, skip_counts, skip_examples):
    valid_count = len(rows)
    skipped_count = sum(skip_counts.values())
    print(
        f"[build_manifest] scan source={source} prefix={prefix} "
        f"scanned_objects={scanned_objects} valid_profile_files={valid_count} "
        f"skipped_objects={skipped_count}",
        flush=True,
    )

    if not skip_counts:
        return

    print(
        f"[build_manifest] warnings for source={source} "
        "unexpected objects were ignored while building the manifest",
        flush=True,
    )
    for reason, count in sorted(skip_counts.items()):
        print(
            f"\treason={reason} count={count} samples={skip_examples[reason]}",
            flush=True,
        )


def load_plate_type_lookup(path):
    plate_metadata = pl.read_csv(path).unique(
        subset=["Metadata_Source", "Metadata_Batch", "Metadata_Plate"]
    )
    return {
        (
            row["Metadata_Source"],
            row["Metadata_Batch"],
            row["Metadata_Plate"],
        ): row["Metadata_PlateType"]
        for row in plate_metadata.select(
            [
                "Metadata_Source",
                "Metadata_Batch",
                "Metadata_Plate",
                "Metadata_PlateType",
            ]
        ).iter_rows(named=True)
    }


def manifest_rows_for_source(
    bucket,
    dataset_prefix,
    source,
    profile_model,
    no_sign_request,
    plate_type_lookup,
    include_plate_types,
    exclude_plate_types,
):
    prefix = f"{dataset_prefix}/{source}/workspace_dl/profiles/{profile_model}/"
    rows = []
    skip_counts = Counter()
    skip_examples = defaultdict(list)
    seen_plate_keys = {}
    scanned_objects = list_source_objects(
        bucket=bucket, prefix=prefix, no_sign_request=no_sign_request
    )

    for obj in scanned_objects:
        key = obj["Key"]
        size_bytes = int(obj["Size"])
        if not key.endswith(".parquet"):
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="non_parquet_object",
                key=key,
            )
            continue

        parts = key.split("/")
        if len(parts) != 8:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="unexpected_path_structure",
                key=key,
            )
            continue

        (
            parsed_dataset_prefix,
            parsed_source,
            parsed_workspace_dir,
            parsed_profiles_dir,
            parsed_model,
            batch,
            plate,
            filename,
        ) = parts
        if parsed_dataset_prefix != dataset_prefix:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="unexpected_dataset_prefix",
                key=key,
            )
            continue
        if parsed_workspace_dir != "workspace_dl" or parsed_profiles_dir != "profiles":
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="unexpected_parent_directories",
                key=key,
            )
            continue
        if parsed_source != source:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="unexpected_source_directory",
                key=key,
            )
            continue
        if parsed_model != profile_model:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="unexpected_profile_model",
                key=key,
            )
            continue
        if filename != f"{plate}.parquet":
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="plate_filename_mismatch",
                key=key,
            )
            continue
        if size_bytes <= 0:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="empty_object",
                key=key,
            )
            continue

        manifest_key = (parsed_source, batch, plate)
        plate_type = plate_type_lookup.get(manifest_key)
        if plate_type is None:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="missing_plate_metadata",
                key=key,
            )
            continue
        if include_plate_types and plate_type not in include_plate_types:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="plate_type_not_included",
                key=f"{key} ({plate_type})",
            )
            continue
        if exclude_plate_types and plate_type in exclude_plate_types:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="plate_type_excluded",
                key=f"{key} ({plate_type})",
            )
            continue
        if manifest_key in seen_plate_keys:
            record_skip(
                skip_counts=skip_counts,
                skip_examples=skip_examples,
                reason="duplicate_plate_object",
                key=key,
            )
            continue
        seen_plate_keys[manifest_key] = key

        rows.append(
            {
                "Metadata_Source": parsed_source,
                "Metadata_Batch": batch,
                "Metadata_Plate": plate,
                "Metadata_PlateType": plate_type,
                "profile_model": parsed_model,
                "s3_key": key,
                "size_bytes": size_bytes,
                "local_relpath": f"{bucket}/{key}",
            }
        )

    rows.sort(key=lambda row: (row["Metadata_Batch"], row["Metadata_Plate"]))
    log_scan_summary(
        source=source,
        prefix=prefix,
        scanned_objects=len(scanned_objects),
        rows=rows,
        skip_counts=skip_counts,
        skip_examples=skip_examples,
    )
    return rows


cfg = snakemake.config
dataset_cfg = cfg["dataset"]
selection_cfg = cfg["selection"]

bucket = dataset_cfg["bucket"]
dataset_prefix = dataset_cfg["prefix"].rstrip("/")
profile_model = snakemake.params["profile_model"].rstrip("/")
selection_mode = selection_cfg.get("mode", "source_subset")

if selection_mode != "source_subset":
    raise ValueError(
        "selection.mode must be 'source_subset' in this v1 deep-learning profile pipeline"
    )

sources = selection_cfg.get("sources") or []
if not sources:
    raise ValueError("selection.sources must contain at least one source")

include_plate_types = set(
    parse_selection_list(selection_cfg.get("include_plate_types"))
)
exclude_plate_types = set(
    parse_selection_list(selection_cfg.get("exclude_plate_types"))
)
if include_plate_types & exclude_plate_types:
    overlap = sorted(include_plate_types & exclude_plate_types)
    raise ValueError(
        "selection.include_plate_types and selection.exclude_plate_types overlap: "
        + ", ".join(overlap)
    )

plate_limit = selection_cfg.get("plate_limit_per_source")
plate_limit = int(plate_limit) if plate_limit is not None else None
overwrite = parse_bool(cfg["download"].get("overwrite", False))
plate_type_lookup = load_plate_type_lookup(snakemake.input.plate_metadata)

print(
    "[build_manifest] start\n"
    f"\tbucket={bucket}\n"
    f"\tdataset_prefix={dataset_prefix}\n"
    f"\tprofile_model={profile_model}\n"
    f"\tsources={sources}\n"
    f"\tinclude_plate_types={sorted(include_plate_types) if include_plate_types else []}\n"
    f"\texclude_plate_types={sorted(exclude_plate_types) if exclude_plate_types else []}\n"
    f"\tplate_limit_per_source={plate_limit}\n"
    f"\toverwrite={overwrite}",
    flush=True,
)

all_rows = []
for source in sources:
    source_rows = manifest_rows_for_source(
        bucket=bucket,
        dataset_prefix=dataset_prefix,
        source=source,
        profile_model=profile_model,
        no_sign_request=True,
        plate_type_lookup=plate_type_lookup,
        include_plate_types=include_plate_types,
        exclude_plate_types=exclude_plate_types,
    )
    if plate_limit is not None:
        source_rows = source_rows[:plate_limit]

    if not source_rows:
        raise ValueError(
            f"No profile parquet files found for source '{source}' "
            f"and model '{profile_model}'"
        )

    source_bytes = sum(row["size_bytes"] for row in source_rows)
    print(
        f"[build_manifest] source={source} files={len(source_rows)} "
        f"size_bytes={source_bytes}",
        flush=True,
    )
    all_rows.extend(source_rows)

if not all_rows:
    raise ValueError("No profile parquet files matched the configured subset")

manifest_df = pl.DataFrame(all_rows).sort(
    ["Metadata_Source", "Metadata_Batch", "Metadata_Plate"]
)
summary_df = (
    manifest_df.group_by("Metadata_Source")
    .agg(
        pl.len().alias("n_files"),
        pl.col("size_bytes").sum().alias("size_bytes"),
    )
    .with_columns(
        pl.col("n_files").cast(pl.Int64),
        pl.col("size_bytes").cast(pl.Int64),
    )
    .with_columns((pl.col("size_bytes") / (1024**3)).round(3).alias("size_gib"))
    .sort("Metadata_Source")
)
total_bytes = manifest_df["size_bytes"].sum()

summary_df = pl.concat(
    [
        summary_df,
        pl.DataFrame(
            {
                "Metadata_Source": ["TOTAL"],
                "n_files": [manifest_df.height],
                "size_bytes": [total_bytes],
                "size_gib": [round(total_bytes / (1024**3), 3)],
            }
        ),
    ],
    how="vertical",
)

manifest_tsv = Path(snakemake.output.manifest_tsv)
summary_tsv = Path(snakemake.output.summary_tsv)

for path in (manifest_tsv, summary_tsv):
    path.parent.mkdir(parents=True, exist_ok=True)

manifest_df.write_csv(manifest_tsv, separator="\t")
summary_df.write_csv(summary_tsv, separator="\t")

print("[build_manifest] summary", flush=True)
for row in summary_df.iter_rows(named=True):
    print(
        f"\t{row['Metadata_Source']}: files={row['n_files']} "
        f"size_bytes={row['size_bytes']} size_gib={row['size_gib']}",
        flush=True,
    )
