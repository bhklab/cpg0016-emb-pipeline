from pathlib import Path
import shutil
from typing import cast

import polars as pl


IDENTIFIER_COLUMNS = [
    "Metadata_Source",
    "Metadata_Batch",
    "Metadata_Plate",
    "Metadata_Well",
    "Metadata_JCP2022",
]
PLACEHOLDER_JCP2022 = "JCP2022_999999"
TREATMENT_PERT_TYPE = "treatment"
COMPOUND_MASTER_COLUMNS = [
    "Metadata_JCP2022",
    "Metadata_InChIKey",
    "Metadata_InChI",
    "Metadata_SMILES",
    "Metadata_pert_type",
    "Metadata_Control_Name",
    "Metadata_Compound_Source_Count",
    "Metadata_Compound_Sources",
    "Metadata_Is_Compound",
]
CONTROL_METADATA_COLUMNS = [
    "Metadata_JCP2022",
    "Metadata_pert_type",
    "Metadata_Control_Name",
]
WELL_PROFILE_COLUMNS = IDENTIFIER_COLUMNS + [
    "Metadata_InChIKey",
    "Metadata_PlateType",
    "Metadata_pert_type",
    "Metadata_Is_Compound",
]
PROFILE_MODEL_CELLPROFILER = "cellprofiler"
NUMERIC_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
}


def read_reference_csv(path):
    return pl.read_csv(path)


def bytes_to_gib(value):
    return value / (1024**3)


def normalize_selection_mode(value, default="none"):
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    return normalized


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


def is_numeric_dtype(dtype):
    return dtype in NUMERIC_DTYPES


def manual_control_metadata_overrides(config):
    rows = config.get("curation", {}).get("control_metadata_overrides", [])
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise TypeError("curation.control_metadata_overrides must be a list")

    normalized_rows = []
    for row_idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise TypeError(
                "curation.control_metadata_overrides entries must be mappings; "
                f"entry {row_idx} is {type(row).__name__}"
            )

        normalized_row = {}
        for column in CONTROL_METADATA_COLUMNS:
            value = row.get(column)
            if value is None or str(value).strip() == "":
                raise ValueError(
                    "curation.control_metadata_overrides entry "
                    f"{row_idx} is missing required column {column}"
                )
            normalized_row[column] = str(value).strip()
        normalized_rows.append(normalized_row)

    return pl.DataFrame(
        normalized_rows,
        schema={column: pl.Utf8 for column in CONTROL_METADATA_COLUMNS},
    )


well_metadata = (
    read_reference_csv(snakemake.input.well_metadata)
    .unique(subset=["Metadata_Source", "Metadata_Plate", "Metadata_Well"])
    .lazy()
)
plate_metadata_frame = read_reference_csv(snakemake.input.plate_metadata).unique(
    subset=["Metadata_Source", "Metadata_Batch", "Metadata_Plate"]
)
plate_metadata = plate_metadata_frame.lazy()
compound_metadata = (
    read_reference_csv(snakemake.input.compound_metadata)
    .unique(subset=["Metadata_JCP2022"])
    .filter(pl.col("Metadata_JCP2022") != PLACEHOLDER_JCP2022)
)
control_metadata = (
    read_reference_csv(snakemake.input.control_metadata)
    .unique(subset=["Metadata_JCP2022"])
    .rename({"Metadata_Name": "Metadata_Control_Name"})
    .drop("Metadata_modality")
    .with_columns(
        pl.when(
            pl.col("Metadata_Control_Name").is_null()
            | (pl.col("Metadata_Control_Name").str.strip_chars() == "")
        )
        .then(None)
        .otherwise(pl.col("Metadata_Control_Name"))
        .alias("Metadata_Control_Name")
    )
)
control_metadata_overrides = manual_control_metadata_overrides(snakemake.config)
if not control_metadata_overrides.is_empty():
    # Source-level plate controls can be missing from the upstream control table.
    control_metadata = pl.concat(
        [control_metadata, control_metadata_overrides],
        how="vertical",
    ).unique(subset=["Metadata_JCP2022"], keep="last")

compound_source_metadata = (
    read_reference_csv(snakemake.input.compound_source_metadata)
    .unique(subset=["Metadata_JCP2022", "Metadata_Compound_Source"])
    .filter(pl.col("Metadata_JCP2022") != PLACEHOLDER_JCP2022)
)

compound_source_coverage = compound_source_metadata.group_by("Metadata_JCP2022").agg(
    pl.len().alias("Metadata_Compound_Source_Count"),
    pl.col("Metadata_Compound_Source").sort().alias("Metadata_Compound_Sources"),
)

raw_profile_paths = [str(path) for path in snakemake.input.raw_profiles]
if not raw_profile_paths:
    raise ValueError("No profile parquet files were downloaded for processing")

profile_model = snakemake.params["profile_model"].rstrip("/")
well_profiles_path = Path(snakemake.output.well_profiles)
well_profiles_path.parent.mkdir(parents=True, exist_ok=True)

raw_profiles = pl.scan_parquet(raw_profile_paths)
raw_profile_schema = raw_profiles.collect_schema()
raw_profile_columns = raw_profile_schema.names()

if profile_model == PROFILE_MODEL_CELLPROFILER:
    expected_profile_columns = ["Metadata_Source", "Metadata_Plate", "Metadata_Well"]
    missing_profile_columns = [
        column
        for column in expected_profile_columns
        if column not in raw_profile_columns
    ]
    if missing_profile_columns:
        raise ValueError(
            "CellProfiler parquet schema is missing required columns: "
            + ", ".join(missing_profile_columns)
        )

    feature_columns = [
        column
        for column, dtype in raw_profile_schema.items()
        if not column.startswith("Metadata_") and is_numeric_dtype(dtype)
    ]
    if not feature_columns:
        raise ValueError("CellProfiler parquet does not expose numeric feature columns")

    profiles = raw_profiles
    if "Metadata_Batch" not in raw_profile_columns:
        profiles = profiles.join(
            plate_metadata_frame.select(
                [
                    "Metadata_Source",
                    "Metadata_Plate",
                    "Metadata_Batch",
                    "Metadata_PlateType",
                ]
            )
            .unique(subset=["Metadata_Source", "Metadata_Plate"])
            .lazy(),
            on=["Metadata_Source", "Metadata_Plate"],
            how="left",
        )
    else:
        profiles = profiles.join(
            plate_metadata,
            on=["Metadata_Source", "Metadata_Batch", "Metadata_Plate"],
            how="left",
        )

    selection_cfg = snakemake.config["selection"]
    selected_sources = parse_selection_list(selection_cfg.get("sources"))
    include_plate_types = parse_selection_list(selection_cfg.get("include_plate_types"))
    plate_limit = selection_cfg.get("plate_limit_per_source")
    plate_limit = int(plate_limit) if plate_limit is not None else None

    if selected_sources:
        profiles = profiles.filter(pl.col("Metadata_Source").is_in(selected_sources))
    if include_plate_types:
        profiles = profiles.filter(
            pl.col("Metadata_PlateType").is_in(include_plate_types)
        )
    if plate_limit is not None:
        plate_limit_keys = plate_metadata_frame
        if selected_sources:
            plate_limit_keys = plate_limit_keys.filter(
                pl.col("Metadata_Source").is_in(selected_sources)
            )
        if include_plate_types:
            plate_limit_keys = plate_limit_keys.filter(
                pl.col("Metadata_PlateType").is_in(include_plate_types)
            )
        plate_limit_keys = (
            plate_limit_keys.sort(
                ["Metadata_Source", "Metadata_Batch", "Metadata_Plate"]
            )
            .group_by("Metadata_Source")
            .head(plate_limit)
            .select(["Metadata_Source", "Metadata_Batch", "Metadata_Plate"])
        )
        profiles = profiles.join(
            plate_limit_keys.lazy(),
            on=["Metadata_Source", "Metadata_Batch", "Metadata_Plate"],
            how="semi",
        )

    profiles = (
        profiles.join(
            well_metadata,
            on=["Metadata_Source", "Metadata_Plate", "Metadata_Well"],
            how="left",
        )
        .join(control_metadata.lazy(), on="Metadata_JCP2022", how="left")
        .join(compound_metadata.lazy(), on="Metadata_JCP2022", how="left")
        .with_columns(
            pl.col("Metadata_InChIKey").is_not_null().alias("Metadata_Is_Compound")
        )
        .with_columns(
            pl.when(
                pl.col("Metadata_Is_Compound") & pl.col("Metadata_pert_type").is_null()
            )
            .then(pl.lit(TREATMENT_PERT_TYPE))
            .otherwise(pl.col("Metadata_pert_type"))
            .alias("Metadata_pert_type")
        )
    )
    feature_width = len(feature_columns)
    feature_kind = "scalar"
else:
    expected_profile_columns = ["source", "batch", "plate", "well"]
    missing_profile_columns = [
        column
        for column in expected_profile_columns
        if column not in raw_profile_columns
    ]
    if missing_profile_columns:
        raise ValueError(
            "Profile parquet schema is missing required columns: "
            + ", ".join(missing_profile_columns)
        )

    feature_columns = [
        column for column in raw_profile_columns if column.endswith("_emb")
    ]
    if not feature_columns:
        raise ValueError(
            "Profile parquet schema does not expose any embedding columns ending with '_emb'"
        )

    embedding_lengths = cast(
        pl.DataFrame,
        pl.scan_parquet(raw_profile_paths[0])
        .select(
            [
                pl.col(column).list.len().first().alias(column)
                for column in feature_columns
            ]
        )
        .collect(),
    ).to_dicts()[0]
    feature_width = sum(int(length) for length in embedding_lengths.values())
    feature_kind = "list_embedding"

    profiles = (
        raw_profiles.rename(
            {
                "source": "Metadata_Source",
                "batch": "Metadata_Batch",
                "plate": "Metadata_Plate",
                "well": "Metadata_Well",
            }
        )
        .join(
            plate_metadata,
            on=["Metadata_Source", "Metadata_Batch", "Metadata_Plate"],
            how="left",
        )
        .join(
            well_metadata,
            on=["Metadata_Source", "Metadata_Plate", "Metadata_Well"],
            how="left",
        )
        .join(control_metadata.lazy(), on="Metadata_JCP2022", how="left")
        .join(compound_metadata.lazy(), on="Metadata_JCP2022", how="left")
        .with_columns(
            pl.col("Metadata_InChIKey").is_not_null().alias("Metadata_Is_Compound")
        )
        .with_columns(
            pl.when(
                pl.col("Metadata_Is_Compound") & pl.col("Metadata_pert_type").is_null()
            )
            .then(pl.lit(TREATMENT_PERT_TYPE))
            .otherwise(pl.col("Metadata_pert_type"))
            .alias("Metadata_pert_type")
        )
    )

output_row_filter = normalize_selection_mode(
    snakemake.config["selection"].get("output_row_filter")
)
if output_row_filter == "none":
    filtered_profiles = profiles
elif output_row_filter == "all_compound_wells":
    filtered_profiles = profiles.filter(pl.col("Metadata_Is_Compound"))
elif output_row_filter == "treatment_compounds_only":
    filtered_profiles = profiles.filter(
        pl.col("Metadata_Is_Compound")
        & (pl.col("Metadata_pert_type") == TREATMENT_PERT_TYPE)
    )
else:
    raise ValueError(
        "Unsupported selection.output_row_filter: "
        f"{snakemake.config['selection'].get('output_row_filter')}"
    )

print(
    f"[process_profiles] output_row_filter={output_row_filter}",
    flush=True,
)

n_rows = cast(pl.DataFrame, filtered_profiles.select(pl.len()).collect()).item()
estimated_feature_bytes = n_rows * feature_width * 4
free_output_bytes = shutil.disk_usage(well_profiles_path.parent).free

print(
    "[process_profiles] "
    f"model={profile_model} feature_kind={feature_kind} "
    f"feature_columns={len(feature_columns)} first_features={feature_columns[:5]}",
    flush=True,
)
print(
    "[process_profiles] preflight "
    f"rows={n_rows} "
    f"feature_width={feature_width} "
    f"estimated_feature_gib_float32={bytes_to_gib(estimated_feature_bytes):.3f} "
    f"free_output_gib={bytes_to_gib(free_output_bytes):.3f}",
    flush=True,
)

if free_output_bytes < estimated_feature_bytes:
    raise ValueError(
        "Insufficient free disk space for processed profile output. "
        f"Estimated feature payload alone is at least "
        f"{bytes_to_gib(estimated_feature_bytes):.3f} GiB after float32 casting, "
        f"but only {bytes_to_gib(free_output_bytes):.3f} GiB is free in "
        f"{well_profiles_path.parent}. Reduce selection.sources, set "
        "selection.plate_limit_per_source, or free disk space before rerunning."
    )

selected_compound_ids = (
    filtered_profiles.filter(pl.col("Metadata_Is_Compound"))
    .select("Metadata_JCP2022")
    .unique()
)

compound_master = (
    compound_metadata.lazy()
    .join(control_metadata.lazy(), on="Metadata_JCP2022", how="left")
    .join(compound_source_coverage.lazy(), on="Metadata_JCP2022", how="left")
    .join(selected_compound_ids, on="Metadata_JCP2022", how="inner")
    .with_columns(pl.lit(True).alias("Metadata_Is_Compound"))
    .with_columns(
        pl.when(pl.col("Metadata_pert_type").is_null())
        .then(pl.lit(TREATMENT_PERT_TYPE))
        .otherwise(pl.col("Metadata_pert_type"))
        .alias("Metadata_pert_type")
    )
    .sort("Metadata_JCP2022")
)

compound_master_path = Path(snakemake.output.compound_master)
compound_master_path.parent.mkdir(parents=True, exist_ok=True)
compound_master_frame = cast(
    pl.DataFrame,
    compound_master.select(COMPOUND_MASTER_COLUMNS)
    .with_columns(
        [
            pl.when(
                pl.col("Metadata_Control_Name").is_null()
                | (pl.col("Metadata_Control_Name").str.strip_chars() == "")
            )
            .then(None)
            .otherwise(pl.col("Metadata_Control_Name"))
            .alias("Metadata_Control_Name"),
            pl.col("Metadata_Compound_Sources")
            .list.join("|")
            .alias("Metadata_Compound_Sources"),
        ]
    )
    .collect(),
)
compound_master_frame.write_csv(
    compound_master_path,
    separator="\t",
    null_value="NA",
)

if feature_kind == "list_embedding":
    cast_expressions = [
        pl.col(column).cast(pl.List(pl.Float32)) for column in feature_columns
    ]
else:
    cast_expressions = [pl.col(column).cast(pl.Float32) for column in feature_columns]

profiles = filtered_profiles.select(
    WELL_PROFILE_COLUMNS + feature_columns
).with_columns(cast_expressions)

print(
    f"[process_profiles] writing well profiles to {well_profiles_path}",
    flush=True,
)
profiles.sink_parquet(well_profiles_path.as_posix(), compression="zstd")

validation = cast(
    pl.DataFrame,
    pl.scan_parquet(well_profiles_path)
    .select(
        [
            pl.len().alias("n_rows"),
            pl.struct(IDENTIFIER_COLUMNS[:-1]).n_unique().alias("unique_well_keys"),
            pl.col("Metadata_JCP2022").null_count().alias("null_jcp2022"),
            (~pl.col("Metadata_Is_Compound")).sum().alias("noncompound_rows"),
            pl.col("Metadata_pert_type")
            .is_not_null()
            .sum()
            .alias("pert_type_populated_rows"),
            (pl.col("Metadata_pert_type") == TREATMENT_PERT_TYPE)
            .sum()
            .alias("treatment_rows"),
        ]
    )
    .collect(),
)
print(f"[process_profiles] well profile validation\n{validation}", flush=True)

compound_validation = cast(
    pl.DataFrame,
    pl.scan_csv(compound_master_path, separator="\t")
    .select(
        [
            pl.len().alias("n_rows"),
            pl.col("Metadata_JCP2022").n_unique().alias("n_unique_jcp2022"),
            pl.col("Metadata_InChIKey").null_count().alias("null_inchikey"),
        ]
    )
    .collect(),
)
print(
    f"[process_profiles] compound master validation\n{compound_validation}", flush=True
)
