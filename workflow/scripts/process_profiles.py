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
WELL_PROFILE_COLUMNS = IDENTIFIER_COLUMNS + [
    "Metadata_InChIKey",
    "Metadata_PlateType",
    "Metadata_pert_type",
    "Metadata_Is_Compound",
]


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


well_metadata = (
    read_reference_csv(snakemake.input.well_metadata)
    .unique(subset=["Metadata_Source", "Metadata_Plate", "Metadata_Well"])
    .lazy()
)
plate_metadata = (
    read_reference_csv(snakemake.input.plate_metadata)
    .unique(subset=["Metadata_Source", "Metadata_Batch", "Metadata_Plate"])
    .lazy()
)
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

well_profiles_path = Path(snakemake.output.well_profiles)
well_profiles_path.parent.mkdir(parents=True, exist_ok=True)

raw_profiles = pl.scan_parquet(raw_profile_paths)
raw_profile_schema = raw_profiles.collect_schema()
expected_profile_columns = ["source", "batch", "plate", "well"]
missing_profile_columns = [
    column
    for column in expected_profile_columns
    if column not in raw_profile_schema.names()
]
if missing_profile_columns:
    raise ValueError(
        "Profile parquet schema is missing required columns: "
        + ", ".join(missing_profile_columns)
    )

embedding_columns = [
    column for column in raw_profile_schema.names() if column.endswith("_emb")
]
if not embedding_columns:
    raise ValueError(
        "Profile parquet schema does not expose any embedding columns ending with '_emb'"
    )

profile_model = snakemake.params["profile_model"].rstrip("/")
n_rows = cast(pl.DataFrame, raw_profiles.select(pl.len()).collect()).item()
embedding_lengths = cast(
    pl.DataFrame,
    pl.scan_parquet(raw_profile_paths[0])
    .select(
        [
            pl.col(column).list.len().first().alias(column)
            for column in embedding_columns
        ]
    )
    .collect(),
).to_dicts()[0]
embedding_width = sum(int(length) for length in embedding_lengths.values())
estimated_embedding_bytes = n_rows * embedding_width * 4
free_output_bytes = shutil.disk_usage(well_profiles_path.parent).free

print(
    f"[process_profiles] model={profile_model} embedding_columns={embedding_columns}",
    flush=True,
)
print(
    "[process_profiles] preflight "
    f"rows={n_rows} "
    f"embedding_width={embedding_width} "
    f"estimated_embedding_gib_float32={bytes_to_gib(estimated_embedding_bytes):.3f} "
    f"free_output_gib={bytes_to_gib(free_output_bytes):.3f}",
    flush=True,
)

if free_output_bytes < estimated_embedding_bytes:
    raise ValueError(
        "Insufficient free disk space for processed profile output. "
        f"Estimated embedding payload alone is at least "
        f"{bytes_to_gib(estimated_embedding_bytes):.3f} GiB after float32 casting, "
        f"but only {bytes_to_gib(free_output_bytes):.3f} GiB is free in "
        f"{well_profiles_path.parent}. Reduce selection.sources, set "
        "selection.plate_limit_per_source, or free disk space before rerunning."
    )

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
        pl.when(pl.col("Metadata_Is_Compound") & pl.col("Metadata_pert_type").is_null())
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

profiles = filtered_profiles.select(
    WELL_PROFILE_COLUMNS + embedding_columns
).with_columns(
    [pl.col(column).cast(pl.List(pl.Float32)) for column in embedding_columns]
)

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
