from pathlib import Path

import polars as pl


REQUIRED_COMPOUND_MASTER_COLUMNS = [
    "Metadata_JCP2022",
    "Metadata_InChIKey",
    "Metadata_InChI",
    "Metadata_SMILES",
    "Metadata_pert_type",
    "Metadata_Control_Name",
    "Metadata_Compound_Source_Count",
    "Metadata_Compound_Sources",
]
OUTPUT_COLUMNS = [
    "Metadata_JCP2022",
    "Metadata_InChIKey",
    "Metadata_pert_type",
    "Metadata_Control_Name",
    "Metadata_InChI",
    "Metadata_SMILES",
    "Metadata_Compound_Source_Count",
    "Metadata_Compound_Sources",
    "AnnotationDB_CID",
    "AnnotationDB_Name",
    "AnnotationDB_SMILES",
    "In_AnnotationDB",
]


def validate_columns(frame, required_columns, label):
    missing_columns = [
        column for column in required_columns if column not in frame.columns
    ]
    if missing_columns:
        raise ValueError(
            f"{label} is missing required columns: {', '.join(missing_columns)}"
        )


input_names = set(snakemake.input.keys())
if "compound_masters" in input_names:
    compound_master_inputs = snakemake.input["compound_masters"]
else:
    compound_master_inputs = [snakemake.input.compound_master]
if isinstance(compound_master_inputs, str):
    compound_master_inputs = [compound_master_inputs]

output_path = Path(snakemake.output.compound_metadata)
annotationdb_cache = Path(snakemake.input.annotationdb_cache)

compound_master_frames = [
    pl.read_csv(
        Path(compound_master_path),
        separator="\t",
        null_values="NA",
    )
    for compound_master_path in compound_master_inputs
]
compound_master = (
    pl.concat(compound_master_frames, how="vertical_relaxed")
    .unique(subset=["Metadata_JCP2022"], keep="first")
    .sort("Metadata_JCP2022")
)
validate_columns(
    compound_master, REQUIRED_COMPOUND_MASTER_COLUMNS, "compound_master.tsv"
)

duplicate_inchikeys = (
    compound_master.group_by("Metadata_InChIKey")
    .len()
    .filter(pl.col("len") > 1)
    .select("Metadata_InChIKey")
)
if duplicate_inchikeys.height > 0:
    raise ValueError(
        "compound_master.tsv contains duplicate Metadata_InChIKey rows; "
        "cannot build an InChIKey-keyed compound metadata table"
    )

duplicate_jcp = (
    compound_master.group_by("Metadata_JCP2022")
    .len()
    .filter(pl.col("len") > 1)
    .select("Metadata_JCP2022")
)
if duplicate_jcp.height > 0:
    raise ValueError(
        "compound_master.tsv contains duplicate Metadata_JCP2022 rows; "
        "cannot build a JCP-keyed compound metadata table"
    )

annotationdb = (
    pl.read_csv(annotationdb_cache, null_values=["", "NA"])
    .rename(
        {
            "inchikey": "Metadata_InChIKey",
            "cid": "AnnotationDB_CID",
            "name": "AnnotationDB_Name",
            "smiles": "AnnotationDB_SMILES",
        }
    )
    .select(
        [
            "Metadata_InChIKey",
            "AnnotationDB_CID",
            "AnnotationDB_Name",
            "AnnotationDB_SMILES",
        ]
    )
    .unique(subset=["Metadata_InChIKey"])
)

normalized_control_name = (
    pl.when(
        pl.col("Metadata_Control_Name").is_null()
        | (pl.col("Metadata_Control_Name").str.strip_chars() == "")
    )
    .then(None)
    .otherwise(pl.col("Metadata_Control_Name"))
)

compound_metadata = (
    compound_master.select(REQUIRED_COMPOUND_MASTER_COLUMNS)
    .join(annotationdb, on="Metadata_InChIKey", how="left")
    .with_columns(
        [
            normalized_control_name.alias("Metadata_Control_Name"),
            pl.col("AnnotationDB_CID").is_not_null().alias("In_AnnotationDB"),
        ]
    )
    .select(OUTPUT_COLUMNS)
    .sort("Metadata_JCP2022")
)

output_path.parent.mkdir(parents=True, exist_ok=True)
compound_metadata.write_csv(
    output_path,
    separator="\t",
    null_value="NA",
)

match_counts = compound_metadata.select(
    [
        pl.len().alias("compound_rows"),
        pl.col("In_AnnotationDB").sum().alias("annotationdb_matches"),
    ]
).row(0, named=True)
print(
    "[build_compound_metadata] "
    f"compound_rows={match_counts['compound_rows']} "
    f"annotationdb_matches={match_counts['annotationdb_matches']} "
    f"annotationdb_cache={annotationdb_cache} "
    f"output={output_path}",
    flush=True,
)
