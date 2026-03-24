import json
import time
from pathlib import Path
from urllib.request import Request, urlopen

import polars as pl


ANNOTATIONDB_URL = "https://annotationdb.bhklab.ca/compound/all"
ANNOTATIONDB_TIMEOUT_SECONDS = 300.0
ANNOTATIONDB_RETRIES = 5
ANNOTATIONDB_BACKOFF_SECONDS = 5.0
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
    "Metadata_Display_Name",
    "Metadata_InChI",
    "Metadata_SMILES",
    "Metadata_Compound_Source_Count",
    "Metadata_Compound_Sources",
    "AnnotationDB_CID",
    "AnnotationDB_Name",
    "AnnotationDB_SMILES",
    "AnnotationDB_Has_Match",
]


def validate_columns(frame, required_columns, label):
    missing_columns = [
        column for column in required_columns if column not in frame.columns
    ]
    if missing_columns:
        raise ValueError(
            f"{label} is missing required columns: {', '.join(missing_columns)}"
        )


def fetch_annotationdb_all(url):
    request = Request(url, headers={"User-Agent": "cellpainting-annotationdb/1.0"})

    for attempt in range(1, ANNOTATIONDB_RETRIES + 1):
        try:
            with urlopen(request, timeout=ANNOTATIONDB_TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            if not isinstance(payload, list):
                raise ValueError(
                    "AnnotationDB /compound/all did not return a JSON list"
                )
            return payload
        except Exception:
            if attempt == ANNOTATIONDB_RETRIES:
                raise
            sleep_seconds = ANNOTATIONDB_BACKOFF_SECONDS * attempt
            print(
                "[build_compound_metadata] "
                f"annotationdb attempt={attempt} failed; retrying in {sleep_seconds:.1f}s",
                flush=True,
            )
            time.sleep(sleep_seconds)


compound_master_path = Path(snakemake.input.compound_master)
output_path = Path(snakemake.output.compound_metadata)

compound_master = pl.read_csv(
    compound_master_path,
    separator="\t",
    null_values="NA",
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

print(
    f"[build_compound_metadata] reading AnnotationDB bulk compound index from {ANNOTATIONDB_URL}",
    flush=True,
)
annotationdb_payload = fetch_annotationdb_all(ANNOTATIONDB_URL)
annotationdb = (
    pl.DataFrame(annotationdb_payload)
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
            pl.coalesce([normalized_control_name, pl.col("AnnotationDB_Name")]).alias(
                "Metadata_Display_Name"
            ),
            pl.col("AnnotationDB_CID").is_not_null().alias("AnnotationDB_Has_Match"),
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
        pl.col("AnnotationDB_Has_Match").sum().alias("annotationdb_matches"),
    ]
).row(0, named=True)
print(
    "[build_compound_metadata] "
    f"compound_rows={match_counts['compound_rows']} "
    f"annotationdb_matches={match_counts['annotationdb_matches']} "
    f"output={output_path}",
    flush=True,
)
