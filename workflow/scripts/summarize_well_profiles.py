import argparse
from pathlib import Path
from typing import cast

import polars as pl


DEFAULT_INPUT_DIR = Path("data/procdata")
DEFAULT_OUTPUT_ROOT = Path("data/metadata")
IMPORTANT_NULL_COLUMNS = [
    "Metadata_JCP2022",
    "Metadata_PlateType",
    "Metadata_pert_type",
    "Metadata_InChIKey",
]
WELL_KEY_COLUMNS = [
    "Metadata_Source",
    "Metadata_Batch",
    "Metadata_Plate",
    "Metadata_Well",
]
TREATMENT_PERT_TYPE = "treatment"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Summarize a processed JUMP well-profile parquet with overall counts, "
            "per-source counts, and treatment-compound replicate statistics."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        help=(
            "Path to a processed *_well_profiles.parquet file. Defaults to the "
            "only matching file under data/procdata/."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Directory where summary TSVs will be written. Defaults to "
            "data/metadata/<input_stem>_summary/."
        ),
    )
    return parser.parse_args()


def treatment_filter():
    return pl.col("Metadata_Is_Compound") & (
        pl.col("Metadata_pert_type") == TREATMENT_PERT_TYPE
    )


def control_compound_filter():
    return pl.col("Metadata_Is_Compound") & (
        pl.col("Metadata_pert_type") != TREATMENT_PERT_TYPE
    )


def noncompound_filter():
    return ~pl.col("Metadata_Is_Compound")


def resolve_input(input_path):
    if input_path is not None:
        if not input_path.exists():
            raise FileNotFoundError(f"Input parquet does not exist: {input_path}")
        return input_path

    matches = sorted(DEFAULT_INPUT_DIR.glob("*_well_profiles.parquet"))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise FileNotFoundError(
            "No *_well_profiles.parquet files were found under data/procdata/. "
            "Pass --input explicitly."
        )

    options = "\n".join(f"- {path}" for path in matches)
    raise ValueError(
        "Multiple *_well_profiles.parquet files were found. Pass --input explicitly:\n"
        f"{options}"
    )


def resolve_output_dir(output_dir, input_path):
    if output_dir is not None:
        return output_dir

    return DEFAULT_OUTPUT_ROOT / f"{input_path.stem}_summary"


def sort_by_source(frame, extra_sort_columns=None, extra_descending=None):
    if "Metadata_Source" not in frame.columns:
        return frame

    sort_columns = ["_source_order", "Metadata_Source"]
    descending = [False, False]
    if extra_sort_columns:
        sort_columns.extend(extra_sort_columns)
        descending.extend(extra_descending or [False] * len(extra_sort_columns))

    return (
        frame.with_columns(
            pl.col("Metadata_Source")
            .str.extract(r"(\d+)$")
            .cast(pl.Int64)
            .alias("_source_order")
        )
        .sort(sort_columns, descending=descending)
        .drop("_source_order")
    )


def write_section(output_dir, title, frame):
    output_path = output_dir / f"{title}.tsv"
    frame.write_csv(output_path, separator="\t")
    return output_path


def metric_frame(metrics):
    return pl.DataFrame(
        {
            "metric": list(metrics.keys()),
            "value": [str(value) for value in metrics.values()],
        }
    )


def collect_input_summary(lazy_frame, input_path):
    schema = lazy_frame.collect_schema()
    embedding_columns = [name for name in schema.names() if name.endswith("_emb")]

    embedding_width = 0
    if embedding_columns:
        lengths = (
            cast(
                pl.DataFrame,
                pl.scan_parquet(input_path)
                .select(
                    [
                        pl.col(column).list.len().first().alias(column)
                        for column in embedding_columns
                    ]
                )
                .collect(),
            )
            .row(0, named=True)
        )
        embedding_width = sum(int(length) for length in lengths.values())

    return metric_frame(
        {
            "input_path": input_path,
            "embedding_columns": ",".join(embedding_columns)
            if embedding_columns
            else "",
            "embedding_column_count": len(embedding_columns),
            "embedding_width_total": embedding_width,
        }
    )


def collect_overall_metrics(lazy_frame):
    treatment = treatment_filter()
    control_compound = control_compound_filter()
    noncompound = noncompound_filter()

    metrics = (
        cast(
            pl.DataFrame,
            lazy_frame.select(
                [
                    pl.len().alias("rows"),
                    pl.struct(WELL_KEY_COLUMNS).n_unique().alias("unique_well_keys"),
                    (pl.len() - pl.struct(WELL_KEY_COLUMNS).n_unique()).alias(
                        "duplicate_well_keys"
                    ),
                    pl.col("Metadata_Source").n_unique().alias("sources"),
                    pl.col("Metadata_Batch").n_unique().alias("batches"),
                    pl.col("Metadata_Plate").n_unique().alias("plates"),
                    pl.col("Metadata_JCP2022").n_unique().alias("jcp_ids_total"),
                    treatment.sum().alias("treatment_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(treatment)
                    .n_unique()
                    .alias("treatment_drugs"),
                    control_compound.sum().alias("control_compound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(control_compound)
                    .n_unique()
                    .alias("control_compounds"),
                    noncompound.sum().alias("noncompound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(noncompound)
                    .n_unique()
                    .alias("noncompound_jcp_ids"),
                ]
            )
            .collect(),
        )
        .row(0, named=True)
    )

    return metric_frame(metrics)


def collect_category_counts(lazy_frame):
    treatment = treatment_filter()
    control_compound = control_compound_filter()
    noncompound = noncompound_filter()

    counts = (
        cast(
            pl.DataFrame,
            lazy_frame.select(
                [
                    pl.len().alias("rows_total"),
                    treatment.sum().alias("treatment_compound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(treatment)
                    .n_unique()
                    .alias("treatment_compound_ids"),
                    control_compound.sum().alias("control_compound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(control_compound)
                    .n_unique()
                    .alias("control_compound_ids"),
                    noncompound.sum().alias("noncompound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(noncompound)
                    .n_unique()
                    .alias("noncompound_ids"),
                ]
            )
            .collect(),
        )
        .row(0, named=True)
    )

    rows_total = int(counts["rows_total"])
    return pl.DataFrame(
        {
            "category": [
                "treatment_compound",
                "control_compound",
                "noncompound",
            ],
            "rows": [
                counts["treatment_compound_rows"],
                counts["control_compound_rows"],
                counts["noncompound_rows"],
            ],
            "row_pct": [
                round(counts["treatment_compound_rows"] / rows_total * 100, 2),
                round(counts["control_compound_rows"] / rows_total * 100, 2),
                round(counts["noncompound_rows"] / rows_total * 100, 2),
            ],
            "unique_jcp_ids": [
                counts["treatment_compound_ids"],
                counts["control_compound_ids"],
                counts["noncompound_ids"],
            ],
        }
    )


def collect_perturbation_counts(lazy_frame):
    rows_total = cast(
        pl.DataFrame,
        lazy_frame.select(pl.len().alias("rows_total")).collect(),
    ).item()

    return (
        cast(
            pl.DataFrame,
            lazy_frame.group_by("Metadata_pert_type")
            .agg(
                [
                    pl.len().alias("rows"),
                    pl.col("Metadata_JCP2022").n_unique().alias("unique_jcp_ids"),
                    pl.col("Metadata_Is_Compound").sum().alias("compound_rows"),
                    (~pl.col("Metadata_Is_Compound")).sum().alias("noncompound_rows"),
                ]
            )
            .with_columns(
                [
                    pl.col("Metadata_pert_type")
                    .fill_null("null")
                    .alias("Metadata_pert_type"),
                    (pl.col("rows") / rows_total * 100).round(2).alias("row_pct"),
                ]
            )
            .sort("rows", descending=True)
            .collect(),
        )
    )


def collect_plate_type_counts(lazy_frame):
    rows_total = cast(
        pl.DataFrame,
        lazy_frame.select(pl.len().alias("rows_total")).collect(),
    ).item()

    return (
        cast(
            pl.DataFrame,
            lazy_frame.group_by("Metadata_PlateType")
            .agg(
                [
                    pl.len().alias("rows"),
                    pl.col("Metadata_Plate").n_unique().alias("plates"),
                    pl.col("Metadata_Source").n_unique().alias("sources"),
                ]
            )
            .with_columns((pl.col("rows") / rows_total * 100).round(2).alias("row_pct"))
            .sort("rows", descending=True)
            .collect(),
        )
    )


def collect_null_counts(lazy_frame):
    schema = lazy_frame.collect_schema()
    available_columns = [
        column for column in IMPORTANT_NULL_COLUMNS if column in schema.names()
    ]
    counts = (
        cast(
            pl.DataFrame,
            lazy_frame.select(
                [
                    pl.col(column).null_count().alias(column)
                    for column in available_columns
                ]
            ).collect(),
        )
        .row(0, named=True)
    )
    return metric_frame(counts)


def collect_source_summary(lazy_frame):
    treatment = treatment_filter()
    control_compound = control_compound_filter()
    noncompound = noncompound_filter()

    frame = (
        cast(
            pl.DataFrame,
            lazy_frame.group_by("Metadata_Source")
            .agg(
                [
                    pl.len().alias("rows"),
                    pl.col("Metadata_Batch").n_unique().alias("batches"),
                    pl.col("Metadata_Plate").n_unique().alias("plates"),
                    treatment.sum().alias("treatment_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(treatment)
                    .n_unique()
                    .alias("treatment_drugs"),
                    control_compound.sum().alias("control_compound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(control_compound)
                    .n_unique()
                    .alias("control_compounds"),
                    noncompound.sum().alias("noncompound_rows"),
                    pl.col("Metadata_JCP2022")
                    .filter(noncompound)
                    .n_unique()
                    .alias("noncompound_jcp_ids"),
                ]
            )
            .with_columns(
                [
                    (pl.col("rows") / pl.col("plates")).round(2).alias("wells_per_plate"),
                    (pl.col("treatment_rows") / pl.col("rows") * 100)
                    .round(2)
                    .alias("treatment_row_pct"),
                    (pl.col("control_compound_rows") / pl.col("rows") * 100)
                    .round(2)
                    .alias("control_compound_row_pct"),
                    (pl.col("noncompound_rows") / pl.col("rows") * 100)
                    .round(2)
                    .alias("noncompound_row_pct"),
                ]
            )
            .collect(),
        )
    )

    return sort_by_source(frame)


def collect_plate_type_by_source(lazy_frame):
    frame = (
        cast(
            pl.DataFrame,
            lazy_frame.group_by(["Metadata_Source", "Metadata_PlateType"])
            .agg(
                [
                    pl.len().alias("rows"),
                    pl.col("Metadata_Plate").n_unique().alias("plates"),
                ]
            )
            .collect(),
        )
    )
    return sort_by_source(frame, extra_sort_columns=["rows"], extra_descending=[True])


def collect_treatment_replicates_by_source(lazy_frame):
    treatment = treatment_filter()

    frame = (
        cast(
            pl.DataFrame,
            lazy_frame.filter(treatment)
            .group_by(["Metadata_Source", "Metadata_JCP2022"])
            .agg(pl.len().alias("replicates"))
            .group_by("Metadata_Source")
            .agg(
                [
                    pl.len().alias("treatment_drugs"),
                    pl.col("replicates").mean().round(3).alias("mean_replicates_per_drug"),
                    pl.col("replicates").median().alias("median_replicates_per_drug"),
                    pl.col("replicates").min().alias("min_replicates_per_drug"),
                    pl.col("replicates").quantile(0.9).alias("p90_replicates_per_drug"),
                    pl.col("replicates").max().alias("max_replicates_per_drug"),
                    (pl.col("replicates") == 1).sum().alias("singleton_drugs"),
                ]
            )
            .with_columns(
                (pl.col("singleton_drugs") / pl.col("treatment_drugs") * 100)
                .round(2)
                .alias("singleton_drug_pct")
            )
            .collect(),
        )
    )

    return sort_by_source(frame)


def collect_treatment_replicates_overall(lazy_frame):
    treatment = treatment_filter()

    metrics = (
        cast(
            pl.DataFrame,
            lazy_frame.filter(treatment)
            .group_by("Metadata_JCP2022")
            .agg(
                [
                    pl.len().alias("total_replicates"),
                    pl.col("Metadata_Source").n_unique().alias("sources_per_drug"),
                ]
            )
            .select(
                [
                    pl.len().alias("treatment_drugs"),
                    pl.col("total_replicates")
                    .mean()
                    .round(3)
                    .alias("mean_total_replicates"),
                    pl.col("total_replicates").median().alias("median_total_replicates"),
                    pl.col("total_replicates").min().alias("min_total_replicates"),
                    pl.col("total_replicates").quantile(0.9).alias("p90_total_replicates"),
                    pl.col("total_replicates").max().alias("max_total_replicates"),
                    pl.col("sources_per_drug")
                    .mean()
                    .round(3)
                    .alias("mean_sources_per_drug"),
                    pl.col("sources_per_drug").median().alias("median_sources_per_drug"),
                    pl.col("sources_per_drug").max().alias("max_sources_per_drug"),
                ]
            )
            .collect(),
        )
        .row(0, named=True)
    )

    return metric_frame(metrics)


def collect_treatment_source_overlap(lazy_frame):
    treatment = treatment_filter()

    return (
        cast(
            pl.DataFrame,
            lazy_frame.filter(treatment)
            .group_by("Metadata_JCP2022")
            .agg(pl.col("Metadata_Source").n_unique().alias("sources_per_drug"))
            .group_by("sources_per_drug")
            .agg(pl.len().alias("treatment_drugs"))
            .with_columns(
                (pl.col("treatment_drugs") / pl.col("treatment_drugs").sum() * 100)
                .round(2)
                .alias("drug_pct")
            )
            .sort("sources_per_drug")
            .collect(),
        )
    )


def collect_run_info(input_path, output_dir):
    return metric_frame(
        {
            "input_path": input_path,
            "output_dir": output_dir,
            "treatment_compound_definition": (
                f"Metadata_Is_Compound and Metadata_pert_type == '{TREATMENT_PERT_TYPE}'"
            ),
            "control_compound_definition": (
                f"Metadata_Is_Compound and Metadata_pert_type != '{TREATMENT_PERT_TYPE}'"
            ),
            "noncompound_definition": (
                "Metadata_Is_Compound is false; includes empty wells and "
                "non-small-molecule perturbations"
            ),
        }
    )


def main():
    args = parse_args()
    input_path = resolve_input(args.input)
    output_dir = resolve_output_dir(args.output_dir, input_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    lazy_frame = pl.scan_parquet(input_path)

    sections = [
        ("run_info", collect_run_info(input_path, output_dir)),
        ("input_summary", collect_input_summary(lazy_frame, input_path)),
        ("overall_metrics", collect_overall_metrics(lazy_frame)),
        ("category_counts", collect_category_counts(lazy_frame)),
        ("perturbation_type_counts", collect_perturbation_counts(lazy_frame)),
        ("plate_type_counts", collect_plate_type_counts(lazy_frame)),
        ("null_counts", collect_null_counts(lazy_frame)),
        ("source_summary", collect_source_summary(lazy_frame)),
        ("plate_type_by_source", collect_plate_type_by_source(lazy_frame)),
        (
            "treatment_replicates_by_source",
            collect_treatment_replicates_by_source(lazy_frame),
        ),
        (
            "treatment_replicates_overall",
            collect_treatment_replicates_overall(lazy_frame),
        ),
        ("treatment_source_overlap", collect_treatment_source_overlap(lazy_frame)),
    ]

    written_paths = [
        write_section(output_dir, title, frame) for title, frame in sections
    ]

    print(
        "[summarize_well_profiles] "
        f"input={input_path} "
        f"output_dir={output_dir} "
        f"written_tsvs={len(written_paths)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
