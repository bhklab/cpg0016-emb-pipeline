# JUMP-CP Deep-Learning Profiles Pipeline

This pipeline curates JUMP well-level deep-learning profiles from the public [`cellpainting-gallery`](https://cellpainting-gallery.s3.amazonaws.com/index.html#cpg0016-jump/) AWS bucket and builds a model-specific well-level table, a shared compound metadata table, and a CPCNN MultiAssayExperiment export.

The pipeline is configured use the CPCNN model (`cpcnn_zenodo_7114558`) embeddings by default. The EfficientNet model (`efficientnet_v2_imagenet21k_s_feature_vector_2_0260bc96`) is also an option for stain-level embeddings, at the cost of larger download size.

NOTE: The MAE export step is does not currently support the EfficientNet model embeddings.

The workflow builds:

- a final MAE export at `data/procdata/cpg0016_cpcnn_mae.rds`
- an intermediate well-level master table at `data/procdata/cpg0016_cpcnn_well_profiles.parquet`
- JCP-keyed and InChIKey-keyed compound metadata tables at `data/metadata/`

## Metadata Assembly

The well-level parquet is assembled by `workflow/scripts/process_profiles.py`:

- raw profile parquet contributes: `source`, `batch`, `plate`, `well`, plus whichever embedding columns the selected model exposes
- `well.csv.gz` contributes `Metadata_JCP2022`
- `plate.csv.gz` contributes `Metadata_PlateType`
- `perturbation_control.csv` contributes `Metadata_pert_type` for control compounds; treatment compounds are filled as `Metadata_pert_type = treatment`
- `compound.csv.gz` contributes `Metadata_InChIKey`, `Metadata_InChI`, and `Metadata_SMILES`
- the well-level parquet keeps only the `Metadata_InChIKey` join key on each row
- `data/metadata/compound_master.tsv` keeps the JCP-keyed structure fields and control name metadata
- `data/metadata/compound_metadata.tsv` is the JCP-keyed per-drug table used by the MAE export and enriched from the AnnotationDB API endpoint `/compound/all`

`compound_source.csv.gz` is used to build the JCP-keyed compound tables, not repeated on every well row.

The MAE export is assembled by `workflow/scripts/build_mae.R`:

- `sample_id` is built as `Metadata_Plate.Metadata_Well`
- `colData` keeps curated per-well fields plus both `Metadata_JCP2022` and `Metadata_InChIKey`
- per-drug metadata is stored once at `metadata(mae)$compound_metadata`, keyed by `Metadata_JCP2022`
- the CPCNN `all_emb` list column is materialized as one `SummarizedExperiment` named `all_emb` with assay name `embedding`
- the final object is saved as a single `MultiAssayExperiment` to RDS file

## Usage

Modify the pipeline config at `config/pipeline.yaml`, and:

Dry run:

```bash
pixi run snakemake -n
```

Run:

```bash
pixi run snakemake --cores <N>
```

## Utilities

- `workflow/scripts/find_annotationdb_missing_compounds.py`: This script queries all compounds from AnnotationDB and reports a list of compounds for which no metadata was found.