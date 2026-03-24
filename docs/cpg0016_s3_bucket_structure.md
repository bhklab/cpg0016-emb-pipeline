# cpg0016 JUMP S3 Bucket Structure

## Michael Tran
## 17 March 2026

This document summarizes folders of interest in the Cell Painting Gallery S3 bucket for working with the JUMP Cell Painting `cpg0016` dataset.

## Prefixes

The public bucket is [cellpainting-gallery](https://cellpainting-gallery.s3.amazonaws.com/).

For `cpg0016`, there are two relevant top-level prefixes:

- `cpg0016-jump/`: contains per-source data products (incl. images, CellProfiler outputs, and deep-learning outputs)
- `cpg0016-jump-assembled/`: contains assembled classical profile tables across plates and sources.

## Top level structure

### `cpg0016-jump/`

Organized by source:

- `source_1/`
- `source_2/`
- `source_3/`
- `source_4/`
- `source_5/`
- `source_6/`
- `source_7/`
- `source_8/`
- `source_9/`
- `source_10/`
- `source_11/`
- `source_13/`
- `source_15/`
- `source_all/`

Within each source, the subdirectories are:

- `images/`
- `workspace/`
- `workspace_dl/`

Notes:

- `source_15` exists, but `workspace_dl/` subtree is empty
- `source_all/` does not have a `workspace_dl/` subtree for `cpg0016`
  - mainly exposes harmonized metadata under `workspace/metadata_harmonized/`
- `source_all/workspace/metadata_harmonized/cpg0016-jump_draft.csv` is about 17.5 GiB

### `cpg0016-jump-assembled/`

Contains `source_all/workspace/profiles_assembled/` where the released assembled classical profile tables live.

## Subdirectories

### `workspace/`

`workspace/` contains outputs from the classical CellProfiler morphology pipeline.

Subtrees include:

- `workspace/profiles/`
- `workspace/load_data_csv/`
- `workspace/backend/`
- `workspace/analysis/`

`workspace/profiles/` contains per-plate parquet tables.

Sample file has:

- 384 rows x 4765 columns
- metadata columns such as `Metadata_Source`, `Metadata_Plate`, `Metadata_Well`
- interpretable morphology features such as `Cells_AreaShape_*` and `Nuclei_Texture_*`

This is classical morphology data, not deep-learning embeddings.

### `workspace_dl/`

`workspace_dl/` contains deep-learning-derived features.

Published data currently observed is:

- `embeddings/`
- `profiles/`

## Per-source deep-learning models

The common `workspace_dl` model folders are:

- `cpcnn_zenodo_7114558`
- `efficientnet_v2_imagenet21k_s_feature_vector_2_0260bc96`

Notes:

- `source_4` additionally contains `cpdistiller_mesmer_s41467_025_62193_z` model in profiles

### Classical per-plate profiles

Pattern: `cpg0016-jump/source_N/workspace/profiles/<batch>/<plate>/<plate>.parquet`

Example: `cpg0016-jump/source_4/workspace/profiles/2021_04_26_Batch1/BR00117035/BR00117035.parquet`

### Deep-learning per-plate profiles

Pattern: `cpg0016-jump/source_N/workspace_dl/profiles/<model>/<batch>/<plate>/<plate>.parquet`

Examples:

- `cpg0016-jump/source_4/workspace_dl/profiles/cpcnn_zenodo_7114558/2021_04_26_Batch1/BR00117035/BR00117035.parquet`
- `cpg0016-jump/source_4/workspace_dl/profiles/efficientnet_v2_imagenet21k_s_feature_vector_2_0260bc96/2021_04_26_Batch1/BR00117035/BR00117035.parquet`

### Deep-learning embeddings

`cpcnn` pattern: `cpg0016-jump/source_N/workspace_dl/embeddings/cpcnn_zenodo_7114558/<batch>/<plate>/<well>-<site>/embedding.parquet`

Example: `cpg0016-jump/source_4/workspace_dl/embeddings/cpcnn_zenodo_7114558/2021_04_26_Batch1/BR00117035/A04-08/embedding.parquet`

`efficientnet` pattern: `cpg0016-jump/source_N/workspace_dl/embeddings/efficientnet_v2_imagenet21k_s_feature_vector_2_0260bc96/<batch>/<plate>/<well>/embedding.parquet`

Example: `cpg0016-jump/source_4/workspace_dl/embeddings/efficientnet_v2_imagenet21k_s_feature_vector_2_0260bc96/2021_04_26_Batch1/BR00117035/A18/embedding.parquet`

Notes:

- Public docs describe `embeddings/` generically as well-site based
  - `cpcnn` follows that closely
  - `efficientnet` is stored as one parquet per well, with `site` retained inside the rows

## Deep Learning

### `workspace_dl/embeddings/`

Contains single-cell embeddings.

Examples:

- `cpcnn` embedding file:
  - 25 rows
  - one row per cell from a single site
  - columns include `source`, `batch`, `plate`, `well`, `site`, `nuclei_object_number`, `all_emb`
- `efficientnet` embedding file:
  - 745 rows
  - one row per cell
  - columns include `source`, `batch`, `plate`, `well`, `site`, `agp_emb`, `dna_emb`, `er_emb`, `mito_emb`, `rna_emb`

### `workspace_dl/profiles/`

Contains well-level aggregated deep-learning profiles.

- `cpcnn` profile file:
  - 384 rows
  - columns: `source`, `batch`, `plate`, `well`, `all_emb`
  - `all_emb` length: 672
- `efficientnet` profile file:
  - 384 rows
  - columns: `source`, `batch`, `plate`, `well`, `agp_emb`, `dna_emb`, `er_emb`, `mito_emb`, `rna_emb`
  - each channel embedding length: 1280

## Assembled profile tables

Classical assembled tables are under: `cpg0016-jump-assembled/source_all/workspace/profiles_assembled/`

Relevant released subsets include:

- `ORF`
- `CRISPR`
- `COMPOUND`
- `ALL`

For compounds:

- `profiles_var_mad_int_featselect_harmony.parquet`
  - processed, feature-selected, batch-corrected
  - size on S3: about 2.64 GiB
- `profiles_var_mad_int.parquet`
  - interpretable feature space
  - size on S3: about 11.28 GiB

## Key identifiers

Important metadata fields:

- `Metadata_Source`: data-generating center
- `Metadata_Batch`: acquisition batch
- `Metadata_Plate`: plate identifier
- `Metadata_Well`: well position
- `site`: image field within a well
- `Metadata_JCP2022`: JUMP perturbation ID, for example `JCP2022_085227`
