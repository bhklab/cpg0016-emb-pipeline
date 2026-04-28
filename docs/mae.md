# MAE Structure

The final object is written to `data/results/cpg0016_emb_MultiAssayExperiment.rds`. It contains one `SummarizedExperiment` per selected model.

| Component | Structure |
| --- | --- |
| Assays | `cpcnn` and `efficientnet` when both configured models are selected. Each assay stores embedding features as rows and profiled wells as columns. |
| Assay columns | `Sample.ID`, a well-level key built from plate and well. |
| Assay rows | Embedding vector positions or numeric feature columns from the processed profile parquet. |
| sampleMap | One row per assay/sample pair with `assay`, `primary`, and `colname`; both `primary` and `colname` are `Sample.ID`. |
| colData | Well-level metadata. Each row is a profiled well and is keyed by `Sample.ID`. |
| rowData | Feature metadata with `Feature.ID`, `Feature.Source`, `Feature.Index`, `Profile.Model`, and `Assay.Name`. |

## metadata(mae)

| Object | Source | Purpose |
| --- | --- | --- |
| `Pipeline` | `config/pipeline.yaml` and Snakefile params | Named list containing `ID`, `Version`, and parsed run `Config`. |
| `Drug.Metadata` | `data/procdata/metadata/drug_metadata_raw.tsv` plus AnnotationDB cache | Harmonized compound metadata keyed by `JUMP.CP.ID`. |
