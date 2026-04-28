# MAE Structure

The final object is written to `data/results/cpg0016_emb_MultiAssayExperiment.rds`. It contains one `SummarizedExperiment` per selected model.

| Component | Structure |
| --- | --- |
| Assays | `cpcnn` and `cellprofiler` by default. `cpcnn` stores embedding vector positions as rows, and `cellprofiler` stores scalar morphology features as rows. |
| Assay columns | `Sample.ID`, a well-level key built from plate and well. |
| Assay rows | `cpcnn` rows are embedding vector positions from `all_emb`. `cellprofiler` rows are numeric CellProfiler feature columns from the assembled COMPOUND profile table. |
| sampleMap | One row per assay/sample pair with `assay`, `primary`, and `colname`; both `primary` and `colname` are `Sample.ID`. |
| colData | Well-level metadata. Each row is a profiled well and is keyed by `Sample.ID`. |
| rowData | Feature metadata with `Feature.ID`, `Feature.Source`, `Feature.Index`, `Profile.Model`, and `Assay.Name`. |

## metadata(mae)

| Object | Source | Purpose |
| --- | --- | --- |
| `Pipeline` | `config/pipeline.yaml` and Snakefile params | Named list containing `ID`, `Version`, and parsed run `Config`. |
| `Drug.Metadata` | `data/procdata/metadata/drug_metadata_raw.tsv` plus AnnotationDB cache | Harmonized compound metadata keyed by `JUMP.CP.ID`. |
