# Developer Notes

## Scope Decisions

- The default curation is the JUMP CPG0016 compound-screening subset, not every plate in the Cell Painting Gallery bucket.
- `source_4`, `source_13`, and `source_15` are excluded from the default selection because they are not part of the intended small-molecule drug subset or do not have the same published profile coverage.
- Only `COMPOUND` plate types are included by default. TARGET, ORF, CRISPR, and control-only plate classes remain outside the HDD-facing output.
- All compound wells are retained, including treatment wells, DMSO wells, and positive controls. The treatment/control distinction is preserved in `Perturbation.Type`.

## Identifier Decisions

- The source compound identifier is `JUMP.CP.ID`, derived from `Metadata_JCP2022`.
- The pipeline does not generate compound identifiers for public outputs.
- `Sample.ID` is generated from plate and well only for well-level MAE alignment.
- Public columns with HDD-shared names are intended to be directly joinable to the base HDD; source-specific fields use source-specific prefixes.

## Model Decisions

- `models` in `config/pipeline.yaml` is a plain list of selected model identifiers.
- `cpcnn_zenodo_7114558` and `efficientnet_v2_imagenet21k_s_feature_vector_2_0260bc96` are implemented now.
- `cellprofiler` and `cpdistiller_mesmer_s41467_025_62193_z` are documented as future model families. CellProfiler needs separate scalar morphology handling, and CPDistiller currently has source-limited coverage.
- Each selected model becomes a separate MAE assay with a stable Snakefile-defined assay name.

## Metadata Decisions

- Source `Metadata_*` fields are renamed to public dot-style names in final MAE `colData` and table exports.
- AnnotationDB enrichment is intentionally minimal: PubChem CID, AnnotationDB name, AnnotationDB SMILES, and a match flag.
- The AnnotationDB `/compound/all` response is cached once at `data/rawdata/metadata/all_adb_compounds.csv` and downstream joins read the cache.
- Derived display-name columns are omitted from public outputs; users can choose source or AnnotationDB names downstream.
