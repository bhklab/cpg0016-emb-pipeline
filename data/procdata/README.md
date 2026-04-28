# procdata

Processed intermediates live here. The pipeline assumes generated metadata under `procdata/metadata/`, prepared profile tables under model-specific subdirectories, and other intermediate assay inputs as needed.

Expected usage depends on configured sources and models. A full default run with CPCNN and CellProfiler can require several GB to tens of GB in this directory because CellProfiler keeps thousands of scalar morphology features. Files here are ignored by git.
