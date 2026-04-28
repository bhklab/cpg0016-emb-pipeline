# CPG0016 Embedding Pipeline

This pipeline curates JUMP CPG0016 Cell Painting Gallery compound profiles into processed tables, a `MultiAssayExperiment`, and an MAE-derived tabular archive. Sources include the public `cellpainting-gallery` bucket, JUMP metadata files, assembled CellProfiler COMPOUND profiles, and AnnotationDB `/compound/all` for PubChem CID enrichment.

Run from the repository root:

```bash
pixi run snakemake --cores <n>
```

Edit `config/pipeline.yaml` to change source selection, model selection, paths, and the AnnotationDB API base URL.
