# Pipeline Notes

This documents notable decisions made during curation.

### 1. Exclude `source_4` and `source_13`

- `source_4` is dominated by `ORF` plates.
- `source_13` is dominated by `CRISPR` plates.
- Both sources contain only a small shared compound benchmark panel and were therefore removed from the default drug-focused source list.

Current curated source list:

- `source_1`
- `source_2`
- `source_3`
- `source_5`
- `source_6`
- `source_7`
- `source_8`
- `source_9`
- `source_10`
- `source_11`

### 2. Restrict the manifest to `COMPOUND` plates

- `selection.include_plate_types` is set to `COMPOUND`.
- This excludes non-drug plate classes such as `ORF` and `CRISPR`.
- This also excludes smaller benchmark or control-oriented plate classes such as `TARGET1`, `TARGET2`, `DMSO`, `POSCON8`, and `COMPOUND_EMPTY`.

This filter is applied during manifest construction, so excluded plate types are not downloaded by the pipeline.

### 3. Keep all compound wells in the processed output

- `selection.output_row_filter` is set to `all_compound_wells`.
- Operationally, this means:
  - `Metadata_Is_Compound == true`

This keeps:

- treatment compounds
- DMSO wells
- positive-control wells

This removes:

- empty wells
- `JCP2022_UNKNOWN` wells
- any remaining non-small-molecule perturbations

Downstream modeling should filter out controls using `Metadata_pert_type` by keeping rows where `Metadata_pert_type == "treatment"`.

### 4. Scope `compound_master.tsv` to the curated output

- `data/metadata/compound_master.tsv` is now filtered to compounds that actually appear in the curated well-level output.
- It is no longer the unfiltered union of every compound in the downloaded JUMP compound reference table.
- The well-level parquet keeps only `Metadata_InChIKey` as the well-to-drug join key.
- The structure fields (`Metadata_InChI`, `Metadata_SMILES`) and per-drug annotations are kept in the compound metadata tables rather than repeated on every well-profile row.

This keeps the metadata table aligned with the curated parquet.

## Why `TARGET2` is excluded

`TARGET2` is a small shared JUMP-Target compound benchmark panel designed for matching compound phenotypes against corresponding ORF and CRISPR perturbations. It is still a drug plate, but it is not part of the main large compound-screening plates, so it is excluded from the default drug-only pipeline.

If target-matching or compound-gene benchmarking becomes relevant again, the pipeline can be widened by allowing `TARGET2` in `selection.include_plate_types`.