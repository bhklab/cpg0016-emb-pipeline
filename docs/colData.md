# colData Columns

Final `colData` is built in `workflow/scripts/build_mae.R` from processed well profiles and processed compound metadata enriched with the cached AnnotationDB table. Columns are exported from the MAE to `data/results/cpg0016_emb_tables/colData.tsv`.

| Column | Type | Description | Computed from / origin |
| --- | --- | --- | --- |
| `Sample.ID` | character | Unique well-level sample key used by the MAE sample map. | `Metadata_Plate` and `Metadata_Well`, joined with `.`. |
| `JUMP.CP.ID` | character | Source JUMP Cell Painting compound identifier. | Well metadata `Metadata_JCP2022`; compound-level metadata is keyed on the same source field. |
| `Pubchem.CID` | integer | PubChem compound identifier used to join back to the base HDD. | AnnotationDB `cid`, joined through InChIKey in processed compound metadata. |
| `InChIKey` | character | Compound InChIKey used for metadata joining. | Processed compound metadata `Metadata_InChIKey`, falling back to well metadata `Metadata_InChIKey` if needed. |
| `JUMP.CP.SMILES` | character | Source SMILES retained from the JUMP compound metadata. | Processed compound metadata `Metadata_SMILES`. |
| `In.AnnotationDB` | logical | Whether the compound has an AnnotationDB match or PubChem CID. | Processed `In_AnnotationDB` flag OR non-missing `Pubchem.CID`. |
| `AnnotationDB.Name` | character | AnnotationDB compound name. | AnnotationDB `name` from the cached `/compound/all` response. |
| `AnnotationDB.SMILES` | character | AnnotationDB SMILES string. | AnnotationDB `smiles` from the cached `/compound/all` response. |
| `Source` | character | JUMP source collection. | Well profile `Metadata_Source`. |
| `Batch` | character | Source batch identifier. | Well profile `Metadata_Batch`. |
| `Plate` | character | Plate identifier. | Well profile `Metadata_Plate`. |
| `Well` | character | Well identifier. | Well profile `Metadata_Well`. |
| `Perturbation.Type` | character | Treatment/control class. | Well profile `Metadata_pert_type`. |
