library(arrow)
library(MultiAssayExperiment)
library(S4Vectors)
library(SummarizedExperiment)
library(yaml)

PROFILE_MODEL_CPCNN <- "cpcnn_zenodo_7114558"
EMBEDDING_COLUMN_CPCNN <- "all_emb"
DATASET_NAME <- "JUMP Cell Painting cpg0016 CPCNN Embedding MAE"
DATASET_VERSION <- "v1.1"
RAW_PROFILE_IDENTIFIER_COLUMNS <- c("source", "batch", "plate", "well")
WELL_METADATA_COLUMNS <- c(
  "Metadata_Source",
  "Metadata_Batch",
  "Metadata_Plate",
  "Metadata_Well",
  "Metadata_JCP2022",
  "Metadata_InChIKey",
  "Metadata_PlateType",
  "Metadata_pert_type",
  "Metadata_Is_Compound"
)
COL_DATA_COLUMNS <- c(
  "Metadata_Source",
  "Metadata_Batch",
  "Metadata_Plate",
  "Metadata_Well",
  "Metadata_JCP2022",
  "Metadata_InChIKey",
  "Metadata_pert_type"
)
COMPOUND_METADATA_KEY <- "Metadata_JCP2022"
PUBLIC_COMPOUND_METADATA_KEY <- "JUMP.CP.ID"
PUBLIC_COMPOUND_METADATA_RENAMES <- c(
  "Metadata_InChIKey" = "InChIKey",
  "Metadata_Display_Name" = "Molecule.Name",
  "Metadata_SMILES" = "SMILES",
  "AnnotationDB_CID" = "Pubchem.CID"
)

stopf <- function(message, ...) {
  stop(sprintf(message, ...), call. = FALSE)
}

read_tsv <- function(path) {
  utils::read.delim(
    path,
    sep = "\t",
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
}

normalize_blank_to_na <- function(values) {
  blank <- !is.na(values) & !nzchar(trimws(values))
  values[blank] <- NA_character_
  values
}

split_pipe_strings <- function(values) {
  IRanges::CharacterList(lapply(values, function(value) {
    if (is.na(value) || !nzchar(value)) {
      character()
    } else {
      strsplit(value, "|", fixed = TRUE)[[1L]]
    }
  }))
}

rename_columns <- function(frame, renames) {
  matched_columns <- intersect(names(renames), colnames(frame))
  if (length(matched_columns) == 0L) {
    return(frame)
  }

  colnames(frame)[match(matched_columns, colnames(frame))] <-
    unname(renames[matched_columns])
  frame
}

build_public_compound_metadata <- function(frame) {
  public_frame <- frame
  public_frame[[PUBLIC_COMPOUND_METADATA_KEY]] <- public_frame[[
    COMPOUND_METADATA_KEY
  ]]
  public_frame[["In.JUMP.CP"]] <- TRUE
  public_frame <- rename_columns(public_frame, PUBLIC_COMPOUND_METADATA_RENAMES)

  preferred_columns <- c(
    PUBLIC_COMPOUND_METADATA_KEY,
    COMPOUND_METADATA_KEY,
    "Pubchem.CID",
    "InChIKey",
    "SMILES",
    "Molecule.Name",
    "In.JUMP.CP"
  )
  ordered_columns <- c(
    preferred_columns[preferred_columns %in% colnames(public_frame)],
    setdiff(colnames(public_frame), preferred_columns)
  )
  public_frame[, ordered_columns, drop = FALSE]
}

build_sample_ids <- function(frame) {
  paste(
    frame$Metadata_Plate,
    frame$Metadata_Well,
    sep = "."
  )
}

build_raw_sample_ids <- function(frame) {
  paste(
    frame$plate,
    frame$well,
    sep = "."
  )
}

validate_required_columns <- function(columns, required_columns, label) {
  missing_columns <- setdiff(required_columns, columns)
  if (length(missing_columns) > 0L) {
    stopf(
      "%s is missing required columns: %s",
      label,
      paste(missing_columns, collapse = ", ")
    )
  }
}

fill_embedding_matrix <- function(
  raw_profile_paths,
  sample_index,
  sample_id,
  embedding_column,
  embedding_length
) {
  raw_profile_columns <- c(RAW_PROFILE_IDENTIFIER_COLUMNS, embedding_column)
  embedding_matrix <- matrix(
    NA_real_,
    nrow = embedding_length,
    ncol = length(sample_id)
  )
  filled <- logical(length(sample_id))

  for (profile_index in seq_along(raw_profile_paths)) {
    profile_path <- raw_profile_paths[[profile_index]]
    profile_frame <- arrow::read_parquet(
      profile_path,
      col_select = tidyselect::all_of(raw_profile_columns)
    ) |>
      as.data.frame()
    profile_sample_id <- build_raw_sample_ids(profile_frame)
    match_idx <- unname(sample_index[profile_sample_id])
    keep <- !is.na(match_idx)
    if (!any(keep)) {
      next
    }
    if (any(filled[match_idx[keep]])) {
      stopf(
        "Duplicate sample_id values were encountered while scanning raw profiles: %s",
        profile_path
      )
    }

    local_embeddings <- profile_frame[[embedding_column]][keep]
    local_lengths <- unique(vapply(local_embeddings, length, integer(1)))
    if (length(local_lengths) != 1L || local_lengths != embedding_length) {
      stopf(
        paste(
          "Raw profile %s does not match the expected %s embedding length of %d."
        ),
        profile_path,
        embedding_column,
        embedding_length
      )
    }

    local_matrix <- matrix(
      as.numeric(unlist(local_embeddings, use.names = FALSE)),
      nrow = embedding_length,
      ncol = sum(keep),
      byrow = FALSE
    )
    embedding_matrix[, match_idx[keep]] <- local_matrix
    filled[match_idx[keep]] <- TRUE

    if ((profile_index %% 100L) == 0L) {
      message(sprintf(
        "[build_mae] filled %d/%d raw profile files",
        profile_index,
        length(raw_profile_paths)
      ))
      gc()
    }
  }

  if (!all(filled)) {
    stopf(
      "Embedding matrix is missing %d sample columns after scanning raw profiles",
      sum(!filled)
    )
  }

  embedding_matrix
}

profile_model <- snakemake@params[["profile_model"]]
model_stem <- snakemake@params[["model_stem"]]

if (!identical(profile_model, PROFILE_MODEL_CPCNN)) {
  stopf(
    paste(
      "Single-RDS MAE export is only supported for %s.",
      "Current dataset.profile_model is '%s'.",
      "EfficientNet requires backed storage."
    ),
    PROFILE_MODEL_CPCNN,
    profile_model
  )
}

well_profiles_path <- snakemake@input[["well_profiles"]]
compound_metadata_path <- snakemake@input[["compound_metadata"]]
manifest_path <- snakemake@input[["manifest_tsv"]]
manifest_summary_path <- snakemake@input[["summary_tsv"]]
raw_profile_paths <- unname(snakemake@input[["raw_profiles"]])
config_path <- snakemake@input[["configfile"]]
output_path <- snakemake@output[["mae_rds"]]

dataset <- arrow::open_dataset(well_profiles_path, format = "parquet")
all_columns <- names(dataset)
embedding_columns <- all_columns[endsWith(all_columns, "_emb")]

validate_required_columns(
  all_columns,
  WELL_METADATA_COLUMNS,
  "Well profile parquet"
)
if (!identical(embedding_columns, EMBEDDING_COLUMN_CPCNN)) {
  stopf(
    paste(
      "Expected exactly one embedding column named '%s' in the CPCNN parquet,",
      "but found: %s"
    ),
    EMBEDDING_COLUMN_CPCNN,
    paste(embedding_columns, collapse = ", ")
  )
}

message(sprintf(
  "[build_mae] reading well metadata from %s",
  well_profiles_path
))
well_metadata <- arrow::read_parquet(
  well_profiles_path,
  col_select = tidyselect::all_of(WELL_METADATA_COLUMNS)
) |>
  as.data.frame()

sample_id <- build_sample_ids(well_metadata)
if (anyDuplicated(sample_id) > 0L) {
  stopf("Duplicate sample_id values detected in the curated well parquet")
}

compound_metadata <- read_tsv(compound_metadata_path)
validate_required_columns(
  names(compound_metadata),
  c(COMPOUND_METADATA_KEY, "Metadata_InChIKey"),
  "compound_metadata.tsv"
)
if ("Metadata_Control_Name" %in% names(compound_metadata)) {
  compound_metadata$Metadata_Control_Name <-
    normalize_blank_to_na(compound_metadata$Metadata_Control_Name)
}
if (
  "AnnotationDB_Has_Match" %in%
    names(compound_metadata) &&
    !("In_AnnotationDB" %in% names(compound_metadata))
) {
  names(compound_metadata)[
    names(compound_metadata) == "AnnotationDB_Has_Match"
  ] <- "In_AnnotationDB"
}
if ("In_AnnotationDB" %in% names(compound_metadata)) {
  compound_metadata$In_AnnotationDB <-
    tolower(as.character(compound_metadata$In_AnnotationDB)) == "true"
}
if (anyDuplicated(compound_metadata[[COMPOUND_METADATA_KEY]]) > 0L) {
  stopf("compound_metadata.tsv contains duplicate Metadata_JCP2022 rows")
}

compound_rows <- well_metadata$Metadata_Is_Compound %in% TRUE
missing_compound_keys <- compound_rows &
  (is.na(well_metadata$Metadata_InChIKey) |
    !nzchar(well_metadata$Metadata_InChIKey))
if (any(missing_compound_keys)) {
  stopf(
    "Well metadata contains %d compound rows without Metadata_InChIKey",
    sum(missing_compound_keys)
  )
}
compound_key_match <- match(
  unique(well_metadata$Metadata_JCP2022[compound_rows]),
  compound_metadata[[COMPOUND_METADATA_KEY]]
)
if (any(is.na(compound_key_match))) {
  stopf(
    paste(
      "Compound metadata join key validation failed for %d unique",
      "Metadata_JCP2022 values"
    ),
    sum(is.na(compound_key_match))
  )
}

col_data_frame <- well_metadata[, COL_DATA_COLUMNS, drop = FALSE]
row.names(col_data_frame) <- sample_id
col_data <- S4Vectors::DataFrame(col_data_frame, row.names = sample_id)

compound_metadata_row_names <- compound_metadata[[COMPOUND_METADATA_KEY]]
public_compound_metadata <- build_public_compound_metadata(compound_metadata)
row.names(public_compound_metadata) <- compound_metadata_row_names
compound_metadata_df <- S4Vectors::DataFrame(
  public_compound_metadata,
  row.names = compound_metadata_row_names
)
if ("Metadata_Compound_Sources" %in% names(compound_metadata_df)) {
  compound_metadata_df$Metadata_Compound_Sources <-
    split_pipe_strings(as.character(
      compound_metadata_df$Metadata_Compound_Sources
    ))
}

message(sprintf(
  "[build_mae] deriving embedding length from %s",
  raw_profile_paths[[1L]]
))
first_raw_profile <- arrow::read_parquet(
  raw_profile_paths[[1L]],
  col_select = tidyselect::all_of(
    c(RAW_PROFILE_IDENTIFIER_COLUMNS, EMBEDDING_COLUMN_CPCNN)
  )
) |>
  as.data.frame()
embedding_length <- unique(
  vapply(first_raw_profile[[EMBEDDING_COLUMN_CPCNN]], length, integer(1))
)
if (length(embedding_length) != 1L) {
  stopf("Embedding vectors do not have a constant length across all wells")
}

sample_index <- seq_along(sample_id)
names(sample_index) <- sample_id

message(sprintf(
  "[build_mae] constructing %s assay from %d raw profile files",
  EMBEDDING_COLUMN_CPCNN,
  length(raw_profile_paths)
))
gc()
embedding_matrix <- fill_embedding_matrix(
  raw_profile_paths = raw_profile_paths,
  sample_index = sample_index,
  sample_id = sample_id,
  embedding_column = EMBEDDING_COLUMN_CPCNN,
  embedding_length = embedding_length
)
colnames(embedding_matrix) <- sample_id

if (ncol(embedding_matrix) != nrow(col_data)) {
  stopf(
    "Assay columns (%d) do not match colData rows (%d)",
    ncol(embedding_matrix),
    nrow(col_data)
  )
}

row_data <- S4Vectors::DataFrame(
  feature_index = seq_len(embedding_length),
  embedding_name = EMBEDDING_COLUMN_CPCNN,
  profile_model = profile_model
)
experiment_col_data <- S4Vectors::DataFrame(row.names = sample_id)

all_emb_experiment <- SummarizedExperiment::SummarizedExperiment(
  assays = S4Vectors::SimpleList(embedding = embedding_matrix),
  rowData = row_data,
  colData = experiment_col_data
)

sample_map <- S4Vectors::DataFrame(
  assay = factor(
    rep(EMBEDDING_COLUMN_CPCNN, length(sample_id)),
    levels = EMBEDDING_COLUMN_CPCNN
  ),
  primary = sample_id,
  colname = sample_id
)

mae <- MultiAssayExperiment::MultiAssayExperiment(
  experiments = MultiAssayExperiment::ExperimentList(
    setNames(list(all_emb_experiment), EMBEDDING_COLUMN_CPCNN)
  ),
  colData = col_data,
  sampleMap = sample_map
)

if (!identical(rownames(colData(mae)), sample_id)) {
  stopf("MultiAssayExperiment colData rownames do not match sample_id ordering")
}
if (!identical(colnames(all_emb_experiment), sample_id)) {
  stopf("Assay column names do not match sample_id ordering")
}

metadata(mae) <- list(
  dataset_name = DATASET_NAME,
  dataset_version = DATASET_VERSION,
  profile_model = profile_model,
  model_stem = model_stem,
  embedding_columns = embedding_columns,
  embedding_lengths = stats::setNames(
    as.integer(embedding_length),
    embedding_columns
  ),
  pipeline_config = yaml::read_yaml(config_path, eval.expr = FALSE),
  manifest = read_tsv(manifest_path),
  manifest_summary = read_tsv(manifest_summary_path),
  compound_metadata_key = PUBLIC_COMPOUND_METADATA_KEY,
  compound_metadata = compound_metadata_df
)

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
message(sprintf("[build_mae] writing MAE to %s", output_path))
saveRDS(mae, file = output_path)
