suppressPackageStartupMessages({
  library(arrow)
  library(MultiAssayExperiment)
  library(S4Vectors)
  library(SummarizedExperiment)
  library(yaml)
})

set_vector_memory_limit <- function() {
  if (!exists("mem.maxVSize", mode = "function")) {
    return(invisible(NULL))
  }
  target_gb <- Sys.getenv("PIPELINE_R_MAX_VSIZE_GB", "64")
  target_mb <- suppressWarnings(as.numeric(target_gb) * 1024)
  if (tolower(target_gb) %in% c("inf", "infinite")) {
    target_mb <- Inf
  }
  if (is.na(target_mb)) {
    return(invisible(NULL))
  }
  current_mb <- mem.maxVSize()
  if (
    !is.finite(current_mb) || is.infinite(target_mb) || current_mb < target_mb
  ) {
    try(mem.maxVSize(target_mb), silent = TRUE)
  }
  invisible(NULL)
}

set_vector_memory_limit()

stopf <- function(message, ...) {
  stop(sprintf(message, ...), call. = FALSE)
}

split_param <- function(value) {
  if (is.null(value) || !nzchar(value)) {
    return(character())
  }
  strsplit(value, ",", fixed = TRUE)[[1L]]
}

read_tsv <- function(path) {
  utils::read.delim(
    path,
    sep = "\t",
    quote = "\"",
    comment.char = "",
    na.strings = "NA",
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
}

normalize_blank_to_na <- function(values) {
  values <- as.character(values)
  blank <- !is.na(values) & !nzchar(trimws(values))
  values[blank] <- NA_character_
  values
}

first_non_missing <- function(...) {
  values <- list(...)
  out <- rep(NA_character_, length(values[[1L]]))
  for (value in values) {
    value <- normalize_blank_to_na(value)
    replace <- is.na(out) & !is.na(value)
    out[replace] <- value[replace]
  }
  out
}

as_logical_flag <- function(values) {
  if (is.logical(values)) {
    return(values)
  }
  lowered <- tolower(trimws(as.character(values)))
  out <- lowered %in% c("true", "t", "1", "yes")
  out[is.na(values) | !nzchar(lowered)] <- FALSE
  out
}

as_integer_or_na <- function(values) {
  suppressWarnings(as.integer(as.character(values)))
}

build_sample_ids <- function(frame) {
  paste(frame$Metadata_Plate, frame$Metadata_Well, sep = ".")
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

parquet_column_names <- function(path) {
  names(arrow::open_dataset(path))
}

read_parquet_columns <- function(path, columns) {
  arrow::read_parquet(path, col_select = tidyselect::all_of(columns)) |>
    as.data.frame(stringsAsFactors = FALSE)
}

build_public_drug_metadata <- function(compound_metadata) {
  validate_required_columns(
    names(compound_metadata),
    c("Metadata_JCP2022", "Metadata_InChIKey"),
    "compound_metadata.tsv"
  )

  jump_ids <- as.character(compound_metadata$Metadata_JCP2022)
  if (any(is.na(jump_ids) | !nzchar(trimws(jump_ids)))) {
    stopf("CPG0016 Metadata_JCP2022 values must be non-missing")
  }
  if (anyDuplicated(jump_ids) > 0L) {
    stopf("CPG0016 Metadata_JCP2022 values must be unique")
  }

  pubchem_cid <- as_integer_or_na(compound_metadata$AnnotationDB_CID)
  public <- data.frame(
    JUMP.CP.ID = jump_ids,
    Pubchem.CID = pubchem_cid,
    InChIKey = as.character(compound_metadata$Metadata_InChIKey),
    JUMP.CP.SMILES = as.character(compound_metadata$Metadata_SMILES),
    In.AnnotationDB = as_logical_flag(compound_metadata$In_AnnotationDB) |
      !is.na(pubchem_cid),
    AnnotationDB.Name = as.character(compound_metadata$AnnotationDB_Name),
    AnnotationDB.SMILES = as.character(compound_metadata$AnnotationDB_SMILES),
    Perturbation.Type = as.character(compound_metadata$Metadata_pert_type),
    Control.Name = as.character(compound_metadata$Metadata_Control_Name),
    JUMP.CP.Compound.Source.Count = as_integer_or_na(
      compound_metadata$Metadata_Compound_Source_Count
    ),
    JUMP.CP.Compound.Sources = as.character(
      compound_metadata$Metadata_Compound_Sources
    ),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  row.names(public) <- public$JUMP.CP.ID
  public
}

build_public_coldata <- function(well_metadata, drug_metadata) {
  sample_id <- build_sample_ids(well_metadata)
  public <- data.frame(
    Sample.ID = sample_id,
    Source = as.character(well_metadata$Metadata_Source),
    Batch = as.character(well_metadata$Metadata_Batch),
    Plate = as.character(well_metadata$Metadata_Plate),
    Well = as.character(well_metadata$Metadata_Well),
    JUMP.CP.ID = as.character(well_metadata$Metadata_JCP2022),
    InChIKey = as.character(well_metadata$Metadata_InChIKey),
    Perturbation.Type = as.character(well_metadata$Metadata_pert_type),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  match_idx <- match(public$JUMP.CP.ID, drug_metadata$JUMP.CP.ID)
  public$Pubchem.CID <- drug_metadata$Pubchem.CID[match_idx]
  public$InChIKey <- first_non_missing(
    drug_metadata$InChIKey[match_idx],
    public$InChIKey
  )
  public$JUMP.CP.SMILES <- drug_metadata$JUMP.CP.SMILES[match_idx]
  public$In.AnnotationDB <- drug_metadata$In.AnnotationDB[match_idx]
  public$AnnotationDB.Name <- drug_metadata$AnnotationDB.Name[match_idx]
  public$AnnotationDB.SMILES <- drug_metadata$AnnotationDB.SMILES[match_idx]

  ordered_columns <- c(
    "Sample.ID",
    "JUMP.CP.ID",
    "Pubchem.CID",
    "InChIKey",
    "JUMP.CP.SMILES",
    "In.AnnotationDB",
    "AnnotationDB.Name",
    "AnnotationDB.SMILES"
  )
  public[,
    c(ordered_columns, setdiff(names(public), ordered_columns)),
    drop = FALSE
  ]
}

rbind_fill <- function(frames) {
  all_columns <- unique(unlist(lapply(frames, names), use.names = FALSE))
  aligned <- lapply(frames, function(frame) {
    missing <- setdiff(all_columns, names(frame))
    for (column in missing) {
      frame[[column]] <- NA
    }
    frame[, all_columns, drop = FALSE]
  })
  do.call(rbind, aligned)
}

is_list_feature <- function(values) {
  is.list(values) && !is.data.frame(values)
}

build_assay_from_profile <- function(
  profile_path,
  metadata_columns,
  sample_id,
  assay_name,
  profile_model
) {
  feature_columns <- setdiff(
    parquet_column_names(profile_path),
    metadata_columns
  )
  if (!length(feature_columns)) {
    stopf("No assay feature columns found for %s", assay_name)
  }

  matrices <- list()
  row_data <- list()

  for (feature_column in feature_columns) {
    feature_frame <- read_parquet_columns(profile_path, feature_column)
    values <- feature_frame[[feature_column]]
    feature_frame[[feature_column]] <- NULL

    if (is_list_feature(values)) {
      widths <- unique(vapply(values, length, integer(1)))
      if (length(widths) != 1L) {
        stopf(
          "Feature column %s has inconsistent vector lengths",
          feature_column
        )
      }
      n_values <- length(values)
      flat_values <- unlist(values, use.names = FALSE)
      rm(values, feature_frame)
      gc()
      mat <- matrix(
        as.numeric(flat_values),
        nrow = widths[[1L]],
        ncol = n_values,
        byrow = FALSE
      )
      rm(flat_values)
      row_names <- sprintf("%s_%04d", feature_column, seq_len(nrow(mat)))
    } else if (is.numeric(values) || is.integer(values) || is.logical(values)) {
      numeric_values <- as.numeric(values)
      rm(values, feature_frame)
      mat <- matrix(numeric_values, nrow = 1L)
      rm(numeric_values)
      row_names <- feature_column
    } else {
      rm(values, feature_frame)
      next
    }

    rownames(mat) <- row_names
    colnames(mat) <- sample_id
    matrices[[feature_column]] <- mat
    row_data[[feature_column]] <- data.frame(
      Feature.ID = row_names,
      Feature.Source = feature_column,
      Feature.Index = seq_along(row_names),
      Profile.Model = profile_model,
      Assay.Name = assay_name,
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  }

  if (!length(matrices)) {
    stopf("No numeric or vector assay features found for %s", assay_name)
  }

  assay_matrix <- do.call(rbind, matrices)
  row_data <- do.call(rbind, row_data)
  row.names(row_data) <- row_data$Feature.ID

  SummarizedExperiment::SummarizedExperiment(
    assays = S4Vectors::SimpleList(values = assay_matrix),
    rowData = S4Vectors::DataFrame(row_data, row.names = row.names(row_data)),
    colData = S4Vectors::DataFrame(row.names = colnames(assay_matrix))
  )
}

well_profile_paths <- unname(snakemake@input[["well_profiles"]])
compound_metadata_path <- snakemake@input[["compound_metadata"]]
config_path <- snakemake@input[["configfile"]]
output_path <- snakemake@output[["mae_rds"]]

dataset_id <- snakemake@params[["dataset_id"]]
dataset_version <- snakemake@params[["dataset_version"]]
profile_models <- split_param(snakemake@params[["profile_models"]])
assay_names <- split_param(snakemake@params[["assay_names"]])

if (length(well_profile_paths) != length(assay_names)) {
  stopf("well_profiles and assay_names lengths do not match")
}

compound_metadata <- read_tsv(compound_metadata_path)
if (anyDuplicated(compound_metadata$Metadata_JCP2022) > 0L) {
  stopf("compound_metadata.tsv contains duplicate Metadata_JCP2022 rows")
}
drug_metadata <- build_public_drug_metadata(compound_metadata)

experiments <- list()
coldata_frames <- list()
sample_map_frames <- list()

for (idx in seq_along(well_profile_paths)) {
  profile_path <- well_profile_paths[[idx]]
  assay_name <- assay_names[[idx]]
  profile_model <- profile_models[[idx]]

  cat(sprintf("[build_mae] reading %s\n", profile_path))
  profile_columns <- parquet_column_names(profile_path)
  metadata_columns <- grep("^Metadata_", profile_columns, value = TRUE)
  frame <- read_parquet_columns(profile_path, metadata_columns)

  validate_required_columns(
    names(frame),
    c(
      "Metadata_Source",
      "Metadata_Batch",
      "Metadata_Plate",
      "Metadata_Well",
      "Metadata_JCP2022",
      "Metadata_InChIKey"
    ),
    profile_path
  )

  sample_id <- build_sample_ids(frame)
  if (anyDuplicated(sample_id) > 0L) {
    stopf("Duplicate Sample.ID values detected in %s", profile_path)
  }

  coldata_frames[[assay_name]] <- build_public_coldata(frame, drug_metadata)
  experiments[[assay_name]] <- build_assay_from_profile(
    profile_path = profile_path,
    metadata_columns = metadata_columns,
    sample_id = sample_id,
    assay_name = assay_name,
    profile_model = profile_model
  )
  sample_map_frames[[assay_name]] <- data.frame(
    assay = assay_name,
    primary = sample_id,
    colname = sample_id,
    stringsAsFactors = FALSE
  )
  rm(frame)
  gc()
}

col_data <- rbind_fill(unname(coldata_frames))
col_data <- col_data[!duplicated(col_data$Sample.ID), , drop = FALSE]
row.names(col_data) <- col_data$Sample.ID

sample_map <- do.call(rbind, sample_map_frames)
sample_map$assay <- factor(sample_map$assay, levels = names(experiments))

mae <- MultiAssayExperiment::MultiAssayExperiment(
  experiments = MultiAssayExperiment::ExperimentList(experiments),
  colData = S4Vectors::DataFrame(col_data, row.names = row.names(col_data)),
  sampleMap = S4Vectors::DataFrame(sample_map)
)

metadata(mae) <- list(
  Pipeline = list(
    ID = dataset_id,
    Version = dataset_version,
    Config = yaml::read_yaml(config_path, eval.expr = FALSE)
  ),
  Drug.Metadata = S4Vectors::DataFrame(
    drug_metadata,
    row.names = drug_metadata$JUMP.CP.ID
  )
)

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
cat(sprintf("[build_mae] writing MAE to %s\n", output_path))
saveRDS(mae, file = output_path)
