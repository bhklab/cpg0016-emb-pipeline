suppressPackageStartupMessages({
  library(arrow)
  library(MultiAssayExperiment)
  library(SummarizedExperiment)
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

write_tsv <- function(frame, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  utils::write.table(
    frame,
    file = path,
    sep = "\t",
    quote = TRUE,
    row.names = FALSE,
    col.names = TRUE,
    na = "NA"
  )
}

as_plain_df <- function(value) {
  frame <- as.data.frame(value)
  value_names <- names(value)
  if (length(value_names) == ncol(frame)) {
    names(frame) <- value_names
  }
  row.names(frame) <- NULL
  frame
}

manifest_entries <- list()
add_manifest <- function(component, relative_path, frame, description) {
  manifest_entries[[length(manifest_entries) + 1L]] <<- data.frame(
    Component = component,
    Relative.Path = relative_path,
    Rows = nrow(frame),
    Columns = ncol(frame),
    Description = description,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

mae_path <- snakemake@input[["mae_rds"]]
config_input <- snakemake@input[["configfile"]]
out_dir <- snakemake@output[["outdir"]]

if (dir.exists(out_dir)) {
  unlink(out_dir, recursive = TRUE)
}
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

assay_dir <- file.path(out_dir, "assays")
rowdata_dir <- file.path(out_dir, "rowData")
metadata_dir <- file.path(out_dir, "metadata")
dir.create(assay_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(rowdata_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(metadata_dir, recursive = TRUE, showWarnings = FALSE)

mae <- readRDS(mae_path)
mae_metadata <- S4Vectors::metadata(mae)

coldata <- as_plain_df(MultiAssayExperiment::colData(mae))
sample_key_column <- names(coldata)[[1L]]
coldata_path <- file.path(out_dir, "colData.tsv")
write_tsv(coldata, coldata_path)
add_manifest("colData", "colData.tsv", coldata, "MAE colData export.")

drug_metadata <- as_plain_df(mae_metadata$Drug.Metadata)
drug_metadata_path <- file.path(out_dir, "drug_metadata.tsv")
write_tsv(drug_metadata, drug_metadata_path)
add_manifest(
  "drug_metadata",
  "drug_metadata.tsv",
  drug_metadata,
  "Harmonized drug metadata used by the MAE."
)

for (assay_name in names(MultiAssayExperiment::experiments(mae))) {
  se <- MultiAssayExperiment::experiments(mae)[[assay_name]]
  assay_matrix <- SummarizedExperiment::assay(se)
  assay_frame <- as.data.frame(t(assay_matrix), check.names = FALSE)
  assay_frame <- data.frame(
    sample_key = rownames(assay_frame),
    assay_frame,
    check.names = FALSE
  )
  names(assay_frame)[[1L]] <- sample_key_column
  assay_path <- file.path(assay_dir, paste0(assay_name, ".parquet"))
  arrow::write_parquet(assay_frame, assay_path)
  add_manifest(
    paste0("assay:", assay_name),
    file.path("assays", paste0(assay_name, ".parquet")),
    assay_frame,
    "MAE column by feature assay matrix exported from the MAE."
  )
  rm(assay_frame, assay_matrix)
  gc()

  rowdata <- as_plain_df(SummarizedExperiment::rowData(se))
  rowdata_path <- file.path(rowdata_dir, paste0(assay_name, ".tsv"))
  write_tsv(rowdata, rowdata_path)
  add_manifest(
    paste0("rowData:", assay_name),
    file.path("rowData", paste0(assay_name, ".tsv")),
    rowdata,
    "Feature metadata for one MAE assay."
  )
}

config_path <- file.path(metadata_dir, "pipeline.yaml")
if (!file.copy(config_input, config_path, overwrite = TRUE)) {
  stop("Failed to copy pipeline config", call. = FALSE)
}
config_frame <- data.frame(Path = config_path, stringsAsFactors = FALSE)
add_manifest(
  "pipeline_yaml",
  file.path("metadata", "pipeline.yaml"),
  config_frame,
  "Pipeline configuration copied from the run."
)

file_manifest <- do.call(rbind, manifest_entries)
write_tsv(file_manifest, file.path(metadata_dir, "file_manifest.tsv"))
