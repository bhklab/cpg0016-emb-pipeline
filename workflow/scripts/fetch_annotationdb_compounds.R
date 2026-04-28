suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
})

parse_bool <- function(value) {
  if (is.logical(value)) {
    return(isTRUE(value))
  }
  if (is.null(value) || length(value) == 0L) {
    return(FALSE)
  }
  tolower(trimws(as.character(value[[1L]]))) %in% c("1", "true", "yes", "on")
}

source_uri <- snakemake@params[["source_uri"]]
overwrite <- parse_bool(snakemake@params[["overwrite"]])
cache_csv <- snakemake@output[["cache_csv"]]
stamp <- if ("stamp" %in% names(snakemake@output)) {
  snakemake@output[["stamp"]]
} else {
  NULL
}

dir.create(dirname(cache_csv), recursive = TRUE, showWarnings = FALSE)
if (!is.null(stamp)) {
  dir.create(dirname(stamp), recursive = TRUE, showWarnings = FALSE)
}

if (file.exists(cache_csv) && file.size(cache_csv) > 0L && !overwrite) {
  if (!is.null(stamp)) {
    file.create(stamp)
  }
  cat(sprintf("[fetch_annotationdb_compounds] skip_existing=%s\n", cache_csv))
  quit(save = "no", status = 0)
}

payload <- request(source_uri) |>
  req_user_agent("arpa-h-sister-pipelines/annotationdb/1.0") |>
  req_timeout(300) |>
  req_retry(max_tries = 5) |>
  req_perform() |>
  resp_body_string() |>
  fromJSON(flatten = TRUE)

if (!is.data.frame(payload)) {
  stop("AnnotationDB /compound/all did not return a tabular JSON payload")
}

tmp_path <- paste0(cache_csv, ".tmp")
utils::write.csv(payload, tmp_path, row.names = FALSE, na = "")
file.rename(tmp_path, cache_csv)
if (!is.null(stamp)) {
  file.create(stamp)
}

cat(sprintf(
  "[fetch_annotationdb_compounds] rows=%d output=%s\n",
  nrow(payload),
  cache_csv
))
