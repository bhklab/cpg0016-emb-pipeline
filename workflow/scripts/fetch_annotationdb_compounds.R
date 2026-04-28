library(httr2)
library(jsonlite)
library(readr)

output_file <- snakemake@output[["cache_csv"]]
dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

request(snakemake@params[["source_uri"]]) |>
  req_perform() |>
  resp_body_string() |>
  fromJSON(flatten = TRUE) |>
  write_csv(output_file, na = "")
