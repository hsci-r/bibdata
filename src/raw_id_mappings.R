
box::use(src/core[con])
box::use(stringr[str_c])

#' @export
e_id_files <- list.files(here::here("data/unified"), pattern="^e_id.*\\.parquet$", full.names=TRUE)
files_sql <- str_c("[", str_c("'", sort(e_id_files), "'", collapse=", "), "]")
e_id <- dplyr::tbl(con, glue::glue("read_parquet({files_sql})"))