
box::use(src/core[con])

#' @export
e_id <- dplyr::tbl(con, glue::glue("read_parquet('{here::here('data/work/raw_id_mappings')}/e_id.parquet')"))