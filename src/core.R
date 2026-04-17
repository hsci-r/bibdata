if (is.null(getOption("arrow_duck_con"))) {
  options(
    arrow_duck_con = DBI::dbConnect(
      duckdb::duckdb(bigint = "integer64"),
      bigint = "integer64",
      dbdir = here::here("data/work/duckdb/duckdb.db")
    )
  )
}

#' @export
con <- getOption("arrow_duck_con")

DBI::dbExecute(con, "SET enable_progress_bar = true;")
DBI::dbExecute(con, "SET preserve_insertion_order = false;")
DBI::dbExecute(con, "SET enable_fsst_vectors = true;")
