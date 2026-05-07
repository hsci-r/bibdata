box::use(src/core[con])

Sys.glob(here::here("data/unified/*.parquet")) |>
  purrr::walk(\(path) {
    t <- box::topenv()
    name = tools::file_path_sans_ext(basename(path))
    t[[name]] <- dplyr::tbl(con, glue::glue("read_parquet('{here::here('data/unified')}/{name}*.parquet')"))
  })
