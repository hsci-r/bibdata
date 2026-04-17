box::use(src/core[con])

Sys.glob(here::here("data/work/derived_raw/*.parquet")) |> 
  purrr::walk(\(path) {
    t <- box::topenv()
    name = tools::file_path_sans_ext(basename(path))
    t[[name]] <- dplyr::tbl(con, glue::glue("read_parquet('{here::here('data/work/derived_raw')}/{name}*.parquet')"))
  })