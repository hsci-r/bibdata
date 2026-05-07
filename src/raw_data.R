box::use(src/core[con])
box::use(dplyr[...])
box::use(stringr[...])

collection_info <- tbl(con, glue::glue("read_csv('{here::here('data/schema-info/collection_info.tsv')}')"))

collection_info |> 
  collect() |>
  rowwise() |>
  group_walk(\(row, ...) {
    t <- box::topenv()
    files <- list.files(here::here(str_c("data/", row$dataset)), pattern="\\.parquet$", full.names=TRUE)
    table_names <- str_replace(basename(files), "_\\d+\\.parquet$", "") |> str_replace("\\.parquet$", "")
    groups <- split(files, table_names)
    purrr::iwalk(groups, \(file_list, name) {
      files_sql <- str_c("[", str_c("'", file_list, "'", collapse=", "), "]")
      t[[name]] <- tbl(con, glue::glue("read_parquet({files_sql})"))
    })
  })