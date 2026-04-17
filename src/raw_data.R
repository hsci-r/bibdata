box::use(src/core[con])
box::use(dplyr[...])
box::use(stringr[...])

collection_info <- tbl(con, glue::glue("read_csv('{here::here('data/schema-info/collection_info.tsv')}')"))

collection_info |> 
  collect() |>
  rowwise() |>
  group_walk(\(row, ...) {
    t <- box::topenv()
    if (row$dataset == "wikidata") {
      list.files(here::here("data/wikidata/"), pattern=".*\\.parquet") |> 
        str_replace("(_[0-9])*\\.parquet","") |> 
        unique() |>
        purrr::walk(\(name) t[[str_c('wd_',name)]] <- tbl(con, glue::glue("read_parquet('{here::here('data/wikidata')}/{name}*.parquet')")))
      NULL
    } else if (row$dataset == "isni") {
      list.files(here::here("data/isni/"), pattern=".*\\.parquet") |> 
        str_replace("(_[0-9])*\\.parquet","") |> 
        unique() |>
        purrr::walk(\(name) t[[str_c('isni_',name)]] <- tbl(con, glue::glue("read_parquet('{here::here('data/isni')}/{name}*.parquet')")))
    } else if (row$dataset == "geonames") {
      t[["geonames"]] <- tbl(con, glue::glue("read_parquet('{here::here('data')}/geonames/geonames*.parquet')"))
      t[["geonames_alternate_names"]] <- tbl(con, glue::glue("read_parquet('{here::here('data')}/geonames/alternate_names*.parquet')"))
    } else if (row$standard == "rdf") {
      ds <- tbl(con, glue::glue("read_parquet('{here::here('data')}/{row$dataset}/{row$dataset}*.parquet')")) |>
        relocate(subject, property, object)
      t[[row$dataset]] <- ds
    } else if (row$standard %in% c("intermarc", "marc21", "unimarc", "pica", "istc", "danmarc", "ctmarc")) {
      ds <- tbl(con, glue::glue("read_parquet('{here::here('data')}/{row$dataset}/{row$dataset}*.parquet')"))
      t[[row$dataset]] <- ds
    } else {
      stop("Unknown dataset type ", row)
    }
  })