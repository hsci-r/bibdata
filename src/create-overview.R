#!/usr/bin/env Rscript
renv::install(repos = c(hsci='https://hsci-r.r-universe.dev', cloud='https://cloud.r-project.org'))
#renv::snapshot(type="explicit")
library(optparse)

option_list = list(
  make_option(c("-n", "--dataset-name"), type="character", help="dataset name", metavar="name"),
  make_option(c("-d", "--dataset"), type="character", help="parquet file glob pattern containing the data to create an overview of", metavar="glob"),
  make_option(c("-f", "--field-descriptions"), type="character", help="tsv file containing descriptions for fields", metavar="filename"),
  make_option(c("-s", "--subfield-descriptions"), type="character", help="tsv file containing descriptions for subfields", metavar="filename"),
  make_option(c("-y", "--min-year"), type="integer", help="minimum year for the temporal overview", metavar="year"),
  make_option(c("-z", "--max-year"), type="integer", help="maximum year for the temporal overview", metavar="year"),
  make_option(c("-m", "--date-modified"), type="character", help="date modified of the dataset, in YYYY-MM-DD format", metavar="date"),
  make_option(c("-o", "--out"), type="character", help="output html file name", metavar="filename")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser, convert_hyphens_to_underscores = TRUE)

if (any(is.null(opt$dataset_name), is.null(opt$dataset), is.null(opt$out))) {
  print_help(opt_parser)
  stop()
}

rmarkdown::render("src/bib-overview.Rmd", output_file = paste0("../",opt$out), params = opt, knit_root_dir=getwd())
