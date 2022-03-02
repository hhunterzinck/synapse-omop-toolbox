# Description: Fill OMOP tables with new data by either appending or replacing
#     existing data in them.  
# Author: Haley Hunter-Zinck
# Date: 2021-11-24

# pre-setup  ---------------------------

library(optparse)
source("shared_fxns.R")

# user input ----------------------------

option_list <- list( 
  make_option(c("-f", "--file_data"), type = "character",
              help="Path to new data file"),
  make_option(c("-s", "--synapse_id"), type = "character",
              help="Synapse ID of table to update"),
  make_option(c("-c", "--comment"), type = "character",
              help="Comment describing table update"),
  make_option(c("-d", "--delimiter"), type = "character", default = "\t",
              help="Data file delimiter (default: tab)"),
  make_option(c("-r", "--replace"), action="store_true", default = FALSE, 
              help="Replace the current table contents (rather than appending)"),
  make_option(c("-v", "--verbose"), action="store_true", default = FALSE, 
              help="Output verbose messaging")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$file_data) && !is.null(opt$synapse_id) && !is.null(opt$comment),
          msg = "Rscript fill_omop_tables.R -h")

file_data <- opt$file_data
synid_table <- opt$synapse_id
comment <- opt$comment
delimiter <- opt$delimiter
replace <- opt$replace
verbose <- opt$verbose

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(dplyr)
library(synapser)
invisible(capture.output(synLogin()))

# synapse
table_name <- synGet(synid_table)$properties$name
pk_names <- strsplit(synGetAnnotations(synid_table)$primaryKey[[1]], split = ";")[[1]]

# functions ----------------------------

prepare_data_for_synapse_table <- function(data, max_chr = 524288) {
  
  nchr <- lapply(data, nchar)
  
  for (i in 1:length(nchr)) {
    idx <- which(nchr[[i]] > max_chr)
    data[[i]][idx] <- substr(data[[i]][idx], start = 1, stop = max_chr)
  }
  
  return(data)
}

# main ----------------------------

if (verbose) {
  print(glue("{now()} | reading file '{file_data}'..."))
}

raw <- read.csv(file_data, sep = delimiter, quote = "")
data <- prepare_data_for_synapse_table(raw)
data_fil <- data

if (!replace) {
  
  if (verbose) {
    print(glue("{now()} | filtering data in '{file_data}'..."))
  }
  
  query <- glue("SELECT DISTINCT {paste0(pk_names, collapse = ', ')} FROM {synid_table}")
  data_present <- as.data.frame(lapply(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F)), as.character))
  
  data_fil <- data %>%
    anti_join(data_present, by = c(pk_names))
} 

if (verbose && nrow(data_fil) > 0) {
  print(glue("{now()} | {if (replace) 'replacing' else 'appending'} {nrow(data_fil)} rows for table '{table_name}' ({synid_table})... "))
} else if (verbose && nrow(data_fil) == 0) {
  print(glue("{now()} | no rows to add to table '{table_name}' ({synid_table})... "))
}

if (nrow(data_fil) > 0) {
  n_version <- invisible(capture.output(create_new_table_version(table_id = synid_table,
                                                                 data = data_fil,
                                                                 comment = comment,
                                                                 replace = replace)))
}

# close out ----------------------------

action <- ""
if (replace) {
  action <- "Replaced"
} else {
  action = "Appended"
}

print(glue("{action} data in file '{file_data}' in table '{table_name}' ({synid_table})"))
toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
