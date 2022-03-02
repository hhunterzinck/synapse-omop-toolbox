# Description: Create OMOP tables from field list.  
# Author: Haley Hunter-Zinck
# Date: 2021-11-21

# pre-setup  ---------------------------

library(optparse)
source("shared_fxns.R")

# user input ----------------------------

option_list <- list( 
  make_option(c("-f", "--file"), type = "character",
              help="Path to OMOP CDM field list file"),
  make_option(c("-s", "--synapse_id"), type = "character",
              help="Synapse ID of project in which to create tables"),
  make_option(c("-n", "--view_name"), type = "character", default = "omop_tables",
              help="Name of table view for created tables"),
  make_option(c("-v", "--verbose"), action="store_true", default = FALSE, 
              help="Output verbose messaging")
  
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$file) && !is.null(opt$synapse_id),
          msg = "Rscript create_omop_tables.R -h")

file_omop <- opt$file
synid_project <- opt$synapse_id
view_name <- opt$view_name
verbose <- opt$verbose

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(dplyr)
library(stringr)
library(synapser)
synLogin()

# functions ----------------------------

p_map_datatype <- function(dt, max_size = 500) {
  
  dt = tolower(dt)
  
  if (dt == "integer" || dt == "bigint") {
    return(list(type = "INTEGER"))
  }
  if (dt == "datetime") {
    return(list(type = "STRING"))
  }
  if (dt == "date") {
    return(list(type = "DATE"))
  } 
  if (dt == "float") {
    return(list(type = "DOUBLE"))
  }
  
  if (grepl(pattern = "varchar", x = dt)) {
    size <- gsub(pattern = "[\\(\\)]", replacement = "", x = str_extract(dt, "\\(.+\\)"))
    
    if (size == "max") {
      size = max_size
    } else if (as.double(size) > max_size) {
      size = max_size
    }
    
    return(list(type = "STRING", size = size))
  }
  
  return(NA)
}

map_datatype <- function(dt) {
  mapped_dt <- list()
  for (i in 1:length(dt)) {
    mapped_dt[[i]] <- p_map_datatype(dt[i])
  }
  
  return(mapped_dt)
}

get_primary_key <- function(table_info) {

  pk <- table_info %>% 
    filter(isPrimaryKey == "Yes") %>% 
    select(cdmFieldName)

  if (nrow(pk)) {
    pk <- as.character(unlist(pk))
  } else {
    fk <- table_info %>% 
    filter(isForeignKey == "Yes") %>% 
    select(cdmFieldName)

    pk <- paste0(as.character(unlist(fk)), collapse = ";")
  }

  return(pk)
}

# read ----------------------------

# read OMOP field list
omop <- read.csv(file_omop)

# main ----------------------------

table_names <- unique(omop$cdmTableName)
for (table_name in table_names) {
  
  if (verbose) {
    print(glue("{now()}: creating table '{table_name}'..."))
  }

  table_info <- omop %>% 
    filter(cdmTableName == table_name) %>%
    mutate(cdmFieldNameClean = gsub(pattern = "\"", replacement = "", x = cdmFieldName)) %>%
    select(cdmFieldNameClean, cdmDatatype, isPrimaryKey, isForeignKey) %>%
    rename(cdmFieldName = cdmFieldNameClean)

  pk <- get_primary_key(table_info)
  
  # create schema
  cols <- list()
  mapped_dt <- map_datatype(table_info[, "cdmDatatype"])
  for (i in 1:nrow(table_info)) {
    
    cols[[i]] <- Column(name = table_info[i, "cdmFieldName"], 
                        columnType = mapped_dt[[i]]$type,
                        maximumSize = mapped_dt[[i]]$size)
  }
  schema <- Schema(name = tolower(table_name), columns = cols, parent = synid_project)
  
  # create table
  header <- data.frame(matrix(NA, nrow = 0, ncol = nrow(table_info), 
                              dimnames = list(c(), unlist(table_info$cdmFieldName))))
  table_obj <- Table(schema = schema, values = header)
  
  # store table
  invisible(capture.output(table_obj <- synStore(table_obj)))
  synSetAnnotations(table_obj$tableId, annotations = list(primaryKey = pk))
}

# create entity view  
table_view <- EntityViewSchema(name=view_name, 
                            parent=synid_project, 
                            scopes=synid_project, 
                            includeEntityTypes=c(EntityViewType$TABLE))
table_view <- synStore(table_view)

# close out ----------------------------

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
