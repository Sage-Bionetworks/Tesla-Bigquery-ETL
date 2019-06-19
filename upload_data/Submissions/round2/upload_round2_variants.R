library(plyr)
library(doMC)
library(synapser)
library(tidyverse)
library(data.table)
library(magrittr)
library(bigrquery)

source("../../../utils.R")
registerDoMC(cores = detectCores() - 1)
synLogin()

REQ_VAR_COLS <- c(
    "CHROM", 
    "POS", 
    "VAR_ID", 
    "REF", 
    "ALT"
)

COL_FUNCS <- list(
    "VAR_ID" = as.character,
    "CHROM" = as.character,
    "POS" = as.integer,
    "REF" = as.character,
    "ALT" = as.character)

check_columns <- function(df, required_cols){
    missing_columns <- required_cols[!required_cols %in% colnames(df)]
    if(length(missing_columns > 0)){
        stop("df has missing columns: ",
             stringr:: str_c(missing_columns, collapse = ", "))
    } 
}



get_vcf_cols <- function(args){
    variant_df <- args$id %>%
        download_from_synapse %>% 
        data.table::fread(skip = "#CHROM", sep = "\t") %>% 
        tibble::as_tibble() %>% 
        dplyr::select(`#CHROM`, POS, ID, REF, ALT) %>% 
        magrittr::set_colnames(c("CHROM", "POS", "VAR_ID", "REF", "ALT")) 
    check_columns(variant_df, REQ_VAR_COLS)
    variant_df <- variant_df %>% 
        convert_df_to_types() %>%
        dplyr::mutate(SOURCE_ROW_N = as.character(1:n())) %>% 
        dplyr::mutate(CHROM = stringr::str_remove_all(CHROM, "chr")) %>% 
        dplyr::mutate(CHROM = stringr::str_remove_all(CHROM, "CHR")) %>% 
        dplyr::mutate(VARIANT_ID = stringr::str_c(args$submissionId, "_", VAR_ID)) %>% 
        dplyr::select(-VAR_ID) %>%
        dplyr::mutate(SUBMISSION_ID = args$submissionId)
}

convert_df_to_types <- function(df){
    for(col in names(COL_FUNCS)){
        df <- mutate_col_if_exists(
            df,
            col, 
            col,
            COL_FUNCS[[col]])
    }
    return(df)
}

mutate_col_if_exists <- function(df, old_col, new_col, func, ...){
    if(!is.null(magrittr::extract2(df, old_col))){
        wrapr::let(
            alias = c(
                COL1 = old_col,
                COL2 = new_col), {
                    df  <- dplyr::mutate(df, COL2 = func(COL1, ...))
                }
        )
    } 
    return(df)
}

file_view_df <- 
    "select id, name, patientId, round, team, submissionId from syn18387034 where round = '2'" %>% 
    synTableQuery %>% 
    as.data.frame %>% 
    as_tibble() %>% 
    select(id, name, patientId, round, team, submissionId)


variants_df <- file_view_df %>% 
    filter(str_detect(name, ".vcf$")) %>% 
    filter(round == 2) %>% 
    split(1:nrow(.)) %>% 
    llply(get_vcf_cols, .parallel = F) %>% 
    bind_rows


tbl <- bq_table("neoepitopes", "Version_3", table = "Variants")

bq_table_upload(tbl, variants_df, write_disposition = "WRITE_APPEND")

