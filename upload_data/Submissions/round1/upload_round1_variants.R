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
    "REF", 
    "ALT"
)

COL_FUNCS <- list(
    "CHROM" = as.character,
    "POS" = as.integer,
    "REF" = as.character,
    "ALT" = as.character)


get_vcf_cols <- function(args){
    variant_df <- args$id %>%
        download_from_synapse %>% 
        data.table::fread(skip = "#CHROM", sep = "\t") %>% 
        tibble::as_tibble() %>%
        dplyr::select(`#CHROM`, POS, REF, ALT) %>%
        magrittr::set_colnames(c("CHROM", "POS", "REF", "ALT"))
    check_columns(variant_df, REQ_VAR_COLS)
    variant_df <- variant_df %>% 
        convert_df_to_types() %>%
        dplyr::mutate(VAR_N = 1:n()) %>% 
        dplyr::mutate(CHROM = stringr::str_remove_all(CHROM, "chr")) %>% 
        dplyr::mutate(CHROM = stringr::str_remove_all(CHROM, "CHR")) %>% 
        dplyr::mutate(VARIANT_ID = stringr::str_c(
            args$submissionId, "_", 
            as.character(VAR_N))) %>% 
        dplyr::mutate(SUBMISSION_ID = args$submissionId)
}



check_columns <- function(df, required_cols){
    missing_columns <- required_cols[!required_cols %in% colnames(df)]
    if(length(missing_columns > 0)){
        stop("df has missing columns: ",
             stringr:: str_c(missing_columns, collapse = ", "))
    } 
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
    "select id, name, patientId, round, team, submissionId from syn18387034 where round = '1' and HLAclass is null" %>% 
    synTableQuery %>% 
    as.data.frame %>% 
    as_tibble() %>% 
    select(id, name, patientId, round, team, submissionId) 


key_file_df <- file_view_df %>% 
    filter(str_detect(name, "1.csv$"))

key_df <- key_file_df %>% 
    use_series(id) %>% 
    llply(create_df_from_synapse_id, .parallel = F) %>% 
    map2(key_file_df$submissionId,
         function(df, id) inset(df, "SUBMISSION_ID", value = id)) %>%
    bind_rows %>% 
    select(VAR_ID, CHROM, POS, SUBMISSION_ID) %>% 
    distinct %>% 
    mutate(CHROM = str_remove_all(CHROM, "chr"))

vcf_file_df <- file_view_df %>%
    filter(str_detect(name, "VCF.vcf$"))

vcf_df <- vcf_file_df %>%
    split(1:nrow(.)) %>%
    map(get_vcf_cols) %>% 
    bind_rows()

combined_df <-
    left_join(key_df, vcf_df, by = c("SUBMISSION_ID", "CHROM", "POS")) %>%
    mutate(diff = VAR_ID - VAR_N) %>%
    distinct %>%
    filter(!is.na(VAR_N))

id_offset_df <- combined_df %>%
    group_by(SUBMISSION_ID, diff) %>%
    summarise(count = n())

id_offset_df2 <- id_offset_df %>%
    group_by(SUBMISSION_ID) %>%
    filter(count == max(count)) %>%
    select(-count)

crane_submission_ids <- file_view_df %>% 
    filter(team == "crane") %>% 
    use_series(submissionId) %>% 
    unique
    

crane_key_df <- key_df %>%
    filter(SUBMISSION_ID %in% crane_submission_ids) %>%
    group_by(CHROM, POS, SUBMISSION_ID) %>%
    slice(1) %>%
    ungroup %>%
    group_by(SUBMISSION_ID) %>%
    arrange(VAR_ID) %>%
    mutate(VAR_N = 1:n()) %>%
    ungroup

non_crane_key_df <- key_df %>%
    filter(!SUBMISSION_ID %in% crane_submission_ids) %>%
    left_join(id_offset_df2) %>%
    mutate(VAR_N = VAR_ID - diff) %>%
    select(-diff)

variants_df <-
    bind_rows(crane_key_df, non_crane_key_df) %>%
    right_join(vcf_df) %>%
    rename(SOURCE_ROW_N = VAR_N) %>% 
    mutate(SOURCE_ROW_N = as.character(SOURCE_ROW_N)) %>% 
    select(VARIANT_ID, SUBMISSION_ID, CHROM, POS, REF, ALT, SOURCE_ROW_N) 
    

tbl <- bq_table("neoepitopes", "Version_3", table = "Variants")

bq_table_upload(tbl, variants_df, write_disposition = "WRITE_APPEND")
