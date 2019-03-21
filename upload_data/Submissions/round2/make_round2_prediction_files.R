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

create_df_by_file <- function(args){
    df <- create_df_from_synapse_id(args$id, na.strings = c("NA", "na", "n/a", ""))
    if(nrow(df) == 0) return(NA)
    temp_df <- df %>%
        inset("SOURCE", value = args$source) %>% 
        inset("SOURCE_ROW_N", value = 1:nrow(.)) %>% 
        inset("SUBMISSION_ID", value = args$submissionId) %>% 
        mutate(PROT_POS = as.character(PROT_POS)) %>% 
        mutate(VAR_ID = as.character(VAR_ID)) 
    if(is.null(temp_df$STEP_ID)){
        result_df <- temp_df %>% 
            group_by_at(vars(-c(PROT_POS, VAR_ID, SOURCE_ROW_N))) %>% 
            summarise(PROT_POS = str_c(unique(PROT_POS), collapse = ";"),
                      VAR_ID = str_c(unique(VAR_ID), collapse = ";"),
                      SOURCE_ROW_N = str_c(unique(SOURCE_ROW_N), collapse = ";")) %>% 
            ungroup
    } else{
        result_df <- temp_df %>% 
            group_by_at(vars(-c(PROT_POS, VAR_ID, SOURCE_ROW_N, STEP_ID))) %>% 
            summarise(PROT_POS = str_c(unique(PROT_POS), collapse = ";"),
                      VAR_ID = str_c(unique(VAR_ID), collapse = ";"),
                      SOURCE_ROW_N = str_c(unique(SOURCE_ROW_N), collapse = ";"),
                      STEP_ID = str_c(unique(STEP_ID), collapse = ";")) %>% 
            ungroup
    }
    return(result_df)
}

remove_empty_cols <- function(df){
    df[,colSums(is.na(df)) < nrow(df)]
}

mutate_col_if_exists <- function(df, old_col, new_col, fun, ...){
    if(!is.null(extract2(df, old_col))){
        wrapr::let(
            alias = c(
                COL1 = old_col,
                COL2 = new_col), {
                df  <- mutate(df, COL2 = fun(COL1, ...))
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

prediction_df <- file_view_df %>% 
    filter(str_detect(name, "[1,2,3,4].csv$")) %>% 
    filter(round == 2) %>% 
    mutate(source = ifelse(str_detect(name, "[13].csv$"), "fastq", "vcf")) %>% 
    split(1:nrow(.)) %>% 
    llply(create_df_by_file, .parallel = F) %>% 
    unname %>% 
    discard(is.na(.)) %>% 
    llply(remove_empty_cols, .parallel = T) %>% 
    llply(mutate_col_if_exists, "PROT_POS", "PROT_POS", as.character, .parallel = T) %>% 
    llply(mutate_col_if_exists, "VAR_ID", "VAR_ID", as.character, .parallel = T) %>% 
    llply(mutate_col_if_exists, "RANK", "RANK", as.integer, .parallel = T) %>% 
    llply(mutate_col_if_exists, "PEP_LEN", "PEP_LEN", as.integer, .parallel = T) %>% 
    llply(mutate_col_if_exists, "HLA_ALT_BINDING", "HLA_ALT_BINDING", as.double, .parallel = T) %>% 
    llply(mutate_col_if_exists, "SCORE", "SCORE", as.integer, .parallel = T) %>% 
    llply(mutate_col_if_exists, "HLA_REF_BINDING", "HLA_REF_BINDING", as.double, .parallel = T) %>% 
    llply(mutate_col_if_exists, "REF_ALLELE_EXP", "REF_ALLELE_EXP", as.double, .parallel = T) %>%
    llply(mutate_col_if_exists, "ALT_ALLELE_EXP", "ALT_ALLELE_EXP", as.double, .parallel = T) %>%
    llply(mutate_col_if_exists, "STEP_ID", "STEP_ID", as.integer, .parallel = T) %>%
    bind_rows %>%  
    mutate(HLA_ALLELE = str_remove(HLA_ALLELE, "HLA-")) %>% 
    mutate(HLA_ALLELE = str_remove(HLA_ALLELE, "\\*")) %>% 
    mutate(HLA_ALLELE = str_c(
        str_sub(HLA_ALLELE, end = 1),
        "*",
        str_sub(HLA_ALLELE, start = 2)
    )) %>% 
    mutate(PREDICTION_ID = str_c(SUBMISSION_ID, SOURCE, HLA_ALLELE, ALT_EPI_SEQ, sep = "_")) %>% 
    mutate(HLA_ALLELE_MUT = NA) %>%
    mutate(HLA_ALLELE_MUT = as.double(HLA_ALLELE_MUT)) %>% 
    select(
        RANK, 
        HLA_ALLELE,
        ALT_EPI_SEQ,
        SCORE,
        REF_EPI_SEQ,
        PEP_LEN, 
        HLA_ALLELE_MUT,
        HLA_ALT_BINDING, 
        HLA_REF_BINDING,
        REF_ALLELE_EXP,
        ALT_ALLELE_EXP, 
        RANK_METRICS, 
        RANK_DESC,
        ADDN_INFO,
        SOURCE,
        SOURCE_ROW_N, 
        STEP_ID, 
        SUBMISSION_ID, 
        PREDICTION_ID,
        VAR_ID,
        PROT_POS
    ) %>% 
    group_by(SUBMISSION_ID, SOURCE, HLA_ALLELE, ALT_EPI_SEQ) %>% 
    arrange(RANK)

prediction_df %>%
    ungroup %>% 
    select(PREDICTION_ID, PROT_POS) %>%
    separate_rows(PROT_POS, sep = ";") %>%
    distinct %>% 
    readr::write_csv("round2_protein_positions.csv")

prediction_df %>% 
    ungroup %>% 
    select(PREDICTION_ID, SUBMISSION_ID, VAR_ID)  %>%
    separate_rows(VAR_ID, sep = ";") %>%
    distinct %>% 
    dplyr::mutate(VARIANT_ID = str_c(SUBMISSION_ID, "_", VAR_ID)) %>% 
    dplyr::select(-c(VAR_ID, SUBMISSION_ID)) %>% 
    readr::write_csv("round2_prediction_variants.csv")

bad_prediction_df <- prediction_df %>% 
    slice(-1) %>% 
    ungroup %>% 
    select(-c(VAR_ID, PROT_POS)) %>%
    distinct %>% 
    write_csv("round2_bad_predictions.csv")


prediction_df <- prediction_df %>% 
    slice(1) %>% 
    ungroup %>% 
    select(-c(VAR_ID, PROT_POS)) %>%
    distinct() %>% 
    write_csv("round2_predictions.csv")




