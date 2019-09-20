library(synapser)
library(tidyverse)
library(magrittr)
library(wrapr)
library(data.table)


synLogin()

source("../../../utils.R")

REQ_COLS <- c("HLA_ALLELE", "ALT_EPI_SEQ", "VAR_ID")
REQ_RANKED_COLS <- c(REQ_COLS, "RANK")
OPT_COLS <- c(
    "SCORE", 
    "REF_EPI_SEQ",
    "PEP_LEN",
    "HLA_ALLELE_MUT",
    "HLA_ALT_BINDING",
    "HLA_REF_BINDING",
    "REF_ALLELE_EXP",
    "ALT_ALLELE_EXP",
    "RANK_METRICS",
    "RANK_DESC",
    "ADDN_INFO",
    "STEP_ID",
    "PROT_POS")

COL_FUNCS <- list(
    "RANK" = as.integer,
    "HLA_ALLELE" = as.character, 
    "ALT_EPI_SEQ" = as.character,
    "VAR_ID" = as.character,
    "SCORE" = as.integer,
    "REF_EPI_SEQ" = as.character,
    "PEP_LEN" = as.integer,
    "HLA_ALLELE_MUT" = as.character,
    "HLA_ALT_BINDING" = as.double,
    "HLA_REF_BINDING" = as.double,
    "REF_ALLELE_EXP" = as.double,
    "ALT_ALLELE_EXP" = as.double,
    "RANK_METRICS" = as.character,
    "RANK_DESC" = as.character,
    "ADDN_INFO" = as.character,
    "STEP_ID" = as.character,
    "PROT_POS" = as.integer)

ADDED_COLS <- c(
    "SOURCE_ROW_N",
    "SOURCE",
    "PREDICTION_ID"
)
    

submission_df <- 
    "select id, name, submissionId, patientId from syn18387034 where round = '3'" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(id, name, submissionId, patientId) %>% 
    dplyr::mutate(
        file_type = stringr::str_match(name, "TESLA_[:print:]+$"),
        submissionId = as.character(as.integer(submissionId)),
        patientId = as.character(as.integer(patientId)),
    ) %>% 
    dplyr::select(-name) %>% 
    dplyr::filter(file_type != "TESLA_YAML.yaml") %>% 
    spread(key = "file_type", value = "id")


create_prediction_tables <- function(args){
    print(args)
    prediction_df <- create_prediction_table(args)
    if(!is.na(args$TESLA_OUT_2.csv) && !is.na(args$TESLA_OUT_4.csv)){
        vcf_df <- create_prediction_table(args, "vcf")
        prediction_df <- dplyr::bind_rows(prediction_df, vcf_df)
    } 

    variant_prediction_df <- prediction_df %>% 
        dplyr::select(PREDICTION_ID, VAR_ID) %>% 
        tidyr::unnest() %>% 
        dplyr::mutate(VARIANT_ID = str_c(args$submissionId, "_", VAR_ID)) %>% 
        dplyr::select(-VAR_ID)
    
    protein_position_df <- prediction_df %>% 
        dplyr::select(PREDICTION_ID, PROT_POS) %>% 
        tidyr::unnest()
    
    prediction_df <- prediction_df %>% 
        dplyr::select(-c(VAR_ID, PROT_POS)) %>% 
        dplyr::group_by(SOURCE, ALT_EPI_SEQ, HLA_ALLELE) %>% 
        dplyr::arrange(RANK)
        
    bad_prediction_df <- prediction_df %>%
        dplyr::slice(-1) %>%
        dplyr::ungroup()

    prediction_df <- prediction_df  %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()
    
    readr::write_csv(
        variant_prediction_df, 
        stringr::str_c(args$submissionId, "_variant_prediction_df.csv")
    )
    readr::write_csv(
        protein_position_df, 
        stringr::str_c(args$submissionId, "_protein_position_dff.csv")
    )
    readr::write_csv(
        bad_prediction_df, 
        stringr::str_c(args$submissionId, "_bad_prediction_df.csv")
    )
    readr::write_csv(
        prediction_df, 
        stringr::str_c(args$submissionId, "_prediction_df.csv")
    )
}


create_prediction_table <- function(args, src = "fastq"){
    if(src == "fastq"){
        ranked_df <- create_df_from_synapse_id(args$TESLA_OUT_1.csv)
        unranked_df <- create_df_from_synapse_id(args$TESLA_OUT_3.csv)
    } else {
        ranked_df <- create_df_from_synapse_id(args$TESLA_OUT_2.csv)
        unranked_df <- create_df_from_synapse_id(args$TESLA_OUT_4.csv)
    }
    ranked_df <- ranked_df %>% 
        separate_rows(VAR_ID, sep = ":") %>% 
        separate_rows(PROT_POS, sep = ";")
    
    unranked_df <- unranked_df %>% 
        separate_rows(VAR_ID, sep = ":") %>% 
        separate_rows(PROT_POS, sep = ";")
        
    check_columns(ranked_df, REQ_RANKED_COLS)
    
    ranked_df <- ranked_df %>% 
        convert_df_to_types() %>% 
        dplyr::mutate(SOURCE_ROW_N = as.character(1:n())) %>% 
        dplyr::mutate(STEP_ID = NA)
    
    if (nrow(unranked_df) > 0 ){
        
        check_columns(unranked_df, REQ_COLS)
        
        unranked_df <- unranked_df %>% 
            convert_df_to_types() %>% 
            dplyr::mutate(SOURCE_ROW_N = as.character(1:n())) %>% 
            dplyr::mutate(RANK = NA)
    }
    
    combined_df <- ranked_df %>% 
        dplyr::select(dplyr::one_of(names(COL_FUNCS), ADDED_COLS)) %>% 
        convert_df_to_types() %>% 
        dplyr::filter(!is.na(RANK)) %>%
        dplyr::bind_rows(unranked_df) %>% 
        dplyr::filter(!is.na(HLA_ALLELE)) %>% 
        dplyr::filter(!is.na(ALT_EPI_SEQ)) %>% 
        dplyr::mutate(ALT_EPI_SEQ = format_epitopes(ALT_EPI_SEQ)) %>% 
        dplyr::filter(!ALT_EPI_SEQ == "") %>% 
        dplyr::mutate(SOURCE = src) %>% 
        dplyr::group_by_at(vars(-c(PROT_POS, VAR_ID, SOURCE_ROW_N, STEP_ID))) %>% 
        dplyr::summarise(
            PROT_POS = list(unique(PROT_POS)),
            VAR_ID = list(unique(VAR_ID)),
            SOURCE_ROW_N = str_c(unique(SOURCE_ROW_N), collapse = ";"),
            STEP_ID = str_c(unique(STEP_ID), collapse = ";")
        ) %>% 
        dplyr::ungroup() %>% 
        dplyr::mutate(SUBMISSION_ID = args$submissionId) %>% 
        dplyr::mutate(PREDICTION_ID = stringr::str_c(
            SUBMISSION_ID, 
            SOURCE, 
            ALT_EPI_SEQ, 
            HLA_ALLELE, 
            sep = "_"))

}

format_epitopes <- function(epitopes){
    epitopes %>% 
        stringr::str_remove_all("[^A-Za-z]") %>% 
        toupper() 
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


check_columns <- function(df, required_cols){
    missing_columns <- required_cols[!required_cols %in% colnames(df)]
    if(length(missing_columns > 0)){
        stop("df has missing columns: ",
             str_c(missing_columns, collapse = ", "))
    } 
}

completed <- 
    list.files() %>% 
    purrr::keep(stringr::str_detect(., ".csv$")) %>% 
    tibble::enframe(name = NULL) %>% 
    tidyr::separate(value, sep = "_", into = c("submissionId"), rest = "drop") %>% 
    dplyr::group_by(submissionId) %>% 
    dplyr::summarise(count = dplyr::n()) %>% 
    dplyr::filter(count == 4) %>% 
    dplyr::pull(submissionId)

submission_df %>%
    dplyr::filter(!submissionId %in% completed) %>% 
    dplyr::ungroup() %>%
    dplyr::group_by(submissionId) %>%
    dplyr::group_split() %>%
    purrr::walk(create_prediction_tables)


