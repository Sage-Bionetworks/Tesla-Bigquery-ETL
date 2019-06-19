# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)
library(bigrquery)

source("../../../utils.R")

file_view <- "syn18387034"

synLogin()

roundx_file_df <- 
    "select id, name, submissionId, team, patientId from syn18387034 where round = 'x'" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    dplyr::mutate(file_type = str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(id, file_type, submissionId, team, patientId) %>% 
    tidyr::spread(key = "file_type", value = "id") %>% 
    magrittr::set_colnames(c(
        "SUBMISSION_ID",
        "TEAM",
        "PATIENT_ID",
        "FASTQ_RANKED_FILE",
        "VCF_RANKED_FILE",
        "FASTQ_UNRANKED_FILE",
        "VCF_UNRANKED_FILE",
        "VCF_FILE",
        "YAML_FILE"))



roundx_submission_df <- "../../../" %>% 
    stringr::str_c(c(
        "roundx_testing.csv",
        "roundx_validation.csv",
        "roundx_training.csv"
    )) %>% 
    purrr::map(readr::read_csv) %>% 
    dplyr::bind_rows() %>%
    dplyr::mutate(createdOn = lubridate::as_datetime(createdOn / 1000)) %>% 
    dplyr::select(auprc, createdOn, objectId) %>% 
    magrittr::set_colnames(c(
        "AUPRC", 
        "DATETIME", 
        "SUBMISSION_ID")) %>% 
    dplyr::mutate(SUBMITTED_LATE = F) %>% 
    dplyr::mutate(SUBMISSION_ID = as.character(SUBMISSION_ID)) %>% 
    dplyr::mutate(ROUND = 'x')

combined_df <- dplyr::inner_join(roundx_file_df, roundx_submission_df) %>% 
    dplyr::as_tibble() %>% 
    dplyr::mutate(KEY_FILE = NA) %>% 
    dplyr::mutate(STEP_FILE = NA) %>% 
    dplyr::select(
        SUBMISSION_ID,
        PATIENT_ID,
        TEAM,
        DATETIME,
        ROUND,
        AUPRC,
        SUBMITTED_LATE,
        FASTQ_RANKED_FILE,
        FASTQ_UNRANKED_FILE,
        VCF_RANKED_FILE,
        VCF_UNRANKED_FILE,
        VCF_FILE,
        YAML_FILE,
        KEY_FILE,
        STEP_FILE 
    )

tbl <- bigrquery::bq_table("neoepitopes", "Version_3", table = "Submissions")
bigrquery::bq_table_upload(tbl, combined_df, write_disposition = "WRITE_APPEND")

