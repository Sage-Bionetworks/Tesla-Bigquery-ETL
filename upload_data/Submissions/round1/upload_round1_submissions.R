# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)
library(bigrquery)

source("../../../utils.R")

synLogin()

file_df <- 
    "select id, name, submissionId, team, patientId from syn18387034 where round = '1'" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    dplyr::as_tibble() %>% 
    dplyr::mutate(file_type = str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(id, file_type, submissionId, team, patientId) %>% 
    tidyr::spread(key = "file_type", value = "id") %>% 
    magrittr::set_colnames(c(
        "SUBMISSION_ID",
        "TEAM",
        "PATIENT_ID",
        "KEY_FILE",
        "FASTQ_RANKED_FILE",
        "FASTQ_UNRANKED_FILE",
        "STEP_FILE",
        "VCF_FILE"))
    
submission_df <- "../../../round12.csv" %>% 
    readr::read_csv() %>% 
    dplyr::filter(round == "1") %>% 
    dplyr::mutate(createdOn = lubridate::as_datetime(createdOn / 1000)) %>% 
    dplyr::select(createdOn, objectId) %>% 
    magrittr::set_colnames(c(
        "DATETIME", 
        "SUBMISSION_ID")) %>% 
    dplyr::mutate(SUBMITTED_LATE = F) %>% 
    dplyr::mutate(SUBMISSION_ID = as.character(SUBMISSION_ID)) %>% 
    dplyr::mutate(ROUND = '1')

auprc_df <- "syn18694219" %>% 
    create_df_from_synapse_id() %>% 
    dplyr::select(-V1) %>%
    dplyr::rename(PATIENT_ID = PATIENT) %>% 
    dplyr::mutate(PATIENT_ID = stringr::str_remove_all(PATIENT_ID, "Patient_"))
    

combined_df <- 
    dplyr::inner_join(file_df, submission_df) %>% 
    dplyr::left_join(auprc_df) %>% 
    dplyr::as_tibble() %>% 
    dplyr::mutate(VCF_RANKED_FILE = NA) %>% 
    dplyr::mutate(VCF_UNRANKED_FILE = NA) %>%
    dplyr::mutate(YAML_FILE = NA) %>%
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
