# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)
library(bigrquery)

source("../../../utils.R")


synLogin()

round3_file_df <- 
    "select id, name, submissionId, team, patientId from syn18387034 where round = '3'" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    dplyr::filter(team %in% c("Bristletail", "Weevil")) %>% 
    dplyr::mutate(
        file_type = stringr::str_match(name, "TESLA_[:print:]+$"),
        submissionId = as.character(as.integer(submissionId)),
        patientId = as.character(as.integer(patientId)),
    ) %>% 
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



round3_submission_df <- "../../../project_setup/round3_late_setup/round3.csv" %>% 
    readr::read_csv() %>% 
    dplyr::mutate(createdOn = lubridate::as_datetime(createdOn / 1000)) %>% 
    dplyr::select(createdOn, objectId) %>% 
    magrittr::set_colnames(c(
        "DATETIME", 
        "SUBMISSION_ID")) %>% 
    dplyr::mutate(SUBMITTED_LATE = F) %>% 
    dplyr::mutate(SUBMISSION_ID = as.character(SUBMISSION_ID)) %>% 
    dplyr::mutate(ROUND = '3')

combined_df <- dplyr::inner_join(round3_file_df, round3_submission_df) %>% 
    dplyr::as_tibble() %>% 
    dplyr::mutate(KEY_FILE = NA) %>% 
    dplyr::mutate(STEP_FILE = NA) %>% 
    dplyr::select(
        SUBMISSION_ID,
        PATIENT_ID,
        TEAM,
        DATETIME,
        ROUND,
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

