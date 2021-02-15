# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)
library(bigrquery)

source("../../../utils.R")
devtools::source_url("https://raw.githubusercontent.com/Sage-Bionetworks/synapse_tidy_utils/master/utils.R")

synLogin()

submission_file_tbl  <- create_entity_tbl("syn21212423")
submission_file_tbl2 <- create_entity_tbl("syn21212422")

annotated_file_tbl <- 
    dplyr::bind_rows(submission_file_tbl, submission_file_tbl2) %>% 
    dplyr::select(id, name) %>% 
    add_annotations_to_tbl() %>% 
    dplyr::select(id, name, submissionId, patientId, team, round)  %>% 
    dplyr::mutate(file_type = stringr::str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(-name) %>% 
    tidyr::pivot_wider(values_from  = "id", names_from = "file_type") %>% 
    dplyr::rename(
        SUBMISSION_ID = submissionId, 
        PATIENT_ID = patientId, 
        TEAM = team,
        ROUND = round,
        FASTQ_RANKED_FILE = TESLA_OUT_1.csv,
        VCF_RANKED_FILE = TESLA_OUT_2.csv,
        FASTQ_UNRANKED_FILE = TESLA_OUT_3.csv,
        VCF_UNRANKED_FILE = TESLA_OUT_4.csv,
        VCF_FILE = TESLA_VCF.vcf,
        YAML_FILE = TESLA_YAML.yaml
    ) %>% 
    dplyr::mutate(SUBMITTED_LATE = T) 

datetime_tbl <- "late_round12.csv" %>% 
    readr::read_csv() %>% 
    dplyr::select(SUBMISSION_ID = objectId, createdOn) %>% 
    dplyr::mutate(SUBMISSION_ID = as.character(SUBMISSION_ID)) %>% 
    dplyr::filter(SUBMISSION_ID %in% annotated_file_tbl$SUBMISSION_ID) %>% 
    dplyr::mutate(DATETIME = lubridate::as_datetime(createdOn / 1000)) %>% 
    dplyr::select(SUBMISSION_ID, DATETIME)

combined_df <- dplyr::inner_join(annotated_file_tbl, datetime_tbl) %>% 
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

tbl <- bigrquery::bq_table("neoepitopes", "Version_4", table = "Submissions")
bigrquery::bq_table_upload(tbl, combined_df, write_disposition = "WRITE_APPEND")

