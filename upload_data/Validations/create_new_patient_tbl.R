library(bigrquery)
library(tidyverse)


BQ_PROJECT <- "neoepitopes"
BQ_DATASET <- "Version_3"

BQ_DBI     <- DBI::dbConnect(
    bigrquery::bigquery(), 
    project = BQ_PROJECT,
    dataset = BQ_DATASET)

submission_dbi <- BQ_DBI %>% 
    dplyr::tbl("Submissions") %>% 
    dplyr::filter(ROUND %in% c("1", "2", "x")) %>% 
    dplyr::select(PATIENT_ID) %>% 
    dplyr::distinct()

patient_tbl <- BQ_DBI %>% 
    dplyr::tbl("Patients") %>% 
    dplyr::inner_join(submission_dbi) %>% 
    dplyr::as_tibble()

new_patient_tbl <- bq_table("neoepitopes", "Version_3", table = "Patients_New")
bq_table_upload(new_patient_tbl ,patient_tbl , write_disposition = "WRITE_APPEND")


