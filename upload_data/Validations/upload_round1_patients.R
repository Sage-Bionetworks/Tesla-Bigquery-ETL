library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

df <- "select patientId, diagnosis, sex, tumorType, tumorPurityPercent, checkpointInhibitor, classIHLAalleles from syn8292741 where round = 1" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(-c(ROW_ID, ROW_VERSION, ROW_ETAG))

pat_df <- "syn18498152" %>% 
    create_df_from_synapse_id()

allele_df <- df %>% 
    dplyr::select(patientId, classIHLAalleles) %>% 
    dplyr::distinct() %>% 
    tidyr::drop_na() %>% 
    tidyr::separate_rows(classIHLAalleles, sep = ";") %>% 
    dplyr::mutate(classIHLAalleles = str_sub(classIHLAalleles, end = 7)) %>% 
    dplyr::distinct() %>% 
    magrittr::set_colnames(c("PATIENT_ID", "HLA_ALLELE")) %>% 
    dplyr::mutate(PATIENT_ID = as.character(PATIENT_ID))

patient_df <- df %>% 
    dplyr::select(-classIHLAalleles) %>% 
    dplyr::distinct() %>% 
    dplyr::filter(tumorPurityPercent != "Not Applicable ") %>% 
    magrittr::set_colnames(c("PATIENT_ID", "DIAGNOSIS", "GENDER", "TUMOR_TYPE", "TUMOR_PURITY", "CHECKPOINT_TREATED")) %>% 
    dplyr::left_join(pat_df) %>% 
    dplyr::mutate(PATIENT_ID = as.character(PATIENT_ID)) %>% 
    dplyr::mutate(TUMOR_PURITY = as.numeric(TUMOR_PURITY) / 100) %>% 
    dplyr::mutate(CHECKPOINT_TREATED = ifelse(CHECKPOINT_TREATED == "yes", T, 
                                              ifelse(CHECKPOINT_TREATED == "no", F, NA)))
    
patient_tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3", 
    table = "Patients")

allele_tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3", 
    table = "Patient_Alleles")

bigrquery::bq_table_upload(
    patient_tbl, 
    patient_df, 
    write_disposition = "WRITE_APPEND")

bigrquery::bq_table_upload(
    allele_tbl, 
    allele_df, 
    write_disposition = "WRITE_APPEND")
