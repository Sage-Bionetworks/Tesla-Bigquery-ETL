library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

df <- "select patientId, diagnosis, tumorType, classIHLAalleles from syn16805178" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(-c(ROW_ID, ROW_VERSION, ROW_ETAG)) %>% 
    dplyr::mutate(patientId = as.character(patientId)) %>% 
    dplyr::mutate(patientId = stringr::str_c("x", patientId))

allele_df <- df %>% 
    dplyr::select(patientId, classIHLAalleles) %>% 
    dplyr::distinct() %>% 
    tidyr::drop_na() %>% 
    tidyr::separate_rows(classIHLAalleles, sep = ",") %>% 
    dplyr::mutate(classIHLAalleles = str_sub(classIHLAalleles, end = 7)) %>% 
    dplyr::distinct() %>% 
    magrittr::set_colnames(c("PATIENT_ID", "HLA_ALLELE"))

patient_df <- df %>% 
    dplyr::select(-classIHLAalleles) %>% 
    dplyr::filter(tumorType != "Not Applicable") %>% 
    dplyr::distinct() %>% 
    magrittr::set_colnames(c("PATIENT_ID", "DIAGNOSIS", "TUMOR_TYPE"))
    
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
