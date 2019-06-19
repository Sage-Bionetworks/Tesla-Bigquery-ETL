library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

df <- "syn18498204" %>% 
    create_df_from_synapse_id() %>% 
    dplyr::mutate(TCR_FLOW_I_NUM = as.character(TCR_FLOW_I_NUM)) %>% 
    dplyr::mutate(TCR_FLOW_I_NUM = stringr::str_remove_all(TCR_FLOW_I_NUM, "%")) %>% 
    dplyr::mutate(TCR_FLOW_I_NUM = as.numeric(TCR_FLOW_I_NUM)) %>% 
    dplyr::mutate(TCR_FLOW_II_NUM = as.character(TCR_FLOW_II_NUM)) %>% 
    dplyr::mutate(TCR_FLOW_II_NUM = stringr::str_remove_all(TCR_FLOW_II_NUM, "%")) %>% 
    dplyr::mutate(TCR_FLOW_II_NUM = as.numeric(TCR_FLOW_II_NUM)) %>% 
    dplyr::mutate(TCR_NANOPARTICLE_NUM = as.character(TCR_NANOPARTICLE_NUM)) %>% 
    dplyr::mutate(TCR_NANOPARTICLE_NUM = stringr::str_remove_all(TCR_NANOPARTICLE_NUM, "%")) %>% 
    dplyr::mutate(TCR_NANOPARTICLE_NUM = as.numeric(TCR_NANOPARTICLE_NUM)) %>% 
    dplyr::mutate(TCR_NANOPARTICLE = as.character(TCR_NANOPARTICLE)) %>% 
    dplyr::mutate(TCR_FLOW_I = as.character(TCR_FLOW_I)) %>% 
    dplyr::mutate(TCR_FLOW_II = as.character(TCR_FLOW_II)) %>% 
    dplyr::mutate(PATIENT_ID = as.character(PATIENT_ID)) %>% 
    dplyr::rename(HLA_ALLELE = `HLA*_A*LLELE`) %>% 
    dplyr::mutate(HLA_ALLELE = stringr::str_remove_all(HLA_ALLELE, "\\*")) %>% 
    dplyr::mutate(HLA_ALLELE = str_c(
        stringr::str_sub(HLA_ALLELE, end = 1),
        "*",
        stringr::str_sub(HLA_ALLELE, start = 2)))
    
tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3", 
    table = "Validated_Bindings")

bigrquery::bq_table_upload(
    tbl, 
    df, 
    write_disposition = "WRITE_APPEND")