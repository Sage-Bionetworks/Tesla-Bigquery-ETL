library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

df <- "syn18498153" %>% 
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
    dplyr::mutate(PATIENT_ID = as.character(PATIENT_ID))
    
tbl <- bigrquery::bq_table(
    "neoepitopes",
    "Version_3",
    table = "Validated_Bindings")

bigrquery::bq_table_upload(
    tbl,
    df,
    write_disposition = "WRITE_APPEND")