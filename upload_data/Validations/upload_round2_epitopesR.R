library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

df <- "syn18498206" %>% 
    create_df_from_synapse_id() %>% 
    dplyr::mutate(PATIENT_ID = as.character(PATIENT_ID))
    
tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3", 
    table = "Validated_Epitopes")

bigrquery::bq_table_upload(
    tbl, 
    df, 
    write_disposition = "WRITE_APPEND")