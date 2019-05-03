library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

df <- "syn16804262" %>% 
    create_df_from_synapse_id() %>% 
    dplyr::mutate(TISSUE_TYPE = "expanded TILs") %>% 
    dplyr::rename(PATIENT_ID = PATIENT)
    
tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3", 
    table = "Validated_Bindings")

bigrquery::bq_table_upload(
    tbl, 
    df, 
    write_disposition = "WRITE_APPEND")
