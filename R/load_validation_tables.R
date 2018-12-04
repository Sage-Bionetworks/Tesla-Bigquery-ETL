library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)

synapser::synLogin()

round1_id <- "syn17068546"
round2_id <- "syn17068588"
roundx_id <- "syn16804262"

source("utils.R")

df <- 
    list(round1_id, round2_id, roundx_id) %>% 
    purrr::map(create_df_from_synapse_id) %>% 
    purrr::map(dplyr::mutate, PATIENT = as.character(PATIENT)) %>% 
    dplyr::bind_rows() %>% 
    dplyr::mutate(HLA_ALLELE = stringr::str_remove_all(HLA_ALLELE, "[:punct:]"))

epitope_df <- df %>% 
    dplyr::select(PATIENT, ALT_EPI_SEQ, TCELL_REACTIVITY) %>% 
    dplyr::distinct() %>% 
    tidyr::drop_na()

binding_df <- df %>% 
    dplyr::select(- TCELL_REACTIVITY) %>% 
    dplyr::mutate(PMHC = stringr::str_c(ALT_EPI_SEQ, HLA_ALLELE, sep = "_"))

bigrquery::insert_upload_job("neoepitopes", "Version_2", "Validated_bindings", binding_df)
bigrquery::insert_upload_job("neoepitopes", "Version_2", "Validated_epitopes", epitope_df)

    




