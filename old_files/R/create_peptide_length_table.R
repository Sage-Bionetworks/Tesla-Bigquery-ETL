library(synapser)
library(tidyverse)
library(magrittr)

synapser::synLogin()

round1_id <- "syn17068546"
round2_id <- "syn17068588"
roundx_id <- "syn16804262"

source("utils.R")


training_patients <- c("1", "2", "10", "103", "210")
testing_patients <- c("4", "5", "7", "11", "12", "101", "212")
validation_patients <- c("8", "9", "102")

all_pats <- c(training_patients, testing_patients, validation_patients)

round1_df <- round1_id %>% 
    create_df_from_synapse_id() %>% 
    select(-LJI_BINDING) %>% 
    gather(key = "ASSAY", value = "STATUS", -c(PATIENT, ALT_EPI_SEQ, HLA_ALLELE)) %>% 
    mutate(PATIENT = 100 + PATIENT) 

round2_df <- round2_id %>% 
    create_df_from_synapse_id() %>% 
    select(-LJI_BINDING) %>% 
    gather(key = "ASSAY", value = "STATUS", -c(PATIENT, ALT_EPI_SEQ, HLA_ALLELE)) %>% 
    mutate(PATIENT = 200 + PATIENT)


roundx_df <- roundx_id %>% 
    create_df_from_synapse_id() %>% 
    gather(key = "ASSAY", value = "STATUS", -c(PATIENT, ALT_EPI_SEQ, HLA_ALLELE)) %>% 
    mutate(PATIENT = as.numeric(str_remove_all(PATIENT, "x"))) 


df <- 
    bind_rows(round1_df, round2_df, roundx_df) %>% 
    drop_na() %>% 
    select(PATIENT, ALT_EPI_SEQ) %>% 
    distinct %>% 
    filter(PATIENT %in% all_pats) %>% 
    mutate(LENGTH = nchar(ALT_EPI_SEQ)) %>% 
    group_by(PATIENT) %>% 
    dplyr::summarise(
        MAX = max(LENGTH), 
        MIN = min(LENGTH), 
        MED = median(LENGTH))

# public
table <- synapser::synBuildTable("Round x epitope length summary", "syn7362874", df)
table <- synStore(table)

