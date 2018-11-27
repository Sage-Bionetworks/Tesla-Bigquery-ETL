library(synapser)
library(tidyverse)
library(magrittr)

synapser::synLogin()

round1_id <- "syn17068546"
round2_id <- "syn17068588"
roundx_id <- "syn16804262"
upload_id <- "syn16804254"

source("utils.R")


training_patients <- c("1", "2", "10", "103", "210")
testing_patients <- c("4", "11", "12", "101", "212")
validation_patients <- c("5", "7", "8", "9", "102")

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
    mutate(STATUS = if_else(STATUS == "+", T, F)) %>% 
    group_by(PATIENT, ALT_EPI_SEQ, HLA_ALLELE) %>% 
    dplyr::summarise(STATUS = list(STATUS)) %>% 
    mutate(STATUS = map_lgl(STATUS, any)) %>% 
    mutate(STATUS = if_else(STATUS, "+", "-"))


df %>% 
    filter(PATIENT %in% training_patients) %>% 
    write_csv("training_goldstandard.csv")

df %>% 
    filter(PATIENT %in% testing_patients) %>% 
    write_csv("testing_goldstandard.csv")

df %>% 
    filter(PATIENT %in% validation_patients) %>% 
    write_csv("validation_goldstandard.csv")

activity_obj <- Activity(
    name = "create",
    description = "create roundx gold standard files from rounds 1, 2, x",
    used = list(round1_id, round2_id, roundx_id),
    executed = list("https://github.com/Sage-Bionetworks/Tesla-Bigquery-ETL/blob/master/R/create_gold_standard_files.R")
)

upload_file_to_synapse("training_goldstandard.csv", upload_id, activity_obj = activity_obj)
upload_file_to_synapse("testing_goldstandard.csv", upload_id, activity_obj = activity_obj)
upload_file_to_synapse("validation_goldstandard.csv", upload_id, activity_obj = activity_obj)





