library(synapser)
library(tidyverse)
library(data.table)
library(magrittr)



validation_id <- "syn16804255"
upload_id <- "syn16804254"

source("utils.R")

synLogin()


df <- validation_id %>% 
    create_df_from_synapse_id() %>% 
    select(Patient, `Mutant peptide`, `HLA restriction`, `CD8+ T cell response observed in patient`) %>% 
    set_colnames(c("PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "TCR_FLOW_II")) %>% 
    mutate(PATIENT = str_match(PATIENT, "Patient 0([0-9]{2})")[,2]) %>% 
    mutate(PATIENT = as.character(as.numeric(PATIENT))) %>%  
    mutate(PATIENT = str_c("x", PATIENT)) %>% 
    filter(TCR_FLOW_II %in% c("YES", "NO")) %>% 
    mutate(TCR_FLOW_II = ifelse(TCR_FLOW_II == "YES", "+", "-")) %>% 
    drop_na()

activity_obj <- Activity(
    name = "create",
    description = "format validation results from round x excel sheet",
    used = list(validation_id),
    executed = list("https://github.com/Sage-Bionetworks/Tesla-Bigquery-ETL/blob/master/R/format_roundx_validations.R")
)
write_csv(df, "roundx_validation.csv")
upload_file_to_synapse("roundx_validation.csv", upload_id, activity_obj = activity_obj)
    
