library(synapser)
library(tidyverse)
library(data.table)
library(magrittr)
library(readxl)



xlsx_id   <- "syn17068462"
upload_id <- "syn17068461"


source("utils.R")

synLogin()

df <- xlsx_id %>% 
    download_from_synapse() %>% 
    read_excel %>% 
    select(-"ICS (MSSM)") %>% 
    mutate(`Flow (IML)` = if_else(
        `Comments on Flow results` == 
            "Negative on TILs; Positive on PBMCs",
        "-",
        `Flow (IML)`)) %>% 
    separate_rows(HLA, sep = "; ") %>% 
    mutate(PATIENT = str_remove(Patient_ID, "MSK_Pat")) %>% 
    select(PATIENT, Peptide, HLA, `HLA Binding (LJI)`, `Flow (IML)`) %>% 
    set_colnames(c("PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "LJI_BINDING", "TCR_FLOW_I")) %>% 
    mutate(HLA_ALLELE = str_c(str_sub(HLA_ALLELE, end = 1), "*", str_sub(HLA_ALLELE, 2)))  
    
df1 <- df %>% 
    select(-"TCR_FLOW_I") %>% 
    drop_na()

df2 <- df %>% 
    select(- "LJI_BINDING") %>% 
    drop_na()

    
final_df <- full_join(df1, df2)


activity_obj <- Activity(
    name = "create",
    description = "format validation results from round 2 excel sheet",
    used = list(xlsx_id),
    executed = list("https://github.com/Sage-Bionetworks/Tesla-Bigquery-ETL/blob/master/R/format_round1_validations.R")
)
write_csv(final_df, "round2_validation.csv")
upload_file_to_synapse("round2_validation.csv", upload_id, activity_obj = activity_obj)