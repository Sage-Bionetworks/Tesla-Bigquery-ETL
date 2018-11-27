library(synapser)
library(tidyverse)
library(data.table)
library(magrittr)
library(readxl)



xlsx_id   <- "syn17068460"
upload_id <- "syn12550354"


source("utils.R")

synLogin()

df <- xlsx_id %>% 
    download_from_synapse() %>% 
    read_excel(skip = 1) %>% 
    mutate(HLA2 = str_remove(Comments_HLA_Binding, "Also called with ")) %>% 
    mutate(HLA2 = str_sub(HLA2, end = 6)) %>% 
    unite(HLA_ALLELE, HLA, HLA2, sep = ";", remove = T) %>% 
    separate_rows(HLA_ALLELE, sep = ";") %>% 
    filter(HLA_ALLELE != "NA") %>% 
    mutate(HLA_ALLELE = str_c(str_sub(HLA_ALLELE, end = 1), "*", str_sub(HLA_ALLELE, 2))) %>% 
    mutate(PATIENT = str_remove(TESLA_Pat., "TESLA_P")) %>% 
    select(-c(TESLA_Pat., Peptide_ID, Length, Overall, Comments_HLA_Binding, 
              Comments_ICS, Comments_IML, Comments_NP)) %>% 
    set_colnames(c("ALT_EPI_SEQ", "HLA_ALLELE", "LJI_BINDING", "TCR_FLOW_I", 
                   "TCR_FLOW_II", "TCR_BINDING", 
                   "TCELL_REACTIVITY", "PATIENT"))

df1 <- df %>% 
    select("ALT_EPI_SEQ", "HLA_ALLELE", "PATIENT", "LJI_BINDING") %>% 
    drop_na()

df2 <- df %>% 
    select(- "LJI_BINDING") %>% 
    gather(key = "assay",  value = "result", -c("ALT_EPI_SEQ", "HLA_ALLELE", "PATIENT")) %>% 
    drop_na() %>% 
    filter(result %in% c("+", "-")) %>% 
    distinct() %>% 
    spread(key = "assay",  value = "result")
    
final_df <- full_join(df1, df2)


activity_obj <- Activity(
    name = "create",
    description = "format validation results from round 1 excel sheet",
    used = list(xlsx_id),
    executed = list("https://github.com/Sage-Bionetworks/Tesla-Bigquery-ETL/blob/master/R/format_round1_validations.R")
)
write_csv(final_df, "round1_validation.csv")
upload_file_to_synapse("round1_validation.csv", upload_id, activity_obj = activity_obj)