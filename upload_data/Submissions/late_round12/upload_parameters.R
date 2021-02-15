library(bigrquery)
library(tidyverse)


parameters_df <- "parameters.csv" %>% 
    readr::read_csv() %>% 
    dplyr::mutate(SUBMISSION_ID = as.character(SUBMISSION_ID))

parameter_tbl <- bq_table("neoepitopes", "Version_4", table = "Parameters")
bq_table_upload(parameter_tbl, parameters_df, write_disposition = "WRITE_APPEND")


