library(bigrquery)
library(tidyverse)

steps_df <- "steps.csv" %>% 
    readr::read_csv() %>% 
    dplyr::mutate(SUBMISSION_ID = as.character(SUBMISSION_ID))
step_tbl <- bigrquery::bq_table("neoepitopes", "Version_4", table = "Steps")
bigrquery::bq_table_upload(step_tbl, steps_df, write_disposition = "WRITE_APPEND")



