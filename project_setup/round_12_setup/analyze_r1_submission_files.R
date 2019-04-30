library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)


synLogin()

submission_file_r1_df <- 
    "select id, name, patientId, team, createdOn from syn18387034 where round = '1' and HLAclass is null" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(file_type = stringr::str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(id, patientId, team, file_type, createdOn) %>% 
    arrange(desc(createdOn))

submission_r1_df <- "../../round12.csv" %>% 
    readr::read_csv() %>% 
    filter(round == "1") %>% 
    dplyr::mutate(datetime = lubridate::as_datetime(createdOn / 1000)) %>% 
    arrange(desc(datetime))


submission_file_r2_df <- 
    "select id, name, patientId, team, createdOn from syn18387034 where round = '2' and HLAclass is null" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(file_type = stringr::str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(id, patientId, team, file_type, createdOn) %>% 
    arrange(desc(createdOn))

submission_r2_df <- "../../round12.csv" %>% 
    readr::read_csv() %>% 
    filter(round == "2") %>% 
    dplyr::mutate(datetime = lubridate::as_datetime(createdOn / 1000)) %>% 
    arrange(desc(datetime))
