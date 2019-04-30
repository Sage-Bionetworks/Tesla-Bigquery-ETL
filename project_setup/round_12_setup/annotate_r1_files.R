library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)

synapser::synLogin()

r1_cutoff <- lubridate::as_datetime("2017-05-27 00:00:00")

submission_df <- "../../round12.csv" %>% 
    readr::read_csv() %>% 
    filter(round == "1") %>% 
    dplyr::mutate(datetime = lubridate::as_datetime(createdOn / 1000)) %>% 
    arrange(desc(datetime)) %>% 
    filter(str_detect(submissionName, "^[0-9]+.zip$")) %>% 
    arrange(desc(datetime)) %>% 
    filter(datetime < r1_cutoff) %>% 
    group_by(patientId, team) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(team, patientId, objectId) %>% 
    mutate(patientId = as.character(patientId)) %>% 
    rename(submissionId = objectId) %>% 
    mutate(submissionId = as.character(submissionId))

file_df <- 
    "select id, name, patientId, team, round from syn18387034 where round = '1' and HLAclass is null" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(id, patientId, team, round) %>% 
    mutate(patientId = as.character(patientId)) %>% 
    left_join(submission_df) %>% 
    rename(entity = id)
    


annotations_df <- file_df %>%
    tidyr::nest(-entity, .key = annotations)

purrr::pmap(annotations_df, synapser::synSetAnnotations)

