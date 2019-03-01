library(synapser)
library(tidyverse)
library(magrittr)

synapser::synLogin()

folder_id <- "syn9952809"

annotations_df <- folder_id %>% 
    synapser::synGetChildren(includeTypes = list("folder")) %>%
    synapser::as.list() %>%
    purrr::map_chr("id") %>%
    purrr::map(synapser::synGetChildren, includeTypes = list("file")) %>%
    purrr::map(synapser::as.list) %>% 
    purrr::flatten() %>% 
    purrr::map_chr("id") %>% 
    tibble::enframe(name = NULL, value = "entity") %>% 
    dplyr::mutate(annotations = purrr::map(entity, synapser::synGetAnnotations)) %>% 
    dplyr::mutate(annotations = purrr::map(annotations, as.tibble)) %>% 
    tidyr::unnest() %>% 
    tidyr::unnest() %>% 
    dplyr::mutate(patientId = as.character(patientId)) %>% 
    dplyr::mutate(round = as.character(round)) %>% 
    dplyr::select(-patient) %>% 
    tidyr::nest(-entity, .key = annotations)
    
purrr::pmap(annotations_df, synapser::synSetAnnotations)

