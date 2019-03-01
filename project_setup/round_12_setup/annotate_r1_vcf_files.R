library(synapser)
library(tidyverse)
library(magrittr)

synapser::synLogin()

folder_id <- "syn10140059"

files <- folder_id %>% 
    synapser::synGetChildren(includeTypes = list("file")) %>% 
    synapser::as.list() 

annotations_df <- 
    dplyr::tibble(
        name = purrr::map_chr(files, "name"),
        entity = purrr::map_chr(files, "id")
    ) %>% 
    tidyr::separate(name, into = c("team", "patient"), sep = "_", extra = "drop") %>% 
    dplyr::mutate(round = "1") %>% 
    tidyr::nest(-entity, .key = annotations)

purrr::pmap(annotations_df, synapser::synSetAnnotations)

