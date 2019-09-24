library(synapser)
library(tidyverse)
library(magrittr)
library(data.table)

synpaser::synLogin()

list.files() %>% 
    purrr::keep(., stringr::str_detect(., "[:digit:]+_protein_position_dff.csv")) %>% 
    purrr::map(data.table::fread) %>% 
    purrr::map(dplyr::as_tibble) %>% 
    dplyr::bind_rows() %>% 
    readr::write("protein_position_df.csv.csv")
