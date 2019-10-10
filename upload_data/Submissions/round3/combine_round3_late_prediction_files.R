library(synapser)
library(tidyverse)
library(magrittr)
library(data.table)

synapser::synLogin()

list.files() %>%
    purrr::keep(., stringr::str_detect(., "[:digit:]+_protein_position_dff.csv")) %>%
    purrr::map(data.table::fread) %>%
    purrr::map(dplyr::as_tibble) %>%
    dplyr::bind_rows() %>%
    readr::write_csv("round3_late_protein_positions.csv")

tbls <- list.files() %>%
    purrr::keep(., stringr::str_detect(., "[:digit:]+_bad_prediction_df.csv")) %>%
    purrr::map(data.table::fread) %>%
    purrr::map(dplyr::as_tibble) %>%
    purrr::discard(., purrr::map_int(., nrow) == 0) %>%
    purrr::map(dplyr::mutate, SOURCE_ROW_N = as.integer(SOURCE_ROW_N)) %>%
    dplyr::bind_rows() %>%
    readr::write_csv("round3_late_bad_prediction_df.csv")

list.files() %>%
    purrr::keep(., stringr::str_detect(., "[:digit:]+_prediction_df.csv")) %>%
    purrr::map(data.table::fread) %>%
    purrr::map(dplyr::as_tibble) %>%
    purrr::discard(., purrr::map_int(., nrow) == 0) %>%
    purrr::map(dplyr::mutate, SOURCE_ROW_N = as.integer(SOURCE_ROW_N)) %>%
    dplyr::bind_rows() %>%
    readr::write_csv("round3_late_prediction_df.csv")

list.files() %>%
    purrr::keep(., stringr::str_detect(., "[:digit:]+_variant_prediction_df.csv")) %>%
    purrr::map(data.table::fread) %>%
    purrr::map(dplyr::as_tibble) %>%
    purrr::discard(., purrr::map_int(., nrow) == 0) %>%
    dplyr::bind_rows() %>%
    readr::write_csv("round3_late_variant_prediction_df.csv")
