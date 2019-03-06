library(synapser)
library(bigrquery)
library(tidyverse)
library(magrittr)
library(yaml)
library(wrapr)


synLogin()

source("../../utils.R")

COL_FUNCS <- list(
    "used" = as.logical,
    "changed" = as.logical,
    "comment" = as.character,
    "name" = as.character,
    "relationship" = as.character,
    "value" = as.numeric,
    "values" = as.character,
    "unit" = as.character)



submission_df <- 
    "select id, name, submissionId, patientId from syn18387034 where round = 'x'" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(id, name, submissionId, patientId) %>% 
    dplyr::mutate(file_type = stringr::str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(-name) %>% 
    dplyr::filter(file_type == "TESLA_YAML.yaml") %>% 
    spread(key = "file_type", value = "id")







create_yaml_tables <- function(args){
    
    # args <- submission_df[307,]
    print(args)
    yml_obj <- args$TESLA_YAML.yaml %>%
        download_from_synapse %>% 
        readLines() %>% 
        str_replace_all("null", "NA") %>% 
        str_c(collapse  = "\n") %>% 
        yaml::yaml.load() 
    
    steps_df <- yml_obj %>% 
        map(extract, c("used", "changed", "comment")) %>% 
        map(as_tibble) %>% 
        map(convert_df_to_types) %>% 
        imap(~ inset(.x, "step", value = .y)) %>% 
        bind_rows()

    parameter_df <- yml_obj %>% 
        map('key_parameters') %>% 
        keep(map_lgl(., is.list)) %>%
        map(~ map(.x, param_list_to_df)) %>% 
        map(bind_rows) %>% 
        imap(~ inset(.x, "step", value = .y)) %>% 
        bind_rows()
    
}

param_list_to_df <- function(lst){
    lst %>% 
        as_tibble() %>% 
        convert_df_to_types
}

convert_df_to_types <- function(df){
    for(col in names(COL_FUNCS)){
        df <- mutate_col_if_exists(
            df,
            col, 
            col,
            COL_FUNCS[[col]])
    }
    return(df)
}

mutate_col_if_exists <- function(df, old_col, new_col, func, ...){
    if(!is.null(magrittr::extract2(df, old_col))){
        wrapr::let(
            alias = c(
                COL1 = old_col,
                COL2 = new_col), {
                    df  <- dplyr::mutate(df, COL2 = func(COL1, ...))
                }
        )
    } 
    return(df)
}


df <- submission_df %>%
    dplyr::ungroup() %>%
    dplyr::group_by(submissionId) %>%
    dplyr::group_split() %>%
    purrr::map(create_yaml_tables)
