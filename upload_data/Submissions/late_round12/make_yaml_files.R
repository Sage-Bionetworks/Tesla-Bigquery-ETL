library(synapser)
library(tidyverse)
library(magrittr)
library(yaml)
library(wrapr)


synLogin()

source("../../../utils.R")
devtools::source_url("https://raw.githubusercontent.com/Sage-Bionetworks/synapse_tidy_utils/master/utils.R")


COL_FUNCS <- list(
    "used" = as.logical,
    "changed" = as.logical,
    "comment" = as.character,
    "name" = as.character,
    "relationship" = as.character,
    "value" = as.numeric,
    "values" = as.character,
    "unit" = as.character)



submission_file_tbl  <- create_entity_tbl("syn21212423")

annotated_file_tbl <- submission_file_tbl %>% 
    dplyr::select(id, name) %>% 
    add_annotations_to_tbl()

yaml_file_tbl <- annotated_file_tbl %>% 
    dplyr::select(id, name, submissionId) %>% 
    dplyr::mutate(file_type = stringr::str_match(name, "TESLA_[:print:]+$")) %>% 
    dplyr::select(-name) %>% 
    dplyr::filter(file_type == "TESLA_YAML.yaml") %>% 
    spread(key = "file_type", value = "id")



create_yaml_tables <- function(args){
    print(args)
    
    yml_obj <- args$TESLA_YAML.yaml %>%
        download_from_synapse %>% 
        readLines() %>% 
        stringr::str_replace_all("null", "NA") %>% 
        stringr::str_c(collapse  = "\n") %>% 
        yaml::yaml.load() 
    
    steps_df <- yml_obj %>% 
        purrr::map(magrittr::extract, c("used", "changed", "comment")) %>% 
        purrr::map(dplyr::as_tibble) %>% 
        purrr::map(convert_df_to_types) %>% 
        purrr::imap(~ magrittr::inset(.x, "step", value = .y)) %>% 
        dplyr::bind_rows() %>% 
        dplyr::mutate(SUBMISSION_ID = args$submissionId)
    
    parameters_df <- yml_obj %>% 
        purrr::map('key_parameters') %>% 
        purrr::keep(purrr::map_lgl(., is.list)) %>%
        purrr::map(~ purrr::map(.x, param_list_to_df)) %>% 
        purrr::map(dplyr::bind_rows) %>% 
        purrr::imap(~ inset(.x, "step", value = .y)) %>% 
        dplyr::bind_rows() %>% 
        dplyr::mutate(SUBMISSION_ID = args$submissionId)
    
    return(list(
        "steps_df" = steps_df,
        "parameters_df" = parameters_df
    ))
    
}

param_list_to_df <- function(lst){
    df <- dplyr::as_tibble(lst)
    if("values" %in% colnames(df)){
        df <- df %>% 
            tidyr::unnest() %>% 
            convert_df_to_types() %>% 
            dplyr::group_by(name, relationship) %>% 
            dplyr::summarise(values = stringr::str_c(values, collapse = ";"))
    } else {
        df <- convert_df_to_types(df)
    }
    return(df)
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


tbls <- yaml_file_tbl %>% 
    dplyr::ungroup() %>%
    dplyr::group_by(submissionId) %>%
    dplyr::group_split() %>%
    purrr::map(create_yaml_tables)


change_to_na <- function(string){
    if(string == "NA") string <- NA
    else string <- string
    return(string)
}

steps_tbl <- tbls %>% 
    purrr::map("steps_df") %>% 
    dplyr::bind_rows() %>%  
    dplyr::select(SUBMISSION_ID, step, used, changed, comment) %>% 
    magrittr::set_colnames(c(
        "SUBMISSION_ID",
        "STEP_NAME",
        "USED", 
        "CHANGED", 
        "COMMENT")) %>% 
    dplyr::mutate(COMMENT = map(COMMENT, change_to_na)) %>%
    tidyr::unnest()



   
parameters_tbl <- tbls %>% 
    purrr::map("parameters_df") %>% 
    dplyr::bind_rows() %>% 
    dplyr::select(SUBMISSION_ID, step, name, relationship, values, value, unit) %>% 
    magrittr::set_colnames(c(
        "SUBMISSION_ID",
        "STEP_NAME",
        "PARAMETER_NAME", 
        "RELATIONSHIP", 
        "VALUES",
        "VALUE", 
        "UNIT"))

write_csv(steps_tbl, "steps.csv")
write_csv(parameters_tbl, "parameters.csv")



