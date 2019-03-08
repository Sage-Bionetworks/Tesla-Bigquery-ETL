# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)

source("../../utils.R")


mutate_roundx_patient_ids <- function(ids){
    ids <- dplyr::if_else(
        nchar(ids) == 3, 
        stringr::str_sub(ids, start = 2), 
        stringr::str_c("x", ids))
    stringr::str_remove_all(ids, "^0")
}

upload_dir     <- "syn18386505"
vcf_upload_dir <- "syn18400085" 

tesla_files <- c("TESLA_OUT_1.csv", "TESLA_VCF.vcf", "TESLA_OUT_3.csv",  "TESLA_YAML.yaml",
                 "TESLA_OUT_2.csv", "TESLA_OUT_4.csv")

roundx_testing_df    <- readr::read_csv("roundx_testing.csv")
roundx_validation_df <- readr::read_csv("roundx_validation.csv")

roundx_training_df   <- "roundx_training.csv" %>% 
    readr::read_csv() %>% 
    dplyr::rename(TEAM = team)

dir.create("temp_dir")
setwd("temp_dir")

synLogin()

tesla_team_df <- "SELECT * from syn8220615" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(realTeam, alias) %>% 
    magrittr::set_colnames(c("team", "TEAM"))



download_and_unzip_file <- function(id){
    submission <- synapser::synGetSubmission(id)
    path <- submission$filePath
    zip_file <- basename(path)
    file.rename(path, zip_file)
    command <- stringr::str_c("unzip ", zip_file)
    system(command)
    file.remove(zip_file)
}


upload_files_to_synpase <- function(objectId, TEAM, patientId, prefix){
    
    anno_list <- list(
        "submissionId" = objectId,
        "team" = TEAM,
        "patientId" = patientId,
        "round" = "x"
    )
    
    print(anno_list)
    
    download_and_unzip_file(objectId)
    
    file_df <- 
        list.files() %>% 
        tibble::enframe(name = NULL, value = "file_name") %>% 
        dplyr::filter(file_name %in% tesla_files) %>% 
        dplyr::mutate(path = stringr::str_c(prefix, "_", file_name)) %>% 
        dplyr::mutate(synapse_id = if_else(
            file_name == "TESLA_VCF.vcf", vcf_upload_dir, upload_dir)) 
    
    
    file_df %>% 
        dplyr::select(file_name, path) %>% 
        magrittr::set_colnames(c("from", "to")) %>% 
        pmap(file.rename)
    
    synapse_ids <- file_df %>% 
        dplyr::select(-file_name) %>% 
        purrr::pmap(
            upload_file_to_synapse, 
            annotation_list = anno_list,
            ret = "syn_id") %>% 
        unlist
    
    system("rm -rf *")
}

roundx_df <- 
    dplyr::bind_rows(roundx_testing_df, roundx_validation_df) %>% 
    dplyr::left_join(tesla_team_df, by = "team") %>% 
    dplyr::select(-team) %>% 
    dplyr::bind_rows(roundx_training_df) %>% 
    dplyr::mutate(patientId = as.character(patientId)) %>% 
    dplyr::mutate(patientId = mutate_roundx_patient_ids(patientId)) %>% 
    dplyr::mutate(DATETIME = lubridate::as_datetime(createdOn / 1000)) %>% 
    dplyr::arrange(DATETIME) %>% 
    dplyr::group_by(TEAM, patientId) %>% 
    dplyr::mutate(num = 1:n()) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(prefix = stringr::str_c(TEAM, patientId, num, sep = "_")) %>% 
    dplyr::select(objectId, TEAM, patientId, prefix) %>% 
    purrr::pmap(upload_files_to_synpase)

setwd("../")
file.remove("temp_dir")
