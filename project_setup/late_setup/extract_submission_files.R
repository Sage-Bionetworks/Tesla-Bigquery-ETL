# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)

source("../../utils.R")

upload_dir     <- "syn21212423"
vcf_upload_dir <- "syn21212422" 

tesla_files <- c("TESLA_OUT_1.csv", "TESLA_VCF.vcf", "TESLA_OUT_3.csv",  "TESLA_YAML.yaml",
                 "TESLA_OUT_2.csv", "TESLA_OUT_4.csv")

BQ_PROJECT <- "neoepitopes"
BQ_DATASET <- "Version_4"

BQ_DBI     <- DBI::dbConnect(
    bigrquery::bigquery(), 
    project = BQ_PROJECT,
    dataset = BQ_DATASET)

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


upload_files_to_synpase <- function(objectId, TEAM, patientId, prefix, round){
    
    anno_list <- list(
        "submissionId" = objectId,
        "team" = TEAM,
        "patientId" = patientId,
        "round" = round
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

submissions_tbl <- BQ_DBI %>% 
    dplyr::tbl("Submissions") %>% 
    dplyr::filter(ROUND %in% c("1", "2")) %>% 
    dplyr::select(TEAM, PATIENT_ID) %>% 
    dplyr::as_tibble()

query_tbl <- readr::read_csv("../late_round12.csv") %>% 
    dplyr::mutate(
        DATETIME = lubridate::as_datetime(createdOn / 1000),
        patientId = as.character(patientId),
        objectId = as.character(as.integer(objectId))
    ) %>%
    dplyr::mutate(round = dplyr::if_else(
        patientId %in% c("1", "2", "3", "4", "5"),
        "1",
        "2"
    )) %>% 
    dplyr::arrange(dplyr::desc(DATETIME)) %>% 
    dplyr::group_by(patientId, team) %>% 
    dplyr::slice(1) %>% 
    dplyr::ungroup() %>% 
    dplyr::select(-c(DATETIME, createdOn)) %>% 
    dplyr::anti_join(submissions_tbl, by = c("team" = "TEAM", "patientId" = "PATIENT_ID")) %>% 
    dplyr::mutate(prefix = stringr::str_c(team, patientId, sep = "_")) %>% 
    dplyr::select(objectId, TEAM = team, patientId, prefix, round) %>% 
    purrr::pmap(upload_files_to_synpase)

setwd("../")
file.remove("temp_dir")
