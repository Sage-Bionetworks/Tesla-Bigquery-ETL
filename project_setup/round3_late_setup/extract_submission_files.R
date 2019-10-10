# must run ./query_queues.sh to get correct files

library(synapser)
library(tidyverse)
library(magrittr)
library(lubridate)

devtools::source_url("https://raw.githubusercontent.com/Sage-Bionetworks/synapse_tidy_utils/master/utils.R")


source("../../utils.R")

upload_dir     <- "syn20781822"
vcf_upload_dir <- "syn20781824" 

tesla_files <- c("TESLA_OUT_1.csv", "TESLA_VCF.vcf", "TESLA_OUT_3.csv",  "TESLA_YAML.yaml",
                 "TESLA_OUT_2.csv", "TESLA_OUT_4.csv")

# challengeutils query 'select objectId, createdOn, team, patientId from evaluation_9614265 where status == "VALIDATED" and objectId > 9692809' > round3.csv 

synLogin()



bird_df <- "SELECT * from syn8220615" %>% 
    query_synapse_table() %>% 
    dplyr::select(realTeam, alias) %>% 
    magrittr::set_colnames(c("team", "bird_name"))

insect_df <- "SELECT * from syn20782002" %>% 
    query_synapse_table() %>% 
    dplyr::select(realTeam, alias) %>% 
    magrittr::set_colnames(c("team", "insect_name"))


uploaded_submissions <- "select * from syn18387034 where round = '3'" %>% 
    query_synapse_table() %>% 
    dplyr::arrange(createdOn) %>% 
    dplyr::filter(stringr::str_detect(name, "TESLA_YAML.yaml")) %>% 
    dplyr::pull(submissionId) %>% 
    as.integer()
    
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
        "round" = "3"
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

dir.create("temp_dir")
setwd("temp_dir")

query_df <- readr::read_csv("../round3.csv") %>% 
    dplyr::mutate(DATETIME = lubridate::as_datetime(createdOn / 1000)) %>% 
    dplyr::arrange(dplyr::desc(DATETIME)) %>% 
    dplyr::group_by(patientId, team) %>% 
    dplyr::slice(1) %>% 
    dplyr::ungroup() %>% 
    dplyr::select(-c(DATETIME, createdOn)) %>% 
    dplyr::rename(bird_name = team) %>% 
    dplyr::left_join(bird_df) %>% 
    dplyr::left_join(insect_df) %>% 
    dplyr::rename(TEAM = insect_name) %>% 
    dplyr::select(-c(team, bird_name)) %>% 
    dplyr::filter(!objectId %in% uploaded_submissions) %>% 
    dplyr::mutate(
        prefix = stringr::str_c(TEAM, patientId, sep = "_"),
        objectId = as.character(objectId),
        patientId = as.character(patientId)
        
    ) %>% 
    purrr::pmap(upload_files_to_synpase)

setwd("../")
file.remove("temp_dir")
