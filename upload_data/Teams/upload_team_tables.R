library(synapser)
library(tidyverse)
library(magrittr)
library(bigrquery)


source("../../utils.R")

synLogin()

tesla_team_df <- "SELECT * from syn8220615" %>% 
    synapser::synTableQuery() %>% 
    as.data.frame() %>% 
    tibble::as_tibble() %>% 
    dplyr::select(alias) %>%
    dplyr::rename(TEAM = alias) %>% 
    dplyr::distinct()

survey_df1 <- "syn11300043" %>% 
    synapser::synGet() %>% 
    magrittr::use_series(path) %>% 
    readr::read_csv() %>% 
    dplyr::rename(TEAM = X1)

team_df <- tesla_team_df %>% 
    dplyr::full_join(survey_df1) %>% 
    dplyr::select(TEAM, Comments) %>% 
    dplyr::rename(SURVEY_COMMENTS = Comments)

survey_df2 <- survey_df1 %>% 
    dplyr::select(-Comments) %>% 
    tidyr::gather(key = "question", value = "ANSWER", - TEAM) %>% 
    tidyr::drop_na()

survey_df3 <- survey_df2 %>% 
    dplyr::select(-c(TEAM, ANSWER)) %>% 
    dplyr::distinct() %>% 
    dplyr::mutate(QUESTION_NAME = stringr::str_c("Question", 1:n()))

survey_answers_df <- survey_df2 %>% 
    dplyr::left_join(survey_df3) %>% 
    dplyr::select(TEAM, QUESTION_NAME, ANSWER) %>% 
    dplyr::mutate(ANSWER = if_else(ANSWER == "yes", T, F))

survey_questions_df <- survey_df3 %>% 
    tidyr::separate(question, sep = ": ", into = c(
        "CATEGORY", "QUESTION_PART_1", "QUESTION_PART_2")) %>% 
    dplyr::mutate(QUESTION_PART_2 = stringr::str_remove_all(
        QUESTION_PART_2, "[\\[\\]]")) %>% 
    dplyr::select(QUESTION_NAME, CATEGORY, QUESTION_PART_1, QUESTION_PART_2)

team_tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3", 
    table = "Teams")

survey_answers_tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3",
    table = "Survey_Answers")

survey_questions_tbl <- bigrquery::bq_table(
    "neoepitopes", 
    "Version_3",
    table = "Survey_Questions")

bigrquery::bq_table_upload(
    team_tbl, 
    team_df, 
    write_disposition = "WRITE_TRUNCATE")

bigrquery::bq_table_upload(
    survey_answers_tbl, 
    survey_answers_df, 
    write_disposition = "WRITE_TRUNCATE")

bigrquery::bq_table_upload(
    survey_questions_tbl, 
    survey_questions_df, 
    write_disposition = "WRITE_TRUNCATE")



