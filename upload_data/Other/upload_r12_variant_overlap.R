library(magrittr)

synapser::synLogin()
devtools::source_url("https://raw.githubusercontent.com/Sage-Bionetworks/synapse_tidy_utils/master/utils.R")

round1_df <- "syn10491543" %>% 
    create_entity_tbl() %>% 
    dplyr::filter(stringr::str_detect(name, ".txt$"))

round2_df <- "syn10817102" %>% 
    create_entity_tbl() %>% 
    dplyr::filter(stringr::str_detect(name, "t20.txt$"))

tbl <- 
    dplyr::bind_rows(round1_df, round2_df) %>% 
    dplyr::select(id, name) %>% 
    tidyr::separate(name, sep = "_", into = c("PATIENT_ID"), extra = "drop") %>% 
    dplyr::mutate(
        PATIENT_ID = stringr::str_remove_all(PATIENT_ID, "p"),
        tbl = purrr::map(id, synapse_file_to_tbl, col_names = F)
    ) %>% 
    tidyr::unnest() %>% 
    dplyr::select(
        TEAM = "X1",
        TEAM2 = "X2",
        PATIENT_ID,
        TYPE = "X3",
        UNIQUE_VARIANTS = "X4",
        UNIQUE_VARIANTS2 = "X5",
        SHARED_VARIANTS = "X6" 
    ) %>% 
    dplyr::mutate(
        TEAM = stringr::str_remove_all(TEAM, "_[:print:]+"),
        TEAM2 = stringr::str_remove_all(TEAM2, "_[:print:]+"),
        TYPE = stringr::str_remove_all(TYPE, "[:punct:]")
    )

bq_tbl <- bigrquery::bq_table("neoepitopes", "Version_3", table = "Variant_Overlap")
bigrquery::bq_table_upload(bq_tbl, tbl, write_disposition = "WRITE_APPEND")
