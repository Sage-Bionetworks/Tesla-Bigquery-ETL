library(synapser)
library(magrittr)
library(tidyverse)

devtools::source_url("https://raw.githubusercontent.com/Sage-Bionetworks/synapse_tidy_utils/master/utils.R")

synapser::synLogin()

tbl <- query_synapse_table("select id, submissionId, round, patientId, team from syn18387034 where round = '3'") 
    
tbl2 <- tbl %>% 
    dplyr::mutate(
        submissionId = as.character(as.integer(submissionId)),
        patientId = as.character(as.integer(patientId))
    ) %>% 
    dplyr::rename(entity = id) %>% 
    tidyr::nest(annotations = -entity)

purrr::pmap(tbl2, synapser::synSetAnnotations)
