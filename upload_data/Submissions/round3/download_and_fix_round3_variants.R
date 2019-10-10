library(tidyverse)
library(magrittr)

tbl <- "round3_variants.csv" %>% 
    readr::read_csv()

tbl %>% 
    dplyr::mutate(
        SUBMISSION_ID = as.character(as.integer(SUBMISSION_ID)),
        VARIANT_ID = stringr::str_c(SUBMISSION_ID, CHROM, POS, ALT, sep = "_"),
    ) %>% 
    readr::write_csv("round3_variants.csv")
