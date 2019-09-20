library(magrittr)

synapser::synLogin()
devtools::source_url("https://raw.githubusercontent.com/Sage-Bionetworks/synapse_tidy_utils/master/utils.R")



tbl <- "syn12292430" %>% 
    create_entity_tbl() %>% 
    dplyr::filter(stringr::str_detect(name, "segmetrics.tsv$")) %>% 
    dplyr::select(id, name) %>% 
    tidyr::separate(name, sep = "_", into = c("PATIENT_ID"), extra = "drop") %>% 
    dplyr::mutate(
        PATIENT_ID = stringr::str_remove_all(PATIENT_ID, "patient"),
        tbl = purrr::map(id, synapse_file_to_tbl)
    ) %>% 
    tidyr::unnest() %>% 
    dplyr::select(- id) %>% 
    magrittr::set_colnames(toupper(colnames(.))) %>% 
    dplyr::rename(CHROM = CHROMOSOME) %>% 
    dplyr::mutate(SEGMENT_ID = stringr::str_c(
        PATIENT_ID,
        CHROM, 
        START,
        END,
        sep = "_"
    )) %>% 
    dplyr::select(SEGMENT_ID, dplyr::everything())

gene_tbl <- tbl %>% 
    dplyr::select(SEGMENT_ID, GENE) %>% 
    tidyr::separate_rows(GENE) %>% 
    dplyr::distinct()

segments_tbl <- dplyr::select(tbl, -GENE)


bq_gene_tbl <- bigrquery::bq_table("neoepitopes", "Version_3", table = "CNVKit_Segment_Genes")
bigrquery::bq_table_upload(bq_gene_tbl, gene_tbl, write_disposition = "WRITE_APPEND")

bq_segment_tbl <- bigrquery::bq_table("neoepitopes", "Version_3", table = "CNVKit_Segments")
bigrquery::bq_table_upload(bq_segment_tbl, segments_tbl, write_disposition = "WRITE_APPEND")
