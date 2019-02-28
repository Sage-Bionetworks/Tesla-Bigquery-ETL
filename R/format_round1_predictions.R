
library(plyr)
library(doMC)
library(synapser)
library(tidyverse)
library(data.table)
library(magrittr)

file_view_id <- "syn11427572"
r1_vcf_id    <- "syn10140059"


source("utils.R")
registerDoMC(cores = detectCores() - 1)
synLogin()


create_df_by_file <- function(args){
    df <- create_df_from_synapse_id(args$id, na.strings = c("NA", "na", "n/a", ""))
    if(nrow(df) == 0) return(NA)
    temp_df <- df %>%
        inset("PATIENT", value = str_c("Patient_", args$patient)) %>%
        inset("TEAM", value = args$team) %>% 
        inset("SOURCE", value = args$source) %>% 
        inset("SOURCE_ROW_N", value = 1:nrow(.)) %>% 
        mutate(PROT_POS = as.character(PROT_POS)) %>% 
        mutate(VAR_ID = as.character(VAR_ID)) 
    if(is.null(temp_df$STEP_ID)){
        result_df <- temp_df %>% 
            group_by_at(vars(-c(PROT_POS, VAR_ID, SOURCE_ROW_N))) %>% 
            summarise(PROT_POS = str_c(unique(PROT_POS), collapse = ";"),
                      VAR_ID = str_c(unique(VAR_ID), collapse = ";"),
                      SOURCE_ROW_N = str_c(unique(SOURCE_ROW_N), collapse = ";")) %>% 
            ungroup
    } else{
        result_df <- temp_df %>% 
            group_by_at(vars(-c(PROT_POS, VAR_ID, SOURCE_ROW_N, STEP_ID))) %>% 
            summarise(PROT_POS = str_c(unique(PROT_POS), collapse = ";"),
                      VAR_ID = str_c(unique(VAR_ID), collapse = ";"),
                      SOURCE_ROW_N = str_c(unique(SOURCE_ROW_N), collapse = ";"),
                      STEP_ID = str_c(unique(STEP_ID), collapse = ";")) %>% 
            ungroup
    }
    return(result_df)
}

remove_empty_cols <- function(df){
    df[,colSums(is.na(df)) < nrow(df)]
}

clean_allele_names <- function(df){
    df1 <- filter(df, str_detect(HLA_ALLELE, "\\*"))
    df2 <- df %>% 
        filter(!str_detect(HLA_ALLELE, "\\*")) %>% 
        mutate(allele = str_sub(HLA_ALLELE, end = 1)) %>% 
        mutate(number = str_sub(HLA_ALLELE, start = 2)) %>% 
        mutate(HLA_ALLELE = str_c(allele, "*", number)) %>% 
        select(-c(allele, number))
    bind_rows(df1, df2)
}

mutate_col_if_exists <- function(df, old_col, new_col, fun, ...){
    if(!is.null(extract2(df, old_col))){
        wrapr::let(
            alias = c(
                COL1 = old_col,
                COL2 = new_col), {
                df  <- mutate(df, COL2 = fun(COL1, ...))
            }
        )
    } 
    return(df)
}



get_vcf_cols <- function(args){
    args$id %>% 
        download_from_synapse %>% 
        fread(skip = "#CHROM") %>% 
        as_tibble %>% 
        select(`#CHROM`, POS, ID, REF, ALT) %>% 
        inset("TEAM", value = args$team) %>% 
        inset("PATIENT", value = str_c("Patient_", args$patient)) %>% 
        rename("CHROM" = `#CHROM`) %>% 
        mutate(CHROM = as.character(CHROM)) %>% 
        mutate(ID = as.character(ID)) %>% 
        rename("VAR_ID" = ID)
}

# get_vcf_cols2 <- function(args){
#     args$id %>% 
#         download_from_synapse %>% 
#         fread(skip = "#CHROM", sep = "\t") %>% 
#         as_tibble %>% 
#         select(1:5) %>% 
#         set_colnames(c("CHROM", "POS", "ID", "REF", "ALT")) %>% 
#         inset("TEAM", value = args$team) %>% 
#         inset("PATIENT", value = str_c("Patient_", args$patient)) %>% 
#         mutate(CHROM = as.character(CHROM)) %>% 
#         mutate(ID = as.character(ID))
# }

get_vcf_cols2 <- function(args){
    args$id %>% 
        download_from_synapse %>% 
        fread(skip = "#CHROM", sep = "\t") %>% 
        as_tibble %>% 
        select("#CHROM", "POS", "ID", "REF", "ALT") %>% 
        set_colnames(c("CHROM", "POS", "ID", "REF", "ALT")) %>% 
        inset("TEAM", value = args$team) %>% 
        inset("PATIENT", value = str_c("Patient_", args$patient)) %>% 
        mutate(CHROM = as.character(CHROM)) %>% 
        mutate(ID = as.character(ID))
}

file_view_df <- file_view_id %>% 
    str_c("select id, name, patientId, round, HLAclass, team from ", .) %>%
    synTableQuery %>% 
    as.data.frame %>% 
    as_tibble() %>% 
    dplyr::rename(patient = patientId) %>% 
    filter(round == 1) %>% 
    filter(is.na(HLAclass)) %>% 
    select(id, name, patient, team)


# r1 --------------------------------------------------------------------------


r1_allowed_cols = c(
    "RANK",
    "ALT_EPI_SEQ",
    "HLA_ALLELE",
    "VAR_ID",
    "REF_EPI_SEQ",
    "PEP_LEN",
    "PROT_POS",
    "HLA_ALLELE_MUT",
    "HLA_ALT_BINDING",
    "HLA_REF_BINDING",
    "RANK_METRICS",
    "RANK_DESC",
    "ADDN_INFO",
    "PATIENT",
    "TEAM",
    "SOURCE",
    "SOURCE_ROW_N",
    "STEP_ID")


r1_prediction_df <- file_view_df %>%
    filter(str_detect(name, "[23].csv$")) %>%
    mutate(source = "fastq") %>%
    split(1:nrow(.)) %>%
    llply(create_df_by_file, .parallel = T) %>%
    unname %>%
    discard(is.na(.)) %>%
    llply(remove_empty_cols, .parallel = T) %>%
    bind_rows %>%
    .[,colnames(.) %in% r1_allowed_cols]

# clean allele names, group by columns that make up an individual prediction
r1_prediction_df <- r1_prediction_df %>%
    filter(!str_detect(HLA_ALLELE, "^D")) %>%
    mutate(HLA_ALLELE = str_remove(HLA_ALLELE, "HLA-")) %>%
    mutate(HLA_ALLELE = str_remove(HLA_ALLELE, "\\*")) %>%
    mutate(HLA_ALLELE = str_remove(HLA_ALLELE, ":")) %>%
    group_by(TEAM, PATIENT, SOURCE, HLA_ALLELE, ALT_EPI_SEQ) %>%
    arrange(RANK)


# bad predictions are those with duplicated epitope/allele combination, per team,
# per patient from the same source file type. Only the higest ranked of these
# duplicates are kept

r1_bad_prediction_df <- r1_prediction_df %>%
    slice(-1)

write_tsv(r1_bad_prediction_df, "r1_BAD_PREDS.tsv")
remove(r1_bad_prediction_df)


r1_prediction_df <- r1_prediction_df %>%
    slice(1) %>%
    mutate(PMHC = str_c(ALT_EPI_SEQ, "_", HLA_ALLELE)) %>%
    mutate(PREDICTION_ID = str_c(TEAM, PATIENT, PMHC, SOURCE, sep = "_"))
r1_prediction_df %>%
    ungroup %>%
    select(PREDICTION_ID, PROT_POS) %>%
    separate_rows(PROT_POS, sep = ";") %>%
    distinct %>%
    write_tsv("r1_PROT_POS.tsv")

r1_prediction_df %>%
    ungroup %>%
    select(PREDICTION_ID, VAR_ID) %>%
    separate_rows(VAR_ID, sep = ";") %>%
    distinct %>%
    write_tsv("r1_VAR_ID.tsv")

r1_prediction_df %>%
    ungroup %>%
    select(-c(VAR_ID, PROT_POS)) %>%
    distinct %>%
    write_tsv("r1_PRED.tsv")

remove(r1_prediction_df)

# vcf files

r1_key_file_df <- file_view_df %>%
    filter(str_detect(name, "1.csv$"))

r1_key_df <- r1_key_file_df %>%
    use_series(id) %>%
    llply(create_df_from_synapse_id, .parallel = T) %>%
    map2(r1_key_file_df$team,
         function(df, team) inset(df, "TEAM", value = team)) %>%
    map2(r1_key_file_df$patient,
         function(df, patient) inset(df, "PATIENT", value = str_c("Patient_", patient))) %>%
    bind_rows %>%
    select(VAR_ID, CHROM, POS, OA_CALLER, TEAM, PATIENT) %>%
    distinct %>%
    mutate(CHROM = str_remove_all(CHROM, "chr"))


r1_vcf_file_df <- r1_vcf_id %>%
    get_file_df_from_synapse_dir_id %>%
    filter(!str_detect(name, "HLA2")) %>% 
    dplyr::mutate(team = str_match(name, "([:alpha:]*)_[0-9]*_TESLA_VCF.vcf")[,2]) %>%
    dplyr::mutate(patient = str_match(name, "[:alpha:]*_([0-9]*)_TESLA_VCF.vcf")[,2])


r1_vcf_df <- r1_vcf_file_df %>%
    split(1:nrow(.)) %>%
    llply(get_vcf_cols2, .parallel = T) %>%
    map(mutate, VAR_N = 1:n()) %>%
    bind_rows %>%
    select(-ID) %>%
    distinct %>%
    mutate(CHROM = str_remove_all(CHROM, "chr")) %>% 
    mutate(VARIANT_ID = str_c(TEAM, PATIENT, as.character(VAR_N), sep = ";"))

combined_df <-
    left_join(r1_key_df, r1_vcf_df) %>%
    mutate(diff = VAR_ID - VAR_N) %>%
    distinct %>%
    filter(!is.na(VAR_N))

id_offset_df <- combined_df %>%
    group_by(TEAM, PATIENT, diff) %>%
    summarise(count = n())

id_offset_df2 <- id_offset_df %>%
    group_by(TEAM, PATIENT) %>%
    filter(count == max(count)) %>%
    select(-count)

crane_key_df <- r1_key_df %>%
    filter(TEAM == "crane") %>%
    group_by(CHROM, POS, TEAM, PATIENT) %>%
    slice(1) %>%
    ungroup %>%
    group_by(PATIENT) %>%
    arrange(VAR_ID) %>%
    mutate(VAR_N = 1:n()) %>%
    ungroup

non_crane_key_df <- r1_key_df %>%
    filter(TEAM != "crane") %>%
    left_join(id_offset_df2) %>%
    mutate(VAR_N = VAR_ID - diff) %>%
    select(-diff)

combined_df2 <-
    bind_rows(crane_key_df, non_crane_key_df) %>%
    inner_join(r1_vcf_df) %>%
    select(-VAR_N)

r1_variant_df <- "r1_VAR_ID.tsv" %>%
    fread %>%
    as_tibble %>%
    mutate(TEAM = str_match(PREDICTION_ID, "^([:alnum:]+)_[:print:]+$")[,2]) %>%
    mutate(PATIENT = str_match(PREDICTION_ID, "^[:alnum:]+_([:alnum:]+_[:alnum:]+)_[:print:]+$")[,2]) %>% 
    inner_join(combined_df2)

r1_orphan_table <- r1_vcf_df %>% 
    filter(!VARIANT_ID %in% r1_variant_df$VARIANT_ID)

write_tsv(r1_variant_df, "r1_VARIANT.tsv")
write_tsv(r1_orphan_table, "r1_ORPHAN_VARIANT.tsv")


