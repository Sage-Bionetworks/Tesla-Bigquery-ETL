library(synapser)
library(tidyverse)
library(yaml)
library(rjson)
library(magrittr)

temp_dir      <- "./temp_dir/"
evaluation_id <- 9614042L

synLogin()
dir.create(temp_dir)

get_yaml_from_zip_file <- function(zip_path, temp_dir){
    system(str_c("unzip ", zip_path, " -d ", temp_dir))
    yaml_path <- str_c(temp_dir, "TESLA_YAML.yaml")
    yaml <- yaml::read_yaml(yaml_path)
    system(str_c("rm -rf ", temp_dir, "*"))
    return(yaml)
}





submissions <- evaluation_id %>%
    synapser::synGetSubmissions(status = "VALIDATED") %>% 
    as.list() 

team_ids <- submissions %>% 
    map_chr("teamId") %>% 
    as.integer()

file_names <- submissions %>%
    purrr::map_chr("id") %>%
    as.integer() %>% 
    purrr::map(synGetSubmission) %>% 
    purrr::map_chr("filePath")

yamls <- purrr::map(file_names, get_yaml_from_zip_file, temp_dir)

patient_ids <- file_names %>% 
    basename() %>% 
    str_remove_all(".zip") %>% 
    as.integer()

param_df <- data_frame(
    "team" = team_ids,
    "patient" = patient_ids,
    "yaml" = yamls
)


yml <- yamls[[1]]
jsn <- toJSON(yml)
write(jsn, "test.json")






# 
# 
# expand_named_list_column <- function(df, list_col){
#     dfa <- select(df, - list_col)
#     dfb <- df %>%
#         extract2(list_col) %>%
#         map(named_list_to_df) %>%
#         bind_rows
#     bind_cols(dfa, dfb)
# }
# 
# named_list_to_df <- function(lst){
#     lst %>% 
#         enframe %>% 
#         mutate(value = map_chr(.$value, str_c, collapse = ";")) %>% 
#         spread(key = "name", value = "value") %>% 
#         set_colnames(str_c("parameter_", colnames(.)))
# }
# 
# yaml_to_df <- function(yaml, patient, team){
#     
#     df <- yaml %>%
#         enframe() %>%
#         mutate(value = map(value, enframe)) %>%
#         unnest() %>%
#         set_colnames(c("Step", "Field", "Value")) %>% 
#         filter(!map_lgl(.$Value, is.null)) 
#     
#     dfa <- df %>% 
#         filter(Field != "key_parameters") %>% 
#         mutate_all("unlist") %>% 
#         spread(key = "Field", value = "Value") %>% 
#         mutate_at(vars(changed, used), as.logical) %>% 
#         mutate(team = team) %>% 
#         mutate(patient = patient) %>% 
#         select(team, patient, everything())
#     
#     dfb <- df %>%
#         filter(Field == "key_parameters") %>%
#         select(-Field) %>% 
#         filter(!map_lgl(.$Value, is.null))
#     
#     if(nrow(dfb) == 0) return(dfa)
#     
#     dfb <- dfb %>% 
#         unnest() %>% 
#         expand_named_list_column("Value")
#     
#     full_df <- 
#         full_join(dfa, dfb)
#        
# }
# 
# final_df <- pmap(param_df, yaml_to_df) %>% 
#     bind_rows() %>% 
#     filter(used | changed)
# 
# 
# dfa <- final_df %>% 
#     select(team, Step) %>% 
#     distinct() %>% 
#     group_by(team) %>%
#     summarise(count = n())
# 
# dfb <- final_df %>% 
#     select(-c(changed, used, patient)) %>% 
#     filter(!is.na(parameter_name))
#     
# 
# dfc <- final_df %>% 
#     select(team, Step) %>% 
#     distinct() %>% 
#     group_by(Step) %>%
#     summarise(count = n())
# 
# 
