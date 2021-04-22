rm(list=ls())
library(tidyverse)
library(tidycensus)
library(sf)
load('/Users/kpierce/uscb_query_env.RData')

#API_KEY <- Sys.getenv("USCB_API_KEY")
API_KEY <- Sys.getenv("CENSUS_API_KEY")
census_api_key(API_KEY)#, install=TRUE, overwrite=TRUE)

v19 <- load_variables(2019, dataset='acs5/subject')

ages <- v19 %>% filter(concept=='AGE AND SEX')

age_vars = c(
  '<5'='S0101_C01_002',
  '5-14'='S0101_C01_020',
  '15-17'='S0101_C01_021',
  '18-24'='S0101_C01_023',
  '25-29'='S0101_C01_007',
  '30-34'='S0101_C01_008',
  '35-39'='S0101_C01_009',
  '40-44'='S0101_C01_010',
  '45-49'='S0101_C01_011',
  '50-54'='S0101_C01_012',
  '55-59'='S0101_C01_013',
  '60-64'='S0101_C01_014',
  '65+'='S0101_C01_030'
)

pop2019 <- get_acs(
  geography='zcta',
  variables=age_vars,
  state='TX',
  year=2019,
  geometry=FALSE,
  survey='acs5',
  show_call=TRUE
)

add_grp <- function(age_bin){
  vars_to_groups = c(
    '<5'='<5',
    '5-14'='5-17',
    '15-17'='5-17',
    '18-24'='18-49',
    '25-29'='18-49',
    '30-34'='18-49',
    '35-39'='18-49',
    '40-44'='18-49',
    '45-49'='18-49',
    '50-54'='50-64',
    '55-59'='50-64',
    '60-64'='50-64',
    '65+'='65+'
  )
  return(unname(vars_to_groups[age_bin]))
}

pop2019$age_bin <- sapply(pop2019$variable, add_grp)

grouped_pop2019 <- pop2019 %>% group_by(GEOID, NAME, age_bin) %>% summarize(group_pop = sum(estimate))

write.csv(grouped_pop2019, '~/COVID19/austin-spatial-prev/2019_zcta_pop_5_age_groups.csv')
