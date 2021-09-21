rm(list=ls())
library(tidycensus)
library(tidyverse)
library(stringr)
library(optparse)
library(googledrive)
library(foreach)

geographies <- c('state', 'county', 'county subdivision', 'tract', 'block group', 
                 'block', 'place', 'metropolitan statistical area/micropolitan statistical area', 'combined statistical area',
                 'urban area', 'school district (elementary)', 'school district (secondary)', 'school district (unified)',
                 'zcta'
)

evenness <- function(prop_list){
  
  # encounter probability based on relative abundance
  pr_encounter <- function(p) p*log(p)
  
  # entropy
  shannon_h <- -1*sum(sapply(prop_list, pr_encounter))
  
  # maximum entropy possible given diversity
  h_max <- -1*sum(sapply(rep(1/length(prop_list), length(prop_list)), pr_encounter))
  
  return(shannon_h/h_max)
}

load_all_variables <- function(year){
  
  # get all the variable data
  basic <- load_variables(year, 'acs5', cache=TRUE)
  basic$table <- 'basic'
  table_name <- sapply(basic$name, strsplit, split="_")
  
  data_profiles <- load_variables(year, 'acs5/profile', cache=TRUE)
  data_profiles$table <- 'data_profiles'
  
  survey <- load_variables(year, 'acs5/subject', cache=TRUE)
  survey$table <- 'survey'
  
  all <- rbind(basic, data_profiles)
  all <- rbind(all, survey)
  
  # remove extraneous colons in the label field
  all$label <- sapply(all$label, gsub, pattern=':', replacement='')
  
  # separate aggregate columns
  #all <- separate(all, label, into=c('l1', 'l2', 'l3', 'l4', 'l5', 'l6', 'l7', 'l8'), sep='!!', remove=FALSE)
  all <- separate(all, name, into=c('table_name'), sep='_', remove=FALSE)
  all$year <- year
  
  # update the names
  #new_names <- c()
  #for(n in 1:length(names(all))){
  #  new_names <- c(new_names, paste(names(all)[n], as.character(year), sep='_'))
  #}
  #names(all) <- new_names
  return(all)
}

manual_inspect <- function(var_name){
  # use data frames in global namespace
  # semi-automate the inspection process
  
  print(c('2019', (v19 %>% filter(name == var_name))$label, (v19 %>% filter(name == var_name))$concept))
  print(c('2018', (v18 %>% filter(name == var_name))$label, (v18 %>% filter(name == var_name))$concept))
  print(c('2017', (v17 %>% filter(name == var_name))$label, (v17 %>% filter(name == var_name))$concept))
  print(c('2016', (v16 %>% filter(name == var_name))$label, (v16 %>% filter(name == var_name))$concept))
  print(c('2015', (v15 %>% filter(name == var_name))$label, (v15 %>% filter(name == var_name))$concept))
  print(c('2014', (v14 %>% filter(name == var_name))$label, (v14 %>% filter(name == var_name))$concept))
  print(c('2013', (v13 %>% filter(name == var_name))$label, (v13 %>% filter(name == var_name))$concept))
  print(c('2012', (v12 %>% filter(name == var_name))$label, (v12 %>% filter(name == var_name))$concept))
  print(c('2011', (v11 %>% filter(name == var_name))$label, (v11 %>% filter(name == var_name))$concept))
  print(c('2010', (v10 %>% filter(name == var_name))$label, (v10 %>% filter(name == var_name))$concept))
  
}

v19 <- load_all_variables(2019)
v18 <- load_all_variables(2018)
v17 <- load_all_variables(2017)
v16 <- load_all_variables(2016)
v15 <- load_all_variables(2015)
v14 <- load_all_variables(2014)
v13 <- load_all_variables(2013)
v12 <- load_all_variables(2012)
v11 <- load_all_variables(2011)
v10 <- load_all_variables(2010)


#####################################################
## Opportunity Atlas                               ##
#####################################################

# 1. Job Density SAVE FOR LATER DATA COLLECTION PHASE

# 2. Job Growth Rate SAVE FOR LATER DATA COLLECTION PHASE

# 3. High Paying Jobs SAVE FOR LATER DATA COLLECTION PHASE

# 4. Commute Time
v19 %>% filter(grepl("TRAVEL TIME"), concept)
v19 %>% filter(table_name=='B08303')

manual_inspect('B08303_001') # total pop in TRAVEL TIME TO WORK concept
manual_inspect('B08303_002') # > 5 min
manual_inspect('B08303_003') # 5 - 9 min
manual_inspect('B08303_004') # 10 - 14 min
manual_inspect('B08303_005') # 15 - 19 min
manual_inspect('B08303_006') # 20 - 24 min
manual_inspect('B08303_007') # 25 - 29 min
manual_inspect('B08303_008') # 30 - 34 mintues
manual_inspect('B08303_009') # 35 - 39 minutes
manual_inspect('B08303_010') # 40 - 44 minutes
manual_inspect('B08303_011') # 45 - 59 minutes
manual_inspect('B08303_012') # 60 - 89 minutes
manual_inspect('B08303_013') # 90+ minutes

commute_vars <- c(
  'B08303_001', 'B08303_002', 'B08303_003', 'B08303_004', 'B08303_005', 'B08303_006',
  'B08303_007', 'B08303_008', 'B08303_009', 'B08303_010', 'B08303_011', 'B08303_012', 'B08303_013'
)
commute_var_names <- c(
  'TOTAL_COMMUTE_POP', 'E_5LESS_MIN', 'E_5_9_MIN', 'E_10_14_MIN', 'E_15_19_MIN', 'E_20_24_MIN', 'E_25_29_MIN',
  'E_30_34_MIN', 'E_35_39_MIN', 'E_40_44_MIN', 'E_45_59_MIN', 'E_60_89_MIN', 'E_90PLUS_MIN'
)

all_commute = NULL
for(year in 2010:2019){
  for(geo in geographies){
    commute_data = tibble(
      out_table_field = commute_var_names,
      year = year,
      table_fields = commute_vars,
      table = 'B08303',
      geography = geo,
      state = 'TX',
      variable_type = 'estimate',
      depends_on_var = NA,
      census_product = 'acs5'
    )
    geo_name = gsub(' ', '_', geo)
    geo_name = gsub('/', '_', geo_name)
    fname = paste('~/CooksProTX/commute', year, geo_name, 'vars_may2021.csv', sep='_')
    write.csv(commute_data, fname)
  }
}

write.csv(all_commute, '~/CooksProTX/commute_vars_may2021.csv')


# 5. Renter households

# total population renting (not what we want...)
rental_pop <- v19 %>% filter(grepl('B25033', name)) %>%
  filter(label=='Estimate!!Total' | label=='Estimate!!Total!!Owner occupied' | label=='Estimate!!Total!!Renter occupied')

for(n in rental_pop$name){
  manual_inspect(n) # same for all years (should we later want it)
}

# total number of renter-occupied units
rental_units <- (v19 %>% filter(grepl('B25106_024', name)))
manual_inspect(rental_units$name) # same for all years

# total number of housing units
housing_units <- (v19 %>% filter(grepl('B25106_001', name)))
manual_inspect(housing_units$name) # same for all years

# pct renter occupied units = B25106_024/B25106_001


# 6. Rent Price

# as pct of income
median_rent <- v19 %>% filter(grepl('B25071', name))
manual_inspect(median_rent$name) # same over all years

# median gross rent for 2 bedroom
v19 %>% filter(name=='B25031_004')
manual_inspect('B25031_004') #not available before 2015

# 7. Racial Shares

demographics <- v19 %>% 
  filter(grepl('SEX BY AGE', concept)) %>% 
  filter(label == 'Estimate!!Total') %>%
  filter(str_detect(concept, "^SEX BY AGE \\(") | concept=='SEX BY AGE')
variable_codes <- demographics$name

for(n in variable_codes){
  manual_inspect(n)
} # same over all years

# percentages = B01001[letter]_001 / B01001_001

# 8. Foreign-Born

foreign_born <- v19 %>% 
  filter(concept=='SEX BY AGE FOR THE FOREIGN-BORN POPULATION') %>% 
  filter(label=='Estimate!!Total')
# there are two tables with info on foreign-born populations; they differ only by the age bins used

fb_var <- foreign_born$name[1]
manual_inspect(fb_var) # missing before 2014

v10 %>% filter(concept == 'SEX BY AGE FOR THE FOREIGN-BORN POPULATION' )
birthplace <- v10 %>% 
  filter(grepl("PLACE OF BIRTH", concept)) %>% 
  filter(label=="Estimate!!Total" | label=='Estimate!!Total!!Foreign born') %>%
  filter(concept=='PLACE OF BIRTH BY CITIZENSHIP STATUS')

for(n in birthplace$name){
  manual_inspect(n)
} # this variation has all years represented

#### Assemble the variables

# Since I already obtained the commute time data, no need to re-collect
# all remaining opportunity atlas variables will be pulled separately
# will call these "additional census variables" as I'm not 100% sure
# exactly what census data the opportunity atlas actually used (they don't provide var names)

other_census_names <- c()
other_census_vars <- c()
other_census_depends <- c() # reference name
other_census_var_type <- c()

# racial shares (first element is total)
other_census_names <- c(other_census_names, unname(demographics$concept))
other_census_vars <- c(other_census_vars, demographics$name)
other_census_depends <- c(other_census_depends, rep(NA, length(demographics$name)))
other_census_var_type <- c(other_census_var_type, rep('estimate', length(demographics$name)))

# racial shares percents
other_census_names <- c(other_census_names, sapply(unname(demographics$concept)[2:length(demographics$concept)], paste, 'PERCENT', ' '))
other_census_vars <- c(other_census_vars, rep(unname(demographics$name[1]), length(demographics$name[2:length(demographics$name)])))
other_census_depends <- c(other_census_depends, unname(demographics$concept[2:length(demographics$concept)]))
other_census_var_type <- c(other_census_var_type, rep('percent', length(demographics$name[2:length(demographics$name)])))

# foreign born
other_census_names <- c(other_census_names, paste(unname(birthplace$label[1]), 'by place of birth', sep=' '))
other_census_vars <- c(other_census_vars, birthplace$name[1])
other_census_depends <- c(other_census_depends, NA)
other_census_var_type <- c(other_census_var_type, 'estimate')

other_census_names <- c(other_census_names, unname(birthplace$label[2]))
other_census_vars <- c(other_census_vars, birthplace$name[2])
other_census_depends <- c(other_census_depends, NA)
other_census_var_type <- c(other_census_var_type, 'estimate')

# foreign born pct (collect denominator and reference previously collected numerator)
other_census_names <- c(other_census_names, paste(unname(birthplace$label[2]), 'PERCENT', sep=' '))
other_census_vars <- c(other_census_vars, birthplace$name[1]) # collect total
other_census_depends <- c(other_census_depends, unname(birthplace$label[2])) # depends on n foreign born
other_census_var_type <- c(other_census_var_type, 'percent')

# median rent (percent of gross income)
other_census_names <- c(other_census_names, unname(median_rent$label))
other_census_vars <- c(other_census_vars, median_rent$name)
other_census_depends <- c(other_census_depends, rep(NA, length(median_rent$name)))
other_census_var_type <- c(other_census_var_type, rep('estimate', length(median_rent$name)))

# rental units
other_census_names <- c(other_census_names, unname(rental_units$label))
other_census_vars <- c(other_census_vars, rental_units$name)
other_census_depends <- c(other_census_depends, rep(NA, length(rental_units$name)))
other_census_var_type <- c(other_census_var_type, rep('estimate', length(rental_units$name)))

# total housing units
other_census_names <- c(other_census_names, paste(unname(housing_units$label), 'Housing units', sep='!!'))
other_census_vars <- c(other_census_vars, housing_units$name)
other_census_depends <- c(other_census_depends, rep(NA, length(housing_units$name)))
other_census_var_type <- c(other_census_var_type, rep('estimate', length(housing_units$name)))

# pct rental units
other_census_names <- c(other_census_names, paste(unname(rental_units$label), 'PERCENT', sep=' '))
other_census_vars <- c(other_census_vars, housing_units$name) # collect total
#other_census_vars <- c(other_census_vars, rental_units$name) # collect total
other_census_depends <- c(other_census_depends, unname(rental_units$label)) # depends on n rental units
other_census_var_type <- c(other_census_var_type, 'percent')

census_names <- gsub('SEX BY AGE', 'Estimate, population', unname(other_census_names))
census_depends <- gsub('SEX BY AGE', 'Estimate, population', unname(other_census_depends))


all_other_census = NULL
for(year in 2010:2019){
  for(geo in geographies){
    opportunity_data = tibble(
      out_table_field = census_names,
      year = year,
      table_fields = other_census_vars,
      geography = geo,
      state = 'TX',
      variable_type = other_census_var_type,
      depends_on_var = census_depends,
      census_product = 'acs5'
    )
    geo_name = gsub(' ', '_', geo)
    geo_name = gsub('/', '_', geo_name)
    fname = paste('~/CooksProTX/other_census/', year, geo_name, 'other_census_vars_may2021.csv', sep='_')
    write.csv(opportunity_data, fname)
  }
}

write.csv(all_other_census, '~/CooksProTX/all_other_census_vars_commute_vars_may2021.csv')

#####################################################
## mobility related variables                      ##
#####################################################

# in table DP02
# mobility: Estimate!!RESIDENCE 1 YEAR AGO!!Population 1 year and over!!Different house in the U.S. (also differences by county and state, but not by other areas of geography)
# mobility: Estimate!!RESIDENCE 1 YEAR AGO!!Population 1 year and over - Estimate!!RESIDENCE 1 YEAR AGO!!Population 1 year and over!!Abroad = total in US
# see also https://walker-data.com/tidycensus/articles/other-datasets.html#migration-flows-1

#####################################################
## housing related variables                       ##
#####################################################

housing_variables <- v19 %>% filter(grepl("HOUSING", concept))
housing_concepts <- unique((v19 %>% filter(grepl("HOUSING", concept)))$concept)

write.csv(housing_variables, '~/CooksProTX/2019_ACS_housing_variables.csv')

