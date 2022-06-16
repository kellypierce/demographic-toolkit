rm(list=ls())
library(tidycensus)
library(tidyverse)
library(stringr)
library(optparse)
library(googledrive)

option_list = list(
  make_option(
    c("-k", "--key", type="character", default=NULL, help="Path to text file containing API key", metavar="character")
  ),
  make_option(
    c("-s", "--sheet", type="character", default=NULL, help="Path to google sheet containing variables to pull", metavar="character")
  ),
  make_option(
    c("-o", "--outpath", type="character", default=NULL, help="Path to save outputs", metavar="character")
  )
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
print(opt)

###########################################
# Set variables                           #
###########################################

if(!interactive()){
  API_KEY <- read.table(opt$key, header=TRUE, colClasses='character')
  SHEET_NAME <- opt$sheet
  OUT_PATH <- opt$outpath
}else{
  API_KEY = readline("Please enter your US Census Bureau API key. ")
  SHEET_NAME <- readline("Please enter the name of the Google Sheet containing US Census variables to collect. ")
  OUT_PATH <- readline("Please enter the path for saving collected US Census variables. ")
}

############################################
# Define helper functions                  #
############################################

required_fields_validation <- function(dataframe, column){
  
  if(any(is.na(dataframe[[column]]))){
    return(1)
  }
}

column_level_validation <- function(dataframe, column, supported){
  
  current_levels <- setdiff(unique(dataframe[[column]]), supported)
  if(length(current_levels > 0)){
    print(paste('Unsupported value(s)', current_levels, 'identified in column', column, sep=' '))
    return(1)
  }else{return(0)}
  
}

contingent_column_validation <- function(dataframe, col1, col2){
  
  # validate inputs where col1 must be specified if col2 is specified
  error = 0
  for(row in 1:dim(dataframe)[1]){
    if(!is.na(dataframe[[col2]][row])){
      if(is.na(dataframe[[col1]][row])){
        print(paste(col1, 'must be specified if', col2, 'is provided for row', row, '.', sep=' '))
        error = 1
      }
    }
  }
  return(error)
}

contingent_value_validation <- function(dataframe, col1, val1, col2, val2){
  
  return('Not Implemented')
  # validate inputs where col1 must be specified if col2 is specified
  # validate inputs where col1 and col2 cannot both be null
  
  error = 0
  for(row in 1:dim(dataframe)[1]){
    # initial check that row isn't missing both state and county
    if(is.na(dataframe[[col1]][row])){
      if(is.na(dataframe[[col2]][row])){
        print(paste('Must specify', col1, 'or a combination of', col1, 'and', col2, 'for row', row, 'in the input data sheet.', sep=' '))
        error = 1
      }
      # if col2 is specified, col1 must be specified as well
    }else if(!is.na(dataframe[[col2]][row])){
      if(is.na(dataframe[[col1]][row])){
        print(paste(col1, 'must be specified if', col2, 'is provided for row', row, '.', sep=' '))
        error = 1
      }
    }
  }
  return(error)
}

get_acs_wrapper <- function(dataset){
  
  derived_datasets <- NULL
  
  for(row in 1:dim(dataset)[1]){
    
    # remove all whitespace with a regular expression
    cleaned <- gsub("\\s", "", dataset$table_fields[row], fixed=FALSE)
    
    # turn the string into a list split on commas
    var_set <- unlist(strsplit(cleaned, ','))
    
    # determine if specifying county or setting to NULL
    if('county' %in% names(dataset)){
      if(is.na(dataset$county[row])){
        county_name=NULL
      }else{
        county_name=dataset$county[row]
      }
    }else{
      county_name=NULL
    }
    
    #### make the call to the ACS API
    
    # these geographies must be retrieved for whole country
    national_geographies <- c('urban area', 'school district (unified)',
                              'metropolitan statistical area/micropolitan statistical area', # this means cbsa
                              'state legislative district (upper chamber)',
                              'state legislative district (lower chamber)')
    
    # as of 5/2021, zcta is now organized by state and retrieved at the state level FOR 2019 ONLY
    if(as.numeric(as.character(dataset$year[row])) != 2019){
      national_geographies <- c(national_geographies, 'zcta')
    }
    
    if(dataset$geography[row] %in% national_geographies){
      acs_data <- get_acs(geography=dataset$geography[row], year=dataset$year[row],
                          variables=var_set, survey="acs5", show_call=TRUE)
    }else{ # other geographies allow states
      acs_data <- get_acs(geography=dataset$geography[row], year=dataset$year[row], state=dataset$state[row], county=county_name,
                          variables=var_set, survey="acs5", show_call=TRUE)}
    
    acs_data$variable_type <- dataset$variable_type[row]
    acs_data$out_table_field <- dataset$out_table_field[row]
    acs_data$agg_fxn <- dataset$agg_fxn[row]
    acs_data$geotype <- dataset$geography[row]
    acs_data$year <- dataset$year[row]
    
    # add to overall data output
    derived_datasets <- rbind(derived_datasets, acs_data)
  }
  return(derived_datasets)
}

agg_moe <- function(moe_data){
  
  square <- function(x){
    return(x*x)
  }
  return(sqrt(sum(sapply(moe_data, square))))
  
}

############################################
# Set API key for US Census Bureau         #
############################################

census_api_key(API_KEY)

############################################
# Get the Google Sheet                     #
############################################

# download a local copy to the current working directory; tab-separated
print(paste("Downloading sheet name", SHEET_NAME, sep=' '))
local_file <- drive_download(SHEET_NAME, type='tsv', overwrite=TRUE)

# set column names and classes for validation
# keep almost all columns as character columns to avoid factor-as-integer surprises
column_classes <- c('character', 'character', 'integer', 'character', 'character', 'character', 'character', 'character')
names(column_classes) <- c('table', 'table_fields', 'year', 'geography', 'state', 'county', 'variable_type', 'out_table_field')

# read the local copy
ACS_VARS <- read.delim(local_file$local_path, sep='\t', header=TRUE,
                       colClasses = column_classes, na.strings="")
ACS_VARS <- ACS_VARS[rowSums(is.na(ACS_VARS)) != ncol(ACS_VARS),] # rm rows that are completely NA

############################################
# Input validation                         #
############################################

REQUIRED_COLUMNS <- c('table', 'table_fields', 'year', 'geography', 'state', 'out_table_field')
SUPPORTED_GEOGRAPHIES <-  c('state', 'county', 'county subdivision', 'tract', 'block group', 
                            'block', 'place', 'metropolitan statistical area/micropolitan statistical area', 'combined statistical area',
                            'urban area', 'school district (elementary)', 'school district (secondary)', 'school district (unified)',
                            'zcta')
SUPPORTED_YEARS <- 2010:2020
SUPPORTED_STATES <- state.abb # r built-in list
SUPPORTED_VAR_TYPE <- c('estimate', 'moe', 'difference', 'percent', 'custom')

ERRORS = 0
ERRORS = ERRORS + sum(unlist(sapply(REQUIRED_COLUMNS, required_fields_validation, dataframe=ACS_VARS)))
ERRORS = ERRORS + column_level_validation(dataframe=ACS_VARS, column='geography', supported=SUPPORTED_GEOGRAPHIES)
ERRORS = ERRORS + column_level_validation(dataframe=ACS_VARS, column='year', supported=SUPPORTED_YEARS)
ERRORS = ERRORS + column_level_validation(dataframe=ACS_VARS, column='state', supported=SUPPORTED_STATES)
ERRORS = ERRORS + column_level_validation(dataframe=ACS_VARS, column='variable_type', supported=SUPPORTED_VAR_TYPE)

# if specifying county, state must also be specified
#ERRORS = ERRORS + contingent_column_validation(dataframe=ACS_VARS, col1='state', col2='county')


if(ERRORS > 0){
  stop(paste('Identified', ERRORS, 'validation error(s); see printed messages above.', sep=' '))
}

############################################
# Finalized data list for subsequent join  #
############################################

final_datasets <- list()
list_idx = 1

############################################
# SVI Variables Directly from ACS          #
############################################

direct <- ACS_VARS %>% filter(variable_type != 'custom')

if(dim(direct)[1] > 0){
  print('Processing direct variables.')
  direct_data <- get_acs_wrapper(direct)
  
  # only calculate sqrt(sse) for variables requiring margin of error calc
  if('moe' %in% unique(direct_data$variable_type)){
    direct_moe <- direct_data %>% filter(variable_type == 'moe') %>% group_by(GEOID, out_table_field, geotype, year) %>% summarize(agg_moe = agg_moe(moe))
    direct_moe <- direct_moe %>% pivot_wider(id_cols=c('GEOID', 'geotype', 'year'), names_from='out_table_field', values_from='agg_moe')
    final_datasets[[list_idx]] = direct_moe
    list_idx = list_idx + 1
  }
  
  # only calculate sum(est) for variables requiring sum calc
  if('estimate' %in% unique(direct_data$variable_type)){
    direct_est <- direct_data %>% filter(variable_type == 'estimate') %>% group_by(GEOID, out_table_field, geotype, year) %>% summarize(agg_est = sum(estimate))
    direct_est_wide <- direct_est %>% pivot_wider(id_cols=c('GEOID', 'geotype', 'year'), names_from='out_table_field', values_from='agg_est')
    final_datasets[[list_idx]] = direct_est_wide
    list_idx = list_idx + 1
  }
}



############################################
# SVI Variables Derived from ACS           #
############################################

vtypes <- unique(ACS_VARS$variable_type)

derived <- ACS_VARS %>% filter(variable_type == 'percent' | variable_type == 'difference')
if(dim(derived)[1] > 0){
  print('Processing derived variables.')
  derived_data <- get_acs_wrapper(derived)
  
  # calculate the percentages
  if('percent' %in% unique(derived_data$variable_type)){
    print('Calculating percentages.')
    derived_pct <- derived %>% filter(variable_type == 'percent')
    derived_est_pct <- derived_data %>% filter(variable_type == 'percent') %>% group_by(GEOID, out_table_field, geotype, year) %>% summarize(agg_est = sum(estimate))
    pct_depends <- left_join(derived_est_pct, derived_pct, by=c('out_table_field'='out_table_field', 'year'='year', 'geotype'='geography'))
    # currently supported percentage cases collect the numerator as a direct variable and the denominator dependent on numerator collection
    # clearly that workflow needs some work to be more intuitive...
    pct_all <- left_join(pct_depends, direct_est, by=c('depends_on_var'='out_table_field', 'GEOID'='GEOID', 'year'='year', 'geotype'='geotype'), suffix=c('_denominator', '_numerator'))
    pct_all$agg_est <- (pct_all$agg_est_numerator / pct_all$agg_est_denominator) * 100
    pct_all <- pct_all[, names(pct_all) %in% c('GEOID', 'out_table_field', 'agg_est', 'geotype', 'year')]
    pct_all_wide <- pct_all %>% pivot_wider(id_cols=c('GEOID', 'geotype', 'year'), names_from='out_table_field', values_from='agg_est')
    final_datasets[[list_idx]] = pct_all_wide
    list_idx = list_idx + 1
  }
  
  # calculate the differences
  if('difference' %in% unique(derived_data$variable_type)){
    print('Calculating differences.')
    derived_diff <- derived %>% filter(variable_type == 'difference')
    derived_est_diff <- derived_data %>% filter(variable_type == 'difference') %>% group_by(GEOID, out_table_field, geotype, year) %>% summarize(agg_est = sum(estimate))
    diff_depends <- left_join(derived_est_diff, derived_diff, by=c('out_table_field'='out_table_field', 'year'='year', 'geotype'='geography'))
    # currently supported percentage cases collect the larger value as a direct variable and the smaller value dependent on larger value collection
    # clearly that workflow needs some work to be more intuitive...
    diff_all <- left_join(diff_depends, direct_est, by=c('depends_on_var'='out_table_field', 'GEOID'='GEOID', 'year'='year', 'geotype'='geotype'), suffix=c('_subset', '_total'))
    diff_all$agg_est <- diff_all$agg_est_total - diff_all$agg_est_subset
    diff_all <- diff_all[, names(diff_all) %in% c('GEOID', 'out_table_field', 'agg_est', 'year', 'geotype')]
    diff_all_wide <- diff_all %>% pivot_wider(id_cols=c('GEOID', 'year', 'geotype'), names_from='out_table_field', values_from='agg_est')
    final_datasets[[list_idx]] = diff_all_wide
    list_idx = list_idx + 1
  }
}

custom <- ACS_VARS %>% filter(variable_type == 'custom')
print(
  paste('Support for custom variable definitions not yet implemented. Unable to collect variables', unique(custom$out_table_field), sep=' ')
)

############################################
# Finalize SVI Output                      #
############################################

base_data <- final_datasets[[1]]

if(length(final_datasets) > 1){
  for(i in 2:length(final_datasets)){
    base_data <- full_join(base_data, final_datasets[[i]], by=c('GEOID'='GEOID', 'geotype'='geotype', 'year'='year'))
  }
}

sheet_base <- str_split(SHEET_NAME, '/')[[1]]
sheet_base_name <- sheet_base[length(sheet_base)]
query_timestamp <- gsub('-', '', gsub(':', '', gsub('\\s', '', Sys.time()), fixed=TRUE), fixed=TRUE)
final_path <- paste(OUT_PATH, '/', sheet_base_name, '_', query_timestamp, '.csv', sep='')
write.csv(base_data, final_path, row.names=FALSE)

############################################
# Clean up                                 #
############################################

# todo: delete local copy of google sheet
