# munge the crosswalk into the input form for census_aggregato.r

library(tidyverse)
library(googledrive)
cw <- read.csv('~/CooksProTX/census/manual_svi_crosswalk_feb2021.csv')

# where was tidyverse when I was in grad school??? how was this so easy???
cw_grouped <- cw %>% group_by(svi_name) %>% summarize(
  calc_2019=paste(name_2019, collapse=','),
  calc_2018=paste(name_2018, collapse=','),
  calc_2017=paste(name_2017, collapse=','),
  calc_2016=paste(name_2016, collapse=','),
  calc_2015=paste(name_2015, collapse=','),
  calc_2014=paste(name_2014, collapse=','),
  calc_2013=paste(name_2013, collapse=','),
  calc_2012=paste(name_2012, collapse=','),
  calc_2011=paste(name_2011, collapse=','),
  calc_2010=paste(name_2010, collapse=','),
  )

cw_long <- pivot_longer(cw_grouped, cols=names(cw_grouped)[3:12])
cw_long <- separate(cw_long, col=name, into=c('calc', 'year'), sep='_')
cw_long <- cw_long[, !names(cw_long) %in% c('calc')]
cw_long <- separate(cw_long, col=value, into=c('table'), remove=FALSE)
cw_long$geography <- 'tract'
cw_long$state <- 'TX'
names(cw_long) <- c('out_table_field', 'year', 'table_fields', 'table', 'geography', 'state')
cw_long$variable_type <- 'estimate' # this will get the sum for everything
cw_long$depends_on_var <- NA

# special cases
cw_long_df <- as.data.frame(cw_long) # get a mutable datatype
cw_long_df$out_table_field <- as.character(cw_long_df$out_table_field)
for(i in 1:length(cw_long_df$out_table_field)){
  #print(cw_long_df$out_table_field[i])
  if(grepl('EP_CROWD', cw_long_df$out_table_field[i])){
    cw_long_df$variable_type[i] <- 'percent'
    cw_long_df$depends_on_var[i] <- 'E_CROWD'
    cw_long_df$out_table_field[i] <- 'EP_CROWD'
  }else if(grepl('EP_LIMENG', cw_long_df$out_table_field[i])){
    cw_long_df$variable_type[i] <- 'percent'
    cw_long_df$depends_on_var[i] <- 'E_LIMENG'
    cw_long_df$out_table_field[i] <- 'EP_LIMENG'
  }else if(grepl('E_MINRTY', cw_long_df$out_table_field[i])){
    cw_long_df$variable_type[i] <- 'difference'
    cw_long_df$depends_on_var[i] <- 'E_TOTPOP'
    cw_long_df$out_table_field[i] <- 'E_MINRTY'
  }
}

cw_19 <- cw_long_df %>% filter(year==2019)
write.csv(cw_19, '~/CooksProTX/census/manual_2019_vars.csv')
drive_upload(
  media='~/CooksProTX/census/manual_2019_vars.csv',
  path='Cooks_ProTX/manual_2019_vars.csv',
  type='spreadsheet', # this will ensure it's a google sheet
  overwrite=FALSE # don't overwrite the original!
)

upload_years <- function(year_){
  cw_year <- cw_long_df %>% filter(year==year_)
  local_path <- paste('~/CooksProTX/census/manual_', year_, '_vars_feb2021.csv', sep='')
  remote_path <- paste('Cooks_ProTX/manual_', year_, '_vars_feb2021.csv', sep='')
  write.csv(cw_year, local_path)
  drive_upload(
    media=local_path,
    path=remote_path,
    type='spreadsheet', # this will ensure it's a google sheet
    overwrite=FALSE # don't overwrite the original!
  )
}

upload_years(2018)
upload_years(2017)
upload_years(2016)
upload_years(2015)
upload_years(2014)
upload_years(2013)
upload_years(2012)
upload_years(2011)
upload_years(2010)
