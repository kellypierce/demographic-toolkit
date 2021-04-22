rm(list=ls())
library(tidyverse)
library(tidycensus)
library(sf)
load('/Users/kpierce/uscb_query_env.RData')

#API_KEY <- Sys.getenv("USCB_API_KEY")
API_KEY <- Sys.getenv("CENSUS_API_KEY")
census_api_key(API_KEY)#, install=TRUE, overwrite=TRUE)

v19 <- load_variables(2019, dataset='acs5/subject')
# head(v19) shows S0101_C01_001 as total population

v19_detailed <- load_variables(2019, dataset='acs5')

geographies <- c(
  'zip code tabulation area',
  'urban area',
  'congressional district',
  #'school district (unified)',
  #'metropolitan statistical area/micropolitan statistical area', # cbsa
  #'state legislative district (upper chamber)',
  #'state legislative district (lower chamber)',
  'county'
) # intentionally excluding "tract"; see below

for(geo in geographies){
  for(year in 2010:2019){
    if(year==2010){
      yearly_pop <- get_decennial(
        geography = geo,
        variables='P012001',
        year=year,
        geometry=FALSE,
        show_call=TRUE
      )
    }else{
      yearly_pop <- get_acs(
        geography=geo,
        variables='S0601_C01_001',
        year=year,
        geometry=FALSE,
        survey='acs5',
        show_call=TRUE
      )
    }
    # drop variable column
    fname <- paste('/Users/kpierce/CooksProTX/census/total_population_sizes/total_pop_', geo, '_', year, '.csv', sep='')
    write.csv(yearly_pop, fname)
  }
}

# state required for tract data
state_geographies <- c(
  'tract'
)
for(geo in state_geographies){
  for(year in 2010:2019){
    if(year==2010){
      yearly_pop <- get_decennial(
        geography = geo,
        state='TX',
        variables='P012001',
        year=year,
        geometry=FALSE,
        show_call=TRUE
      )
    }else{
      yearly_pop <- get_acs(
        geography = geo,
        state='TX',
        variables='S0601_C01_001',
        year=year,
        geometry=FALSE,
        survey='acs5',
        show_call=TRUE
      )
    }
    # drop variable column
    fname <- paste('/Users/kpierce/CooksProTX/census/total_population_sizes/total_pop_', geo, '_', year, '.csv', sep='')
    write.csv(yearly_pop, fname)
  }
}


##### USING tigris #####
rm(list=ls())
library(tigris)
library(sf)

## zip code tabulation areas
zcta2010 <- zctas(year=2010)
st_write(zcta2010, '/Users/kpierce/CooksProTX/spatial/tigris/usa_zcta/zcta_2010.shp')

zcta2011 <- zctas(year=2011) # no 2011 ZCTAs

for(year in 2012:2019){
  zcta <- zctas(year=year)
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/usa_zcta/zcta_', year, '.shp', sep='')
  st_write(zcta, fname)
  rm(zcta)
}

## tract # missing 2011?
for(year in 2012:2019){
  tract <- tracts(year=year, state='TX', refresh=TRUE)
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/texas_census_tracts/census_tracts_', year, '.shp', sep='')
  st_write(tract, fname)
  rm(tract)
}

## counties
for(year in 2010:2019){
  county <- counties(year=year)
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/usa_counties/counties_', year, '.shp', sep='')
  st_write(county, fname)
  rm(county)
}

## school districts
for(year in 2011:2019){
  school <- school_districts(year=year, state='TX')
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/texas_school_districts/school_districs_', year, '.shp', sep='')
  st_write(school, fname)
  rm(school)
}

## congressional districts
for(year in 2011:2019){
  congress <- congressional_districts(year=year)
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/usa_congressional_districts/congressional_districs_', year, '.shp', sep='')
  st_write(congress, fname)
  rm(congress)
}

## state leg districts
for(year in 2011:2019){
  stateleg <- state_legislative_districts(year=year, state='TX')
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/texas_legislative_districts/legislative_districs_', year, '.shp', sep='')
  st_write(stateleg, fname)
  rm(stateleg)
}

## core based statistical areas (metropolitan statistical areas and micropolitan statistical areas)
for(year in 2011:2019){
  cbsa <- core_based_statistical_areas(year=year)
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/usa_core_base_statistical_areas/cbsa_', year, '.shp', sep='')
  st_write(cbsa, fname)
  rm(cbsa)
}

## urban areas (not available for 2012)
for(year in 2012:2019){
  urban <- urban_areas(year=year, refresh=TRUE)
  fname <- paste('/Users/kpierce/CooksProTX/spatial/tigris/usa_urban_areas/urban_areas_', year, '.shp', sep='')
  st_write(urban, fname)
  rm(urban)
}

