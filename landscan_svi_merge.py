import pandas as pd
from spatialpandas.io import read_parquet
import numpy as np
import os
import sys
import xarray as xr
import sqlite3


def sanity_check(xr1, xr2, columns):

    for c in columns:
        np.testing.assert_almost_equal(
            np.nansum(xr1[c].values.flatten()),
            np.nansum(xr2[c].values.flatten()),
            decimal=3
        )


class AnnualSVI:

    def __init__(self, db_path, landscan_path, year, save_template):
        self.conn = sqlite3.connect(db_path)
        self.landscan = read_parquet(landscan_path)
        self.year = year
        self.save_template = save_template
        self.weighted_landscan = self.annual_landscan_weight()
        self.svi_wide, self.dollars_wide = self.annual_svi()
        self.weighted_svi, self.weighted_dollars = self.annual_svi_weighted_landscan()
        self.mapping = self.generate_mapping()

    def generate_mapping(self):

        units = pd.read_sql_query("""select * from display_data;""", self.conn)
        units_totals = {
            'persons commuting': 'TOTAL_COMMUTE_POP',
            'persons': 'TOTPOP',
            'housing units': 'HU',
            'housing structures': 'HU',
            'households': 'HH',
            'housholds': 'HH',
            'dollars': None,
            'children': None
        }
        mapping = {
            row['NAME']: units_totals[row['UNITS']] for i, row in units.iterrows() if
            units_totals[row['UNITS']] is not None
        }

        return mapping

    def annual_svi(self):

        svi_query = f"""select * from demographics d \
            join display_data dd on d.DEMOGRAPHICS_NAME = dd.NAME \
            where d.UNITS = 'count' and \
                d.YEAR = {self.year} and \
                d.GEOTYPE = 'tract' and \
                dd.UNITS in ('persons commuting', 'persons', 'households', 'housholds', 'housing units', 'housing structures')
            """

        dollars_query = f"""select * from demographics d \
            join display_data dd on d.DEMOGRAPHICS_NAME = dd.NAME \
            where d.YEAR = {self.year} and \
                d.GEOTYPE = 'tract' and \
                d.DEMOGRAPHICS_NAME in ('MEDIAN_GROSS_RENT_PCT_HH_INCOME', 'PCI')
            """

        svi = pd.read_sql_query(svi_query, self.conn)
        dollars = pd.read_sql_query(dollars_query, self.conn)

        svi_wide = pd.pivot(svi, index='GEOID', columns='DEMOGRAPHICS_NAME', values='VALUE').reset_index()
        dollars_wide = pd.pivot(dollars, index='GEOID', columns='DEMOGRAPHICS_NAME', values='VALUE').reset_index()

        return svi_wide, dollars_wide


    def annual_landscan_weight(self):

        # get total landscan population by tract (GEOID)
        rdf_grouped = self.landscan.groupby('GEOID').sum('average_population').reset_index()
        rdf_grouped = rdf_grouped[['GEOID', 'average_population']].rename(
            columns={'average_population': 'landscan_tract_total_pop'})

        # one-to-many join to set up weight calculation
        weighted_landscan = pd.merge(self.landscan, rdf_grouped, left_on='GEOID', right_on='GEOID', how='left')

        # perform weight calcuation
        weighted_landscan['landscan_weight'] = weighted_landscan['average_population'] / weighted_landscan['landscan_tract_total_pop']

        return weighted_landscan

    def annual_svi_weighted_landscan(self):

        self.weighted_landscan = self.weighted_landscan.set_index('GEOID')

        svi_wide_indexed = self.svi_wide.set_index('GEOID')
        dollars_wide_indexed = self.dollars_wide.set_index('GEOID')

        weighted_svi = pd.merge(self.weighted_landscan, svi_wide_indexed, left_index=True, right_index=True, how='left')

        # we don't need the weights for dollars, but we do the merge here for consistency of workflow
        # PCI and median hh rent as income pct are one-to-many joined with landscan;
        # landscan columns will be dropped and those variables will be averaged when xarray is coarsened
        weighted_dollars = pd.merge(self.weighted_landscan, dollars_wide_indexed, left_index=True, right_index=True, how='left')

        return weighted_svi, weighted_dollars

    def svi_xarray(self, svi_column, total_column):

        total = '{}_corrected'
        counts = '{}_LANDSCAN_COUNT'
        percents = '{}_LANDSCAN_PERCENT'

        # subset the data and calculate the landscan count estimate
        try:
            data_subset = self.weighted_svi[['x', 'y', 'landscan_weight', svi_column, total_column]].reset_index()
        except KeyError:
            print('Variable {} not in data for year {}'.format(svi_column, self.year))
            return
        data_subset[counts.format(svi_column)] = data_subset['landscan_weight'] * (data_subset[svi_column])
        data_subset[total.format(total_column)] = data_subset['landscan_weight'] * data_subset[total_column]

        # set index, drop unnecessary columns, and convert to xarray
        data_subset = data_subset.drop(['GEOID', 'landscan_weight', total_column], axis=1)
        data_subset = data_subset.set_index(['x', 'y'])
        data_subset_xr = data_subset.to_xarray()

        # coarsen from 3 to 30 arcsecond resolution
        data_subset_xr_30arcsec = data_subset_xr.coarsen(
            boundary='pad',
            x=10,
            y=10,
        ).sum().compute()

        # quick sanity check on total average and landscan estimate populations across resolutions
        sanity_check(data_subset_xr, data_subset_xr_30arcsec, columns=[svi_column, counts.format(svi_column)])

        # calculate landscan percent
        landscan_pct = data_subset_xr_30arcsec[counts.format(svi_column)] / data_subset_xr_30arcsec[total.format(total_column)]
        data_subset_xr_30arcsec[percents.format(svi_column)] = landscan_pct

        # mask the cells with values of "0"
        data_subset_xr_30arcsec = data_subset_xr_30arcsec.drop_vars([svi_column])
        data_subset_xr_30arcsec = data_subset_xr_30arcsec.where(data_subset_xr_30arcsec > 0)

        data_subset_xr_30arcsec.to_netcdf(path=self.save_template.format(svi_column, self.year))

        return

    def dollars_xarray(self, dollar_column):

        tract_dollars = self.weighted_dollars.set_index(['x', 'y'])[[dollar_column]]

        # converting the sparse dataframe will fill in the empty values with NaN
        # NaN values are ignored in the mean calculation
        tract_dollars_xr = tract_dollars.to_xarray()
        tract_dollars_xr_coarse = tract_dollars_xr.coarsen(
            boundary='pad',
            x=10,
            y=10,
        ).mean(skipna=True).compute()

        tract_dollars_xr_coarse.to_netcdf(path=self.save_template.format(dollar_column, self.year))

        return

    def process(self):

        # process dollars
        for d in ['MEDIAN_GROSS_RENT_PCT_HH_INCOME', 'PCI']:
            dollar_save_path = self.save_template.format(d, self.year)
            if os.path.exists(dollar_save_path):
                print('Skipping already processed variable {} for year {}'.format(d, self.year))
            else:
                print('Processing variable {} for year {}'.format(d, self.year))
                self.dollars_xarray(dollar_column=d)

        # process SVI
        for key, value in self.mapping.items():

            # skip the "total" columns
            if key != value:

                # skip columns that have already been processed
                save_path = self.save_template.format(key, self.year)
                if os.path.exists(save_path):
                    print('Skipping already processed variable {} for year {}'.format(key, self.year))
                else:
                    print('Processing variable {} for year {}'.format(key, self.year))
                    try:
                        self.svi_xarray(svi_column=key, total_column=value)
                    except MemoryError:
                        print('Memory error encountered processing variable {} for year {}'.format(key, self.year))
                        print('Rough estimate of memory used by weighted SVI dataframe: {}'.format(sys.getsizeof(self.weighted_svi)))


def main(db_path, landscan_path, year, save_template):

    a = AnnualSVI(
        db_path=db_path,
        landscan_path=landscan_path,
        year=year,
        save_template=save_template
    )
    a.process()


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--db_path', help='path to sqlite database file')
    parser.add_argument('-l', '--landscan', help='path to parquet format landscan + census tract spatial join')
    parser.add_argument('-y', '--year', help='year of census data to process')
    parser.add_argument('-s', '--save_template', help='string template for formatting save path')

    opts = parser.parse_args()

    # "/scratch1/06134/kpierce/landscan/{}_{}_landscan_30arcsecond_masked_xr_20211111.nc"
    main(db_path=opts.db_path, landscan_path=opts.landscan, year=opts.year, save_template=opts.save_template)