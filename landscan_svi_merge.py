import pandas as pd
from spatialpandas.io import read_parquet
import numpy as np
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
        weighted_landscan  = pd.merge(self.landscan, rdf_grouped, left_on='GEOID', right_on='GEOID', how='left')

        # perform weight calcuation
        weighted_landscan['landscan_weight'] = self.weighted_landscan['average_population'] / self.weighted_landscan['landscan_tract_total_pop']

        return weighted_landscan

    def annual_svi_weighted_landscan(self):

        self.weighted_landscan = self.weighted_landscan.set_index('GEOID')

        svi_wide_indexed = self.svi_wide.set_index('GEOID')
        dollars_wide_indexed = self.dollars_wide.set_index('GEOID')

        weighted_svi = pd.merge(self.weighted_landscan, svi_wide_indexed, left_index=True, right_index=True, how='left')
        weighted_dollars = pd.merge(self.weighted_landscan, dollars_wide_indexed, left_index=True, right_index=True, how='left')

        return weighted_svi, weighted_dollars

    def svi_xarray(self, svi_column, total_column):

        total = '{}_corrected'
        counts = '{}_LANDSCAN_COUNT'
        percents = '{}_LANDSCAN_PERCENT'

        # subset the data and calculate the landscan count estimate
        data_subset = self.weighted_svi[['x', 'y', 'landscan_weight', svi_column, total_column]].reset_index()
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

        return data_subset_xr_30arcsec

    def process(self):

        for key, value in self.mapping.items():
            self.svi_xarray(svi_column=key, total_column=value)


def main(db_path, landscan_path, save_template):

    for y in [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]:
        a = AnnualSVI(
            db_path=db_path,
            landscan_path=landscan_path,
            year=y,
            save_template=save_template
        )
        a.process()


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--db_path', help='path to sqlite database file')
    parser.add_argument('-l', '--landscan', help='path to parquet format landscan + census tract spatial join')
    parser.add_argument('-s', '--save_template', help='string template for formatting save path')

    opts = parser.parse_args()

    # "/scratch1/06134/kpierce/landscan/{}_{}_landscan_30arcsecond_masked_xr_20211111.nc"
    main(db_path=opts.db_path, landscan_path=opts.landscan, save_template=opts.save_template)