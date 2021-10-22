import pandas as pd
import spatialpandas as spd
from spatialpandas.geometry import PointArray
import geopandas as gpd
from shapely.geometry import Polygon
import rioxarray
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import math
import multiprocessing as mp
import time
import logging
logging.basicConfig(level=logging.DEBUG)


###############
#    NOTES    #
###############

# setting the "chunks" argument allows us to read in the xarray as a dask array instead of a numpy array
# spatial merges at scale:
# - https://www.quansight.com/post/spatial-filtering-at-scale-with-dask-and-spatialpandas
# package `spatialpandas` is CRS-agnostic, so any pre-join CRS manipulations must be done with geopandas

def floor_integer(xarray):
    """
    Cast float type xarray.DataArray with one variable to integer, taking the floor of the float values first.
    Produces the same result as directly casting to integer for values already close to their floor, but is
    more transparent and deliberate about value handling.
    """

    xarray_floor = xr.ufuncs.floor(xarray)
    xarray_floor = xarray_floor.fillna(0)
    xarray_floor = xarray_floor.astype('int')

    return xarray_floor


def load_landscan(night_path, day_path):

    night_raster = rioxarray.open_rasterio(
        night_path,
        masked=True,
        chunks={'x': 1000, 'y': 1000}
    )
    day_raster = rioxarray.open_rasterio(
        day_path,
        masked=True,
        chunks={'x': 1000, 'y': 1000}
    )

    return night_raster, day_raster


class Landscan:

    def __init__(self, night_raster, day_raster, shp):
        self.night_raster = night_raster
        self.day_raster = day_raster
        self.shp = shp
        self.raster_crs = None
        self.shp_crs = None
        self.average_raster = None
        self.average_raster_dask = None
        self.spdf = None

    def average_population(self):
        """
        Calculate the average population from night and day population rasters
        """

        assert self.night_raster.rio.crs == self.day_raster.rio.crs
        self.raster_crs = self.night_raster.rio.crs

        # convert floats to integers
        night_int = floor_integer(self.night_raster)
        day_int = floor_integer(self.day_raster)

        # convert data arrays to datasets
        night_ds = night_int.to_dataset(name='night_population')
        day_ds = day_int.to_dataset(name='day_population')

        # merge datasets
        average_raster = xr.merge([night_ds, day_ds])

        # calculate average and round to lowest whole person
        avg_pop = average_raster.to_array(dim='new').mean('new')
        avg_pop_round = avg_pop.round()
        avg_pop_int = floor_integer(avg_pop_round)

        # add average population dimension to dataset
        self.average_raster = average_raster.assign(average_population=avg_pop_int)

    def dask_spatial_sort(self, savepath):

        ddf = self.average_raster.to_dask_dataframe()
        df = ddf.map_partitions(
            lambda df: spd.GeoDataFrame(dict(
                position=PointArray(df[['x', 'y']]),
                **{col: df[col] for col in df.columns}
            ))
        )

        # todo: do we have to save this to disk and reload to get the benefits of dask parallelization?
        t0 = time.time()
        df.pack_partitions(npartitions=df.npartitions, shuffle='disk').to_parquet(savepath)
        dt = time.time() - t0
        logging.info(f'Spatial sort required {dt} seconds.')

        self.average_raster_dask = spd.io.read_parquet_dask(savepath)


    def gpd_to_spd(self):

        assert 'geometry' in self.shp.columns

        load_gpd = self.shp[['GEOID', 'geometry']]
        try:
            load_gpd.to_crs(self.raster_crs)
        except Exception:
            logging.debug(f'Unable to convert shapefile {self.shp_path} to CRS {self.raster_crs}')
            raise Exception

        self.shp_crs = load_gpd.crs
        self.spdf = spd.geodataframe.GeoDataFrame(load_gpd, geometry='geometry')


    def spd_sjoin_wrapper(self):

        assert self.raster_crs == self.shp_crs
        rdf = spd.sjoin(self.average_raster_dask, self.spdf, how='inner').compute()


# wrapper function moified from https://towardsdatascience.com/geospatial-operations-at-scale-with-dask-and-geopandas-4d92d00eb7e8
def sjoin_wrapper(dask_df, geo_df):
    # extract the lat and lon from a dask df partition
    local_df = dask_df[['x', 'y']].copy()

    # assume that the geo_df CRS can be usedf with the dask partition data
    new_geometry = gpd.points_from_xy(local_df['x'], local_df['y'])
    local_gdf = gpd.GeoDataFrame(
        local_df,
        geometry=new_geometry,
        crs=geo_df.crs
    )

    local_gdf = gpd.sjoin(
        local_gdf, geo_df, how='left', op='within')

    return local_gdf['GEOID']

# texas_total_df['GEOID'] = texas_total_df.map_partitions(sjoin_wrapper, texas_tracts, meta=('GEOID', str))
# texas_total_df.to_parquet('/work2/06134/kpierce/frontera/landscan/texas_total_population_landscan_3arcsecond_20211020.parquet', has_nulls=False,
#  object_encoding='json', compression="SNAPPY")


def make_divisible_square_extent(raster_xr, finalize_fxn, multiple=None):
    """
    pad raster with zeros to expand extent
    raster_xr: raster data in xarray
    multiple: None or integer value by which raster extent should be divisible
    """

    n_row = raster_xr.rio.height
    n_col = raster_xr.rio.width

    new_dim = max(n_row, n_col)

    # ensure the new dimensions are divisible by the desired aggregation scale
    if multiple:
        new_dim = math.ceil(new_dim / multiple) * multiple

    # extend the x and y coordinates at the appropriate resolutions
    y_resolution = raster_xr.rio.resolution()[1]
    x_resolution = raster_xr.rio.resolution()[0]

    start_x = raster_xr['x'][-1]
    start_y = raster_xr['y'][-1]

    # set the diagonal
    new_x = np.array([start_x + i * x_resolution for i in range(1, new_dim - n_col + 1)])
    new_y = np.array([start_y + i * y_resolution for i in range(1, new_dim - n_row + 1)])
    population = np.zeros((len(new_x), len(new_y))).astype(int)

    final_xr = finalize_fxn(new_x, new_y, population, raster_xr)

    return final_xr


def merge_datasets(new_x, new_y, population, original_array):
    extended_xr = xr.Dataset(
        data_vars=dict(population=(['x', 'y'], population)),
        coords=dict(
            x=(['x', ], new_x),
            y=(['y', ], new_y)
        )
    )

    final_xr = xr.combine_nested(
        [[original_array.to_dataset(name='population')], [extended_xr]],
        concat_dim=['x', 'y']
    )

    # fill NaN with 0 and recast type to int
    final_xr_array = final_xr.to_array()
    final_xr_array = final_xr_array.fillna(0)
    final_xr_array = final_xr_array.astype('int')

    # confirm no data loss (or gain) in concatenation
    try:
        total_a = np.nansum(original_array.values.flatten()) + np.nansum(extended_xr['population'].values.flatten())
        total_b = np.nansum(final_xr_array.values.flatten())
        print(f'Aggregate total = {total_a}; final total = {total_b}')
        assert total_a == total_b
    except AssertionError:
        print(f'Aggregate total {total_a} is not equal to final total {total_b}')

    return final_xr_array


def non_overlapping_tiles(total_length, tile_length):

    slices = [i for i in range(0, total_length + tile_length, tile_length)]

    return slices


class ReduceRaster:

    def __init__(self, square_raster, window_size, chunk_size):

        self.square_raster = square_raster
        self.window_size = window_size
        self.chunk_size = chunk_size
        self.tile_indexes = None

    def sanity_checks(self):

        n_row = self.square_raster.rio.height
        n_col = self.square_raster.rio.width

        n_errors = 0

        try:
            assert n_row == n_col
        except AssertionError:
            print(f'Raster with dimensions ({n_row}, {n_col}) is not square.')
            n_errors += 1

        try:
            assert self.chunk_size % self.window_size == 0
        except AssertionError:
            print(f'Chunk size {self.chunk_size} is not a multiple of the aggregation level {self.window_size}.')
            n_errors += 1

        try:
            assert self.square_raster.rio.height % self.chunk_size == 0
        except AssertionError:
            print(
                f'Chunk size {self.chunk_size} is not a multiple of the xarray dimensions {self.square_raster.rio.height}.')
            n_errors += 1

        try:
            assert self.square_raster.rio.height % self.window_size == 0
        except AssertionError:
            print(
                f'Window size {self.window_size} is not a multiple of the xarray dimensions {self.square_raster.rio.height}.')
            n_errors += 1

        return n_errors

    def agg_tile(self, raster_chunk):

        agg_data = dict(x=list(), y=list(), population=list())

        for i in range(1, len(self.tile_indexes)):
            for j in range(1, len(self.tile_indexes)):
                to_aggregate = raster_chunk.isel(
                    x=np.arange(self.tile_indexes[i - 1], self.tile_indexes[i]),
                    y=np.arange(self.tile_indexes[j - 1], self.tile_indexes[j])
                )
                agg_data['x'].append(np.mean(to_aggregate['x']).values.item())
                agg_data['y'].append(np.mean(to_aggregate['y']).values.item())
                agg_data['population'].append(sum(to_aggregate.values.flatten()))

        agg_df = pd.DataFrame.from_dict(agg_data)

        return agg_df

    def reduce_raster_resolution(self, ncpu):

        n_errors = self.sanity_checks()
        if n_errors > 0:
            raise AssertionError('Incorrect parameterization for resolution reduction.')

        # set standard coordinate slices to use for processing data within chunks
        self.tile_indexes = non_overlapping_tiles(total_length=self.chunk_size, tile_length=self.window_size)

        # slice the xarray into large chunks for parallel processing
        xr_slices = non_overlapping_tiles(total_length=self.square_raster.rio.height, tile_length=self.chunk_size)

        tasks = []
        for i in range(1, len(xr_slices)):
            for j in range(1, len(xr_slices)):
                tasks.append(self.square_raster.isel(
                    x=np.arange(xr_slices[i - 1], xr_slices[i]),
                    y=np.arange(xr_slices[j - 1], xr_slices[j])
                ))

        pool = mp.Pool(ncpu)
        results = [pool.apply_async(self.agg_tile, (t,)) for t in tasks]
        pool.close()
        outputs = []
        for r in results:
            out = r.get()
            outputs.append(out)

        agg_df = pd.concat(outputs)

        return agg_df



def fishnet(rio_xarray):

    # get the offset from the centroid to calculate bounding boxes
    width = rio_xarray.rio.resolution()[0] / 2
    height = rio_xarray.rio.resolution()[1] / 2

    x_vals = []
    y_vals = []
    polygons = []
    population = []
    for x in rio_xarray['x'].values:
        for y in rio_xarray['y'].values:
            x_vals.append(x)
            y_vals.append(y)
            population.append(
                rio_xarray.sel({'x': x, 'y': y}).values.item()
            )
            polygons.append(Polygon([(x-width,y-width), (x-width, y+height), (x+width, y+height), (x+width, y-height)]))

    grid = gpd.GeoDataFrame(
        {
            'x': x_vals,
            'y': y_vals,
            'geometry': polygons,
            'population': population
        }
    )

    grid = grid.set_crs(rio_xarray.rio.crs)

    return grid