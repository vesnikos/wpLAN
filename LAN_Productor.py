from collections import defaultdict
import pathlib
import ftplib
import argparse

import click
import rasterio
import numpy as np

try:
    from local_config import wp_ftp, WORKFOLDER
except ImportError:
    # please make a local_config.py in the same directory as this file, and copy paste the following,
    # changing where appropriate:
    # import pathlib
    # output_folder = r"MY OUTPUT FOLDER"
    # WORKFOLDER = pathlib.Path(output_folder)
    # wp_ftp = {
    #     'ftp_server': 'ftp.worldpop.org.uk',
    #     'user': "user",  # wp_username
    #     'password': "passwrd", # wp_password
    # }

    wp_ftp = {
        'ftp_server': 'ftp.worldpop.org.uk',
        'user': 'user',
        'password': 'password',
    }
    WORKFOLDER = pathlib.Path(__file__).parent

GDAL_CACHEMAX = 512  # mb

wp_ftp['root'] = '/WP515640_Global/Covariates/{iso}/{product}'


class IllegalArgumentError(ValueError):
    pass


class DataBroker(object):

    def __init__(self):
        self.data = defaultdict(lambda: [+3e10, -3e10])  # zones start with default 0 min/max

    def get_min(self, zone):
        return self.data[zone][0]

    def get_max(self, zone):
        return self.data[zone][1]

    def set_min(self, zone, value):
        if not self.data[zone][0] < value:
            self.data[zone][0] = value

    def set_max(self, zone, value):
        if not self.data[zone][1] > value:
            self.data[zone][1] = value

    def get_range(self, zone):
        # abs(max - min)
        return np.absolute(self.get_max(zone) - self.get_min(zone))


def main(iso, year1, year2):
    years = [year for year in range(year1, year2) if year != 2011]
    ISO_FOLDER = WORKFOLDER / iso.upper()
    LAN_FOLDER = ISO_FOLDER / 'LAN'
    OUTFOLDER = LAN_FOLDER / 'derived'

    if not OUTFOLDER.exists():
        OUTFOLDER.mkdir(parents=True, exist_ok=True)

    def footer(iso=iso):
        print(end='\n' * 3)
        print('Production of Weighted LAN for country {iso} Finished!'.format(iso=iso.upper()))
        print('\nThe product can be found in the folder: \n{folder}'.format(folder=LAN_FOLDER.absolute()))

    def header():
        print(
            'Welcome to the Weighted LAN producing Program. This program is going to derive the Lights-At-Night Product '
            'for: \n {iso} \n and years: \n{years}'.format(iso=iso, years=[y for y in years]))

    def download_ccid_product():
        with ftplib.FTP(wp_ftp['ftp_server'], user=wp_ftp['user'], passwd=wp_ftp['password']) as ftp:
            ccid_ftp_folder = '/WP515640_Global/Covariates/{iso}/Mastergrid'.format(iso=iso.upper())
            ftp.cwd(ccid_ftp_folder)

            with ccidadminl1_file.open('wb') as f:
                ftp.retrbinary('RETR ' + ccidadminl1_file.name, f.write)

    def get_lan_filenames(year1: int, iso: str) -> (str, str):
        prod1_template = "{iso}_grid_100m_dmsp_{year}.tif"
        prod2_template = "{iso}_grid_100m_viirs_{year}.tif"
        if year1 > 2016 or year1 < 2000:
            return None, None
        if year1 == 2011:
            return prod1_template.format(year=year1, iso=iso)
        if year1 < 2011:
            return prod1_template.format(year=year1, iso=iso), prod1_template.format(year=year1 + 1, iso=iso)
        if year1 > 2011:
            return prod2_template.format(year=year1, iso=iso), prod2_template.format(year=year1 + 1, iso=iso)

    def get_lan_outname(year: int, iso):
        template = '{iso}_{prod}_{year_target}_normlag_{year_plus1}-{year_target}'
        if year < 2011:
            return template.format(
                product='dmsp',
                iso=iso,
                year_target=year,
                year_plus1=year + 1
            )
        if year > 2011:
            return template.format(
                product='viirs',
                iso=iso,
                year_target=year,
                year_plus1=year + 1
            )

    def download_lan_product(year):
        with ftplib.FTP(wp_ftp['ftp_server'], user=wp_ftp['user'], passwd=wp_ftp['password']) as ftp:
            if year < 2012:
                ftp.cwd(wp_ftp['root'].format(iso=iso.upper(), product='DMSP'))
            if year > 2011:
                ftp.cwd(wp_ftp['root'].format(iso=iso.upper(), product='VIIRS'))
            for FILENAME in get_lan_filenames(year, iso=iso):
                outfile = LAN_FOLDER.joinpath(FILENAME)
                if outfile.exists():
                    continue
                with open(outfile, 'wb') as f:
                    ftp.retrbinary('RETR ' + FILENAME, f.write)

    header()

    ccidadminl1_file = '{iso}_grid_100m_ccidadminl1.tif'.format(iso=iso)
    ccidadminl1_file = LAN_FOLDER.joinpath(ccidadminl1_file)
    if not ccidadminl1_file.exists():
        download_ccid_product()

    for year in years:
        LAN_1, LAN_2 = [LAN_FOLDER.joinpath(x) for x in get_lan_filenames(year, iso=iso)]
        if year < 2011:
            outfile = '{iso}_dmsp_{year1}_normlag_{year1}-{year2}.tif'.format(
                iso=iso.upper(),
                year1=year,
                year2=year + 1
            )
            outfile = OUTFOLDER.joinpath(outfile)
        else:
            outfile = '{iso}_viirs_{year1}_normlag_{year1}-{year2}.tif'.format(
                iso=iso.upper(),
                year1=year,
                year2=year + 1
            )
            outfile = OUTFOLDER.joinpath(outfile)

        # realise the LAN's if don't exist
        if not (LAN_FOLDER.joinpath(LAN_1).exists()
                or LAN_FOLDER.joinpath(LAN_2).exists()):
            download_lan_product(year)

        databroker = DataBroker()

        with rasterio.Env(GDAL_CACHEMAX=GDAL_CACHEMAX) as env, rasterio.open(
                ccidadminl1_file.as_posix()) as ccid_raster, \
                rasterio.open(LAN_2.as_posix()) as lan_2_raster, \
                rasterio.open(LAN_1.as_posix()) as lan_1_raster:

            ccid_nodata = ccid_raster.nodata

            def normalize(dstack, nodata):

                def normalize_value(zone, value):

                    if zone == nodata:
                        return nodata

                    min = databroker.get_min(zone)
                    max = databroker.get_max(zone)
                    # range = databroker.get_range(zone)
                    if max - min == 0:
                        return 0.0
                    result = 1.0 / (max - min) * (value - max) + 1.0
                    return result

                gen = [normalize_value(zone, value) for zone, value in dstack]
                result = np.fromiter(gen, dtype=rasterio.float32, )
                return result

            vnorm = np.vectorize(normalize, otypes=[rasterio.float32], signature='(m_2,n),()->(i)')

            # Iter1 get zonal stats
            with click.progressbar([window for _, window in ccid_raster.block_windows(1)]) as windows:
                for window in windows:
                    windows.label = 'Calculating Zonal Statistics'
                    ccid_block = ccid_raster.read(1, window=window)
                    lan1_block = lan_1_raster.read(1, window=window)
                    lan2_block = lan_2_raster.read(1, window=window)
                    lan_block = lan2_block - lan1_block
                    del lan2_block, lan1_block
                    datablock = np.stack((ccid_block, lan_block))
                    del lan_block
                    q = np.where(datablock[0] != ccid_nodata)
                    zones = np.unique(datablock[0][q])
                    for z in zones:
                        qq = np.where(datablock[0][q] == z)
                        min = np.min(datablock[1][q][qq])
                        max = np.max(datablock[1][q][qq])
                        databroker.set_max(zone=z, value=max)
                        databroker.set_min(zone=z, value=min)
                # Iter2 apply and normalize raster
            profile = ccid_raster.profile

            profile.update(dtype=rasterio.float32,
                           count=1, compress='lzw',
                           nodata=8888)
            with rasterio.open(outfile.as_posix(), 'w', **profile) as dst:
                with click.progressbar([window for _, window in ccid_raster.block_windows(1)]) as windows:
                    for window in windows:
                        windows.label = 'Writing Normalized LAN'
                        ccid_block = ccid_raster.read(1, window=window)

                        lan1_block = lan_1_raster.read(1, window=window)
                        lan2_block = lan_2_raster.read(1, window=window)
                        lan_block = lan2_block - lan1_block
                        del lan2_block, lan1_block
                        buffer = np.dstack((ccid_block, lan_block))
                        datastack = vnorm(buffer, profile['nodata'])

                        dst.write(datastack, indexes=1, window=window)

    footer()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('args', nargs='*')

    parsed_arguments = parser.parse_args()

    text_arguments = [arg for arg in parsed_arguments.args if not arg.isnumeric()]
    numeric_arguments = sorted(map(int, [arg for arg in parsed_arguments.args if arg.isnumeric()]))
    if len(numeric_arguments) != 2:
        print('Please provide the starting year and the ending year: YYYY YYYY')
        raise IllegalArgumentError
    iso = text_arguments[0]
    year1, year2 = numeric_arguments[0], numeric_arguments[1]

    # start the main routine
    main(iso, year1, year2)
