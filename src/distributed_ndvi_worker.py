import os
import gc
import sys
import warnings
from datetime import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import rasterio
from rasterio.mask import mask
import ray

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# Configuration & State Management
# ---------------------------------------------------------
class PipelineConfig:
    def __init__(self):
        self.key = 'NDVI'
        self.add_on = 'TH'
        self.joinPath = '/'
        self.fol_dir = '/Volumes/Volume1/Satellite'
        self.ls_pro_cut = ['TH', 'Landsat_TH']
        self.start_date = datetime(2015, 1, 1)
        self.num_cpus = 0.1  # Fractional CPU scheduling for high concurrency
        self.id_machine = 57
        self.n_start = 0

    @property
    def paht_dir_fold_id(self):
        return f'{self.fol_dir}{self.joinPath}{self.key}_{self.add_on}{self.joinPath}'

    def get_paht_dir_file(self, province_id: str) -> str:
        return f'{self.fol_dir}{self.joinPath}{self.key}_{self.add_on}{self.joinPath}{province_id}{self.joinPath}'

    def get_tb_task(self, province_id: str) -> str:
        return f'{self.key}_{province_id}_Landmap_4326_Task'

    def get_tb_plot(self, province_id: str) -> str:
        return f'{self.key}_{province_id}_Landmap_4326_Plot_Time'

var = PipelineConfig()

# Database Connection Credentials
DB_DSN = {
    "host": "192.0.100.53",
    "port": 5432,
    "database": "URC_postgis",
    "user": "postgres",
    "password": "SuperAI"
}

DB_URI = (
    f"postgresql+psycopg2://{DB_DSN['user']}:{DB_DSN['password']}@"
    f"{DB_DSN['host']}:{DB_DSN['port']}/{DB_DSN['database']}"
)

# ---------------------------------------------------------
# Database Operations & Idempotency Guards
# ---------------------------------------------------------
def setup_db(province_id: str):
    """Initializes tables for parcel time-series values and processing task logs."""
    create_tb_query_plot = f'''
        CREATE TABLE IF NOT EXISTS public."{var.get_tb_plot(province_id)}" (
            id int4 NOT NULL,
            Date date NOT NULL,
            key varchar NOT NULL,
            value numeric NULL,
            CONSTRAINT "{var.get_tb_plot(province_id)}_pkey" PRIMARY KEY (id, Date, key)
        )
        TABLESPACE "pg_default";
    '''
    create_tb_query_task = f'''
        CREATE TABLE IF NOT EXISTS public."{var.get_tb_task(province_id)}" (
            province int4 NOT NULL,
            Date date NOT NULL,
            numbervalue numeric NULL,
            key varchar NULL,
            CONSTRAINT "{var.get_tb_task(province_id)}_pkey" PRIMARY KEY (province, Date)
        )
        TABLESPACE "pg_default";
    '''

    try:
        with psycopg2.connect(**DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(create_tb_query_plot)
                cur.execute(create_tb_query_task)
                conn.commit()
    except Exception as e:
        print(f"[!] Database setup error for province {province_id}: {e}")


def get_last_row_lm(province_id: str) -> int:
    """Retrieves expected target parcel count for completeness verification."""
    query = f'''
        SELECT n_province
        FROM public."Landmap_4326_Counts"
        WHERE province_id = {province_id};
    '''
    try:
        with psycopg2.connect(**DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        print(f"[!] Error fetching parcel counts for province {province_id}: {e}")
        return 0


def get_gdf(province_id: str) -> gpd.GeoDataFrame:
    """Fetches cadastral plot geometries from PostGIS."""
    poly_query = f"""
        SELECT id, geom, province_id AS province_code
        FROM public."Landmap_4326"
        WHERE province_id = {province_id};
    """
    engine = create_engine(DB_URI, pool_pre_ping=True)
    with engine.connect() as conn:
        gdf = gpd.read_postgis(poly_query, conn, geom_col='geom')

    return gdf.set_crs("EPSG:4326")


def check_task(Date, key: str, province_id: str) -> int:
    """Checks previously completed record count to ensure execution idempotency."""
    query = f'''
        SELECT sum(numbervalue)
        FROM public."{var.get_tb_task(province_id)}"
        WHERE "date"='{Date}';
    '''
    try:
        with psycopg2.connect(**DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                return int(result[0]) if (result and result[0] is not None) else 0
    except Exception:
        return 0


def del_landsat(Date, key: str, province_id: str):
    """Prunes incomplete or corrupted historic runs before reprocessing."""
    query = f'''
        DELETE FROM public."{var.get_tb_plot(province_id)}"
        WHERE "date"='{Date}' AND key = 'NDVI_Landsat_TH';

        DELETE FROM public."{var.get_tb_task(province_id)}"
        WHERE "date"='{Date}' AND key = 'NDVI_Landsat_TH';
    '''
    try:
        with psycopg2.connect(**DB_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
    except Exception as e:
        print(f"[!] Error cleaning stale data for date {Date}: {e}")


def update_db_plot(ls_insert: list, province_id: str) -> bool:
    """High-throughput chunked upsert for NDVI values per land plot."""
    insert_query = f"""
        INSERT INTO public."{var.get_tb_plot(province_id)}" (id, Date, key, value)
        VALUES %s
        ON CONFLICT (id, Date, key) DO UPDATE
        SET value = EXCLUDED.value;
    """
    try:
        with psycopg2.connect(**DB_DSN) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, ls_insert)
                conn.commit()
        return True
    except Exception as e:
        print(f"[!] Error bulk-inserting plots for province {province_id}: {e}")
        return False


def update_db_task(ls_update: list, province_id: str):
    """Records task processing state and row counts."""
    update_query = f'''
        INSERT INTO public."{var.get_tb_task(province_id)}" (province, Date, numbervalue, key)
        VALUES %s
        ON CONFLICT (province, Date) DO UPDATE
        SET numbervalue = EXCLUDED.numbervalue,
            key         = EXCLUDED.key;
    '''
    try:
        with psycopg2.connect(**DB_DSN) as conn:
            with conn.cursor() as cur:
                execute_values(cur, update_query, ls_update)
                conn.commit()
    except Exception as e:
        print(f"[!] Error updating task state for province {province_id}: {e}")


# ---------------------------------------------------------
# Path Resolvers
# ---------------------------------------------------------
def get_path_cut(key: str, province_id: str, Date) -> str:
    return f'{var.get_paht_dir_file(province_id)}{key}_{province_id}_{Date.strftime("%Y%m%d")}.tif'


def get_path_fold(key: str, province_id: str, Date) -> str:
    return f'{var.paht_dir_fold_id}{Date.strftime("%Y%m%d")}_{key}.tif'


# ---------------------------------------------------------
# Ray Distributed Worker Task
# ---------------------------------------------------------
@ray.remote(num_cpus=var.num_cpus)
def process_single_date(date_str, province_id: str, last_row_lm: int, get_path):
    """
    Ray Remote Task:
    1. Evaluates idempotency state against target plot counts.
    2. Performs windowed raster masking for parcel boundaries.
    3. Calculates 50th percentile (median) NDVI and scales values.
    4. Executes atomic batch upserts to PostGIS.
    """
    sp_time = datetime.now()
    key = var.key
    Date = date_str
    path = get_path(key, province_id, Date)
    read = None

    try:
        last_row = check_task(Date, key, province_id)

        # Reprocess if record count doesn't match baseline
        if int(last_row) != int(last_row_lm):
            del_landsat(Date, key, province_id)

            read = rasterio.open(path)
            crs = read.crs

            # Load vector geometries and reproject to raster CRS
            gdf = get_gdf(province_id)
            gdf = gdf.to_crs(crs)

            ls_insert = []
            ls_update = []

            for _, row in gdf.iterrows():
                plot_id = row['id']
                geometry = row['geom']
                province_code = row['province_code']

                if str(province_code) == 'None':
                    province_code = '99'

                try:
                    clipped_raster, _ = mask(read, [geometry], crop=True)
                    clipped_raster = clipped_raster[clipped_raster != 0]

                    # Vectorized 50th percentile calculation
                    if clipped_raster.size > 0:
                        value = np.nanpercentile(clipped_raster, 50)
                        value = round((value - 127.5) / 127.5, 2)
                    else:
                        value = np.nan

                    if not np.isnan(value):
                        ls_insert.append((int(plot_id), str(Date), f'{key}_{var.add_on}', float(value)))
                        ls_update.append([key, Date, province_code])
                    else:
                        ls_update.append([key, Date, '999'])

                except Exception:
                    ls_update.append([key, Date, '899'])

            # Aggregate state updates
            js_update = {}
            for item in [tuple(x) for x in ls_update]:
                js_update[item] = js_update.get(item, 0) + 1

            ls_update_records = [
                (int(j[2]), str(j[1]), int(j[3]), f'{j[0]}_{var.add_on}')
                for j in [list(i) + [js_update[i]] for i in set(tuple(x) for x in ls_update)]
            ]

            log_update_db_plot = True
            if ls_insert:
                log_update_db_plot = update_db_plot(ls_insert, province_id)

            if log_update_db_plot:
                update_db_task(ls_update_records, province_id)

            return 'P'
        else:
            return 'UTD'

    except Exception as e:
        print(f"[!] Processing failed for Date: {Date}, Province: {province_id}: {e}")
        return str(e)

    finally:
        if read is not None:
            try:
                read.close()
            except Exception:
                pass
        gc.collect()  # Flush GDAL raster buffer references
        ep_time = datetime.now()
        print(f"\tFinished: Province {province_id}, Date {date_str} in {ep_time - sp_time}")