# Distributed Satellite NDVI Pipeline

Extract parcel-level vegetation statistics from encoded NDVI GeoTIFFs and store time series in PostGIS. The project combines Ray tasks, raster masking, geographic reprojection, database upserts, and a Power BI report.

![Power BI report preview](docs/powerbi_dashboard_preview.png)

## Pipeline

1. Enumerate dated raster files for each configured province.
2. Read parcel boundaries and expected parcel counts from PostGIS.
3. Dispatch a Ray task per date, mask the raster by each parcel, and calculate the median pixel value.
4. Convert the encoded value using `(value - 127.5) / 127.5`, then upsert parcel statistics and task counts.
5. Explore the resulting time series in Power BI.

The conversion assumes the input raster already contains NDVI encoded on a 0–255 scale. The pipeline does not compute NDVI from raw red and near-infrared bands. Fractional CPU reservations control task scheduling; they are not a measured speedup or a CPU usage limit.

## Repository guide

| File | Purpose |
| --- | --- |
| [Worker](src/distributed_ndvi_worker.py) | Configuration, database operations, raster masking, and Ray remote task |
| [Pipeline manager](src/pipeline_manager.py) | Province/date discovery, task dispatch, and notifications |
| [Configuration example](config/db_config.example.py) | Reference configuration template; not imported by the current scripts |
| [Power BI report](powerbi/crop_health_ndvi_report.pbix) | Report artifact; refresh requires your data source |

## Setup

This is an environment-dependent pipeline, with no bundled raster or parcel dataset. You need Python, the packages below, a PostGIS database, and matching raster files accessible to the worker processes.

### Install dependencies

Run these commands from a terminal with Python available:

```bash
git clone https://github.com/Panutle/distributed-satellite-ndvi-pipeline.git
cd distributed-satellite-ndvi-pipeline
python -m venv .venv
```

Activate the environment using the command for your shell:

| Shell | Command |
| --- | --- |
| Windows PowerShell | `.\.venv\Scripts\Activate.ps1` |
| macOS / Linux | `source .venv/bin/activate` |

```bash
python -m pip install -r requirements.txt
```

### Configure the implementation

Copying `config/db_config.example.py` to `config/db_config.py` does **not** change runtime settings: the worker currently defines its own `PipelineConfig` and `DB_DSN` and does not import that template or read its environment variables.

Before execution, adapt these locations:

| Location | Required configuration |
| --- | --- |
| `PipelineConfig` in the worker | Raster root (`fol_dir`), dataset key, region suffix, start date, and CPU reservation |
| `DB_DSN` in the worker | Your PostGIS connection settings; externalize credentials before deployment |
| `run_pipeline()` in the manager | Province selection and notification endpoint |
| Ray initialization | The scripts contain no explicit `ray.init(address="auto")`; configure cluster initialization before dispatch if using an existing cluster |

The database must already contain `public."Landmap_4326"` with `id`, `geom`, and `province_id`, and `public."Landmap_4326_Counts"` with `province_id` and `n_province`. Source geometries are treated as EPSG:4326. The script creates its output and task tables, not those source tables.

For the default `key=NDVI` and `add_on=TH`, province rasters are expected under `<fol_dir>/NDVI_TH/<province_id>/` with names such as `NDVI_01_20240101.tif`. Review the path functions when using another dataset layout.

### Execute after configuration

```bash
python src/pipeline_manager.py
```

Start with one province and a small date range by adjusting the manager and worker configuration. Inspect the output tables and parcel counts before expanding the run. Missing source files, connection failures, and caught exceptions must be checked in the logs; process completion alone is not proof of a successful load.

### View the report

Open `powerbi/crop_health_ndvi_report.pbix` in Power BI Desktop and update its data-source settings to your database. If Git retrieved an LFS pointer instead of the report, install Git LFS and run `git lfs pull` from this repository.

## Scope and limitations

- The repository demonstrates distributed geospatial processing and time-series storage; it does not include a reproducible throughput benchmark.
- Median aggregation reduces sensitivity to extreme pixel values but does not replace cloud masking or source-quality filtering.
- Reprojection aligns coordinate systems; it does not guarantee a particular positional accuracy.
- Task-count checks and upserts support repeat processing, but source revisions and concurrent runs need additional validation.
