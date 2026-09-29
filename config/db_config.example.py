"""
Database & Distributed Pipeline Configuration Template
======================================================
Copy this file to `config/db_config.py` (or set environment variables) 
and populate with your actual cluster and database credentials.

DO NOT commit sensitive production credentials to public repositories.
"""

import os
from datetime import datetime

# ---------------------------------------------------------
# PostgreSQL / PostGIS Spatial Database Configuration
# ---------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("POSTGIS_HOST", "192.0.100.53"),
    "port": int(os.getenv("POSTGIS_PORT", 5432)),
    "database": os.getenv("POSTGIS_DB", "URC_postgis"),
    "user": os.getenv("POSTGIS_USER", "postgres"),
    "password": os.getenv("POSTGIS_PASSWORD", "YourSecurePasswordHere")
}

# SQLAlchemy connection string URI format
DB_URI = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ---------------------------------------------------------
# Ray Cluster & Distributed Pipeline Runtime Parameters
# ---------------------------------------------------------
class PipelineSettings:
    """Runtime orchestration parameters for Ray distributed workers."""
    def __init__(self):
        # Calculation parameters
        self.key = os.getenv("PIPELINE_KEY", "NDVI")
        self.add_on = os.getenv("PIPELINE_ADDON", "TH")
        self.joinPath = os.sep
        
        # Filesystem / Shared Storage Volume
        self.fol_dir = os.getenv("SATELLITE_DIR", "/Volumes/Volume1/Satellite")
        self.ls_pro_cut = ['TH', 'Landsat_TH']
        
        # Temporal bounding
        self.start_date = datetime(2015, 1, 1)
        
        # Ray Scheduling Parameters
        # Fractional CPU reservation per worker task (enables high I/O concurrency)
        self.num_cpus = float(os.getenv("RAY_WORKER_CPU_FRACTION", 0.1))
        
        # Cluster Telemetry & Machine Identity
        self.id_machine = int(os.getenv("MACHINE_NODE_ID", 57))
        self.n_start = 0

    @property
    def paht_dir_fold_id(self) -> str:
        """Root directory path for satellite tiles."""
        return os.path.join(self.fol_dir, f"{self.key}_{self.add_on}") + self.joinPath

    def get_paht_dir_file(self, province_id: str) -> str:
        """Province-specific raster tile directory."""
        return os.path.join(self.fol_dir, f"{self.key}_{self.add_on}", str(province_id)) + self.joinPath

    def get_tb_task(self, province_id: str) -> str:
        """Dynamic PostGIS tracking table name for execution idempotency."""
        return f"{self.key}_{province_id}_Landmap_4326_Task"

    def get_tb_plot(self, province_id: str) -> str:
        """Target PostGIS table name for parcel time-series NDVI records."""
        return f"{self.key}_{province_id}_Landmap_4326_Plot_Time"


# ---------------------------------------------------------
# Telemetry & Alert Webhooks (ntfy.sh / Slack / LINE)
# ---------------------------------------------------------
ALERT_CONFIG = {
    "enabled": True,
    "ntfy_topic": os.getenv("NTFY_TOPIC", "Notification_Train_Models"),
    "ntfy_endpoint": os.getenv("NTFY_ENDPOINT", "https://ntfy.sh/Notification_Train_Models")
}