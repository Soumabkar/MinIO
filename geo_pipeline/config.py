TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "admin"

# Source
SRC_CATALOG = "iceberg"
SRC_SCHEMA  = "test_schema"
SRC_TABLE   = "test_table"

# Cible
DST_CATALOG = "iceberg"
DST_SCHEMA  = "test_schema"
DST_TABLE   = "table_cible"

# CRS
SRC_EPSG = 4326   # WGS84 (lon/lat)
DST_EPSG = 3785   # Web Mercator (mètres)
