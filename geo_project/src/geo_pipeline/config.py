"""
config.py — Configuration centralisée (Value Object + Factory pattern)
Toutes les constantes en un seul endroit, aucune valeur en dur ailleurs.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TrinoConfig:
    """Paramètres de connexion Trino (immuable)."""
    host:    str = os.getenv("TRINO_HOST",    "localhost")
    port:    int = int(os.getenv("TRINO_PORT", "8080"))
    user:    str = os.getenv("TRINO_USER",    "admin")
    catalog: str = os.getenv("TRINO_CATALOG", "iceberg")
    schema:  str = os.getenv("TRINO_SCHEMA",  "geo_schema")
    http_scheme: str = "http"

    @property
    def url(self) -> str:
        return f"trino://{self.user}@{self.host}:{self.port}/{self.catalog}/{self.schema}"


@dataclass(frozen=True)
class TableConfig:
    """Noms des tables source et destination."""
    source: str = os.getenv("GEO_TABLE_SOURCE", "source_geo")
    dest:   str = os.getenv("GEO_TABLE_DEST",   "dest_geo")


@dataclass(frozen=True)
class GeoConfig:
    """Paramètres géospatiaux (CRS, tolérances)."""
    crs_source:         str   = "EPSG:4326"   # WGS84 — entrée/sortie
    crs_metric:         str   = "EPSG:2154"   # Lambert-93 — calculs métriques
    col_geom_wkt:       str   = "geom_wkt"    # nom colonne WKT dans source
    col_id_metier:      str   = "code_zone"   # clé de déduplication
    buffer_metres:      float = 500.0
    simplify_tolerance: float = 0.0001        # degrés (WGS84)
    bbox_france: tuple = (-5.5, 41.0, 10.0, 52.0)  # (lon_min, lat_min, lon_max, lat_max)


@dataclass(frozen=True)
class LoadConfig:
    """Paramètres de chargement."""
    batch_size: int = 500   # lignes par INSERT Trino


@dataclass(frozen=True)
class AppConfig:
    """Configuration globale — assemblée par la factory."""
    trino:  TrinoConfig  = field(default_factory=TrinoConfig)
    tables: TableConfig  = field(default_factory=TableConfig)
    geo:    GeoConfig    = field(default_factory=GeoConfig)
    load:   LoadConfig   = field(default_factory=LoadConfig)

    @classmethod
    def from_env(cls) -> AppConfig:
        """Factory : charge la configuration depuis les variables d'environnement."""
        return cls(
            trino  = TrinoConfig(),
            tables = TableConfig(),
            geo    = GeoConfig(),
            load   = LoadConfig(),
        )

    @classmethod
    def for_testing(cls) -> AppConfig:
        """Factory : configuration minimale pour les tests unitaires."""
        return cls(
            trino  = TrinoConfig(host="localhost", port=8080),
            tables = TableConfig(source="source_geo_test", dest="dest_geo_test"),
            geo    = GeoConfig(),
            load   = LoadConfig(batch_size=10),
        )
