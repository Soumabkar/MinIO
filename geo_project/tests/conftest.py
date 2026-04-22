"""
conftest.py — Fixtures partagées entre tous les tests.
"""
import pytest
import geopandas as gpd
import pandas as pd
from shapely import wkt

from src.geo_pipeline.config import AppConfig
from src.geo_pipeline.models import RawGeoRecord, EnrichedGeoRecord
from src.geo_pipeline.repository import AbstractGeoRepository, MergeStats


# =============================================================================
# Données de test
# =============================================================================

RAW_WKT_VALID = "POLYGON((2.3 48.9, 2.4 48.9, 2.4 49.0, 2.3 49.0, 2.3 48.9))"
RAW_WKT_VALID2 = "POLYGON((2.5 48.8, 2.6 48.8, 2.6 48.9, 2.5 48.9, 2.5 48.8))"
POLYGON_INVALID = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"  # self-intersection


@pytest.fixture
def cfg() -> AppConfig:
    return AppConfig.for_testing()


@pytest.fixture
def single_raw_record() -> RawGeoRecord:
    return RawGeoRecord(
        code_zone  = "ZN-001",
        nom_zone   = "Zone Test Nord",
        type_zone  = "industriel",
        population = 0,
        geom_wkt   = RAW_WKT_VALID,
    )


@pytest.fixture
def raw_records() -> list:
    return [
        RawGeoRecord(
            code_zone  = "ZN-001",
            nom_zone   = "Zone Nord",
            type_zone  = "industriel",
            population = 0,
            geom_wkt   = RAW_WKT_VALID,
        ),
        RawGeoRecord(
            code_zone  = "ZN-002",
            nom_zone   = "Zone Est",
            type_zone  = "résidentiel",
            population = 12500,
            geom_wkt   = RAW_WKT_VALID2,
        ),
        RawGeoRecord(
            code_zone  = "ZN-003",
            nom_zone   = "Zone Sud",
            type_zone  = "commercial",
            population = None,
            geom_wkt   = "POLYGON((2.2 48.7, 2.35 48.7, 2.35 48.8, 2.2 48.8, 2.2 48.7))",
        ),
    ]


@pytest.fixture
def enriched_record() -> EnrichedGeoRecord:
    return EnrichedGeoRecord(
        code_zone           = "ZN-001",
        nom_zone            = "Zone Nord",
        type_zone           = "industriel",
        population          = 0,
        geom_wkt            = RAW_WKT_VALID,
        geom_simplifiee_wkt = RAW_WKT_VALID,
        buffer_wkt          = RAW_WKT_VALID,
        centroide_lon       = 2.35,
        centroide_lat       = 48.95,
        aire_m2             = 81_000_000.0,
        perimetre_m         = 36_000.0,
        aire_km2            = 81.0,
        bbox_minx           = 2.3,
        bbox_miny           = 48.9,
        bbox_maxx           = 2.4,
        bbox_maxy           = 49.0,
        nom_region          = "Île-de-France",
        code_region         = "11",
    )


@pytest.fixture
def enriched_records(enriched_record) -> list:
    r2 = EnrichedGeoRecord(
        code_zone           = "ZN-002",
        nom_zone            = "Zone Est",
        type_zone           = "résidentiel",
        population          = 12500,
        geom_wkt            = RAW_WKT_VALID2,
        geom_simplifiee_wkt = RAW_WKT_VALID2,
        buffer_wkt          = RAW_WKT_VALID2,
        centroide_lon       = 2.55,
        centroide_lat       = 48.85,
        aire_m2             = 81_500_000.0,
        perimetre_m         = 36_500.0,
        aire_km2            = 81.5,
        bbox_minx           = 2.5,
        bbox_miny           = 48.8,
        bbox_maxx           = 2.6,
        bbox_maxy           = 48.9,
    )
    return [enriched_record, r2]


@pytest.fixture
def regions_ref() -> gpd.GeoDataFrame:
    data = {
        "nom_region":  ["Île-de-France", "Auvergne-Rhône-Alpes"],
        "code_region": ["11", "84"],
        "geometry": [
            wkt.loads("POLYGON((1.5 48.0, 3.5 48.0, 3.5 49.5, 1.5 49.5, 1.5 48.0))"),
            wkt.loads("POLYGON((2.0 44.0, 7.0 44.0, 7.0 47.0, 2.0 47.0, 2.0 44.0))"),
        ],
    }
    return gpd.GeoDataFrame(data, crs="EPSG:4326")


# =============================================================================
# Repository factice (in-memory) pour les tests d'intégration
# =============================================================================

class InMemoryGeoRepository(AbstractGeoRepository):
    """
    Implémentation en mémoire du repository.
    Aucune connexion réseau — permet de tester le pipeline sans Trino.
    """

    def __init__(self, source_data: list):
        self._source    = source_data
        self._dest: dict = {}          # code_zone → EnrichedGeoRecord

    def fetch_source(self) -> list:
        return list(self._source)

    def merge_into_dest(self, records: list) -> MergeStats:
        nb_avant = len(self._dest)
        for r in records:
            self._dest[r.code_zone] = r   # INSERT ou UPDATE (upsert dict)
        return MergeStats(nb_avant, len(self._dest))

    def count_dest(self) -> int:
        return len(self._dest)

    def fetch_dest_sample(self, limit: int = 5) -> pd.DataFrame:
        rows = list(self._dest.values())[:limit]
        return pd.DataFrame([r.__dict__ for r in rows])

    def get_record(self, code_zone: str) -> EnrichedGeoRecord:
        """Helper de test : accès direct à un enregistrement."""
        return self._dest.get(code_zone)

    def all_records(self) -> list:
        return list(self._dest.values())


@pytest.fixture
def in_memory_repo(raw_records):
    return InMemoryGeoRepository(source_data=raw_records)
RAW_WKT_INVALID = POLYGON_INVALID
