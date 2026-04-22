"""
test_models.py — Tests unitaires des modèles du domaine.
"""
import pytest
from src.geo_pipeline.models import RawGeoRecord, EnrichedGeoRecord


class TestRawGeoRecord:

    def test_creation_valide(self):
        r = RawGeoRecord(
            code_zone="ZN-001", nom_zone="Test",
            type_zone="industriel", population=0,
            geom_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        )
        assert r.code_zone == "ZN-001"
        assert r.population == 0

    def test_code_zone_vide_leve_erreur(self):
        with pytest.raises(ValueError, match="code_zone"):
            RawGeoRecord(
                code_zone="", nom_zone="Test",
                type_zone=None, population=None,
                geom_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
            )

    def test_geom_wkt_vide_leve_erreur(self):
        with pytest.raises(ValueError, match="geom_wkt"):
            RawGeoRecord(
                code_zone="ZN-001", nom_zone=None,
                type_zone=None, population=None,
                geom_wkt=""
            )

    def test_population_none_acceptee(self):
        r = RawGeoRecord(
            code_zone="ZN-X", nom_zone=None,
            type_zone=None, population=None,
            geom_wkt="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        )
        assert r.population is None

    def test_immutabilite(self, single_raw_record):
        """RawGeoRecord est frozen=True — pas de modification possible."""
        with pytest.raises((AttributeError, TypeError)):
            single_raw_record.code_zone = "AUTRE"


class TestEnrichedGeoRecord:

    def test_is_in_bbox_centroide_valide(self, enriched_record):
        assert enriched_record.is_in_bbox(-5.5, 41.0, 10.0, 52.0) is True

    def test_is_in_bbox_centroide_hors_france(self, enriched_record):
        # Centroïde à 2.35/48.95 est dans la bbox France — on teste hors
        assert enriched_record.is_in_bbox(10.0, 50.0, 20.0, 60.0) is False

    def test_is_in_bbox_limite_exacte(self, enriched_record):
        # Centroïde exactement sur la limite → inclus
        assert enriched_record.is_in_bbox(
            enriched_record.centroide_lon,
            enriched_record.centroide_lat,
            enriched_record.centroide_lon,
            enriched_record.centroide_lat,
        ) is True

    def test_immutabilite(self, enriched_record):
        with pytest.raises((AttributeError, TypeError)):
            enriched_record.aire_m2 = 999.0
