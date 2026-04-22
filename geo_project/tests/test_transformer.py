"""
test_transformer.py — Tests unitaires des stratégies de transformation.
"""
import pytest
import geopandas as gpd
from shapely import wkt

from src.geo_pipeline.transformer import (
    GeoTransformer,
    GeometryValidationStrategy,
    MetricsStrategy,
    SimplifyStrategy,
    BufferStrategy,
    SpatialJoinStrategy,
    WktExportStrategy,
    ReprojectionStrategy,
)
from src.geo_pipeline.config import GeoConfig
from src.geo_pipeline.models import RawGeoRecord


# =============================================================================
# Helpers
# =============================================================================

def make_gdf(wkt_str: str, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Crée un GeoDataFrame minimal pour tester une stratégie."""
    return gpd.GeoDataFrame(
        {"code_zone": ["ZN-TEST"], "nom_zone": ["Test"],
         "type_zone": ["industriel"], "population": [0]},
        geometry=[wkt.loads(wkt_str)],
        crs=crs,
    )


POLYGON_WGS84 = "POLYGON((2.3 48.9, 2.4 48.9, 2.4 49.0, 2.3 49.0, 2.3 48.9))"
POLYGON_INVALID = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"   # nœud papillon


# =============================================================================
# Tests par stratégie
# =============================================================================

class TestGeometryValidationStrategy:

    def test_valide_inchangee(self):
        gdf = make_gdf(POLYGON_WGS84)
        result = GeometryValidationStrategy().apply(gdf)
        assert result.geometry.is_valid.all()

    def test_invalide_corrigee(self):
        gdf = make_gdf(POLYGON_INVALID)
        assert not gdf.geometry.is_valid.all()
        result = GeometryValidationStrategy().apply(gdf)
        assert result.geometry.is_valid.all()

    def test_gdf_non_modifie_en_place(self):
        """Doit retourner une copie, pas modifier l'original."""
        gdf = make_gdf(POLYGON_INVALID)
        original_valid = gdf.geometry.is_valid.copy()
        GeometryValidationStrategy().apply(gdf)
        assert list(gdf.geometry.is_valid) == list(original_valid)


class TestReprojectionStrategy:

    def test_crs_change(self):
        gdf = make_gdf(POLYGON_WGS84, crs="EPSG:4326")
        result = ReprojectionStrategy("EPSG:2154").apply(gdf)
        assert result.crs.to_epsg() == 2154

    def test_coordonnees_changent(self):
        gdf = make_gdf(POLYGON_WGS84, crs="EPSG:4326")
        result = ReprojectionStrategy("EPSG:2154").apply(gdf)
        # En Lambert-93 les coordonnées sont en mètres (~600_000 / 6_800_000)
        bounds = result.geometry.bounds.iloc[0]
        assert bounds["minx"] > 100_000   # pas des degrés


class TestMetricsStrategy:

    def setup_method(self):
        # GDF déjà en Lambert-93 (on applique Metrics après Reprojection)
        gdf_wgs = make_gdf(POLYGON_WGS84)
        self.gdf_m = gdf_wgs.to_crs("EPSG:2154")
        self.strategy = MetricsStrategy(crs_source="EPSG:4326")

    def test_colonnes_creees(self):
        result = self.strategy.apply(self.gdf_m)
        for col in ["aire_m2", "aire_km2", "perimetre_m",
                    "centroide_lon", "centroide_lat",
                    "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"]:
            assert col in result.columns, f"Colonne manquante : {col}"

    def test_aire_positive(self):
        result = self.strategy.apply(self.gdf_m)
        assert result["aire_m2"].iloc[0] > 0

    def test_coherence_aire_km2(self):
        result = self.strategy.apply(self.gdf_m)
        aire_m2  = result["aire_m2"].iloc[0]
        aire_km2 = result["aire_km2"].iloc[0]
        assert abs(aire_km2 - aire_m2 / 1_000_000) < 0.001

    def test_perimetre_positif(self):
        result = self.strategy.apply(self.gdf_m)
        assert result["perimetre_m"].iloc[0] > 0

    def test_centroide_dans_france(self):
        result = self.strategy.apply(self.gdf_m)
        lon = result["centroide_lon"].iloc[0]
        lat = result["centroide_lat"].iloc[0]
        assert -5.5 <= lon <= 10.0
        assert 41.0 <= lat <= 52.0

    def test_bbox_coherente(self):
        result = self.strategy.apply(self.gdf_m)
        row = result.iloc[0]
        assert row["bbox_minx"] < row["bbox_maxx"]
        assert row["bbox_miny"] < row["bbox_maxy"]


class TestSimplifyStrategy:

    def test_colonne_creee(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = SimplifyStrategy(0.0001, "EPSG:4326").apply(gdf_m)
        assert "geom_simplifiee_wkt" in result.columns

    def test_wkt_non_vide(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = SimplifyStrategy(0.0001, "EPSG:4326").apply(gdf_m)
        assert result["geom_simplifiee_wkt"].iloc[0] not in ("", None)

    def test_wkt_commence_par_polygon(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = SimplifyStrategy(0.0001, "EPSG:4326").apply(gdf_m)
        assert result["geom_simplifiee_wkt"].iloc[0].startswith("POLYGON")


class TestBufferStrategy:

    def test_colonne_creee(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = BufferStrategy(500, "EPSG:4326").apply(gdf_m)
        assert "buffer_wkt" in result.columns

    def test_buffer_plus_grand_que_source(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        aire_avant = gdf_m.geometry.area.iloc[0]
        result = BufferStrategy(500, "EPSG:4326").apply(gdf_m)
        # Remettre en Lambert pour comparer les aires
        gdf_buffer = gpd.GeoDataFrame(
            geometry=gpd.GeoSeries.from_wkt([result["buffer_wkt"].iloc[0]]),
            crs="EPSG:4326"
        ).to_crs("EPSG:2154")
        assert gdf_buffer.geometry.area.iloc[0] > aire_avant


class TestSpatialJoinStrategy:

    def test_avec_regions(self, regions_ref):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = SpatialJoinStrategy(regions_ref).apply(gdf_m)
        assert "nom_region" in result.columns
        assert "code_region" in result.columns
        # POLYGON_WGS84 est dans la région Île-de-France
        assert result["nom_region"].iloc[0] == "Île-de-France"

    def test_sans_regions(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = SpatialJoinStrategy(None).apply(gdf_m)
        import pandas as pd; assert pd.isna(result["nom_region"].iloc[0]) or result["nom_region"].iloc[0] is None
        assert result["code_region"].iloc[0] is None

    def test_zone_hors_regions(self, regions_ref):
        # Polygon en Norvège — hors des régions françaises
        poly_norvege = "POLYGON((10 60, 11 60, 11 61, 10 61, 10 60))"
        gdf_m = make_gdf(poly_norvege).to_crs("EPSG:2154")
        result = SpatialJoinStrategy(regions_ref).apply(gdf_m)
        # Pas dans les régions de référence → None
        import pandas as pd; assert pd.isna(result["nom_region"].iloc[0]) or result["nom_region"].iloc[0] is None


class TestWktExportStrategy:

    def test_colonne_geom_wkt_creee(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = WktExportStrategy("EPSG:4326").apply(gdf_m)
        assert "geom_wkt" in result.columns

    def test_wkt_valide_parseable(self):
        gdf_m = make_gdf(POLYGON_WGS84).to_crs("EPSG:2154")
        result = WktExportStrategy("EPSG:4326").apply(gdf_m)
        wkt_str = result["geom_wkt"].iloc[0]
        parsed = wkt.loads(wkt_str)  # ne lève pas d'exception
        assert parsed is not None


# =============================================================================
# Tests du GeoTransformer complet
# =============================================================================

class TestGeoTransformer:

    def test_transform_retourne_enriched_records(self, raw_records, regions_ref, cfg):
        transformer = GeoTransformer(cfg.geo, regions_ref)
        result = transformer.transform(raw_records)
        assert len(result) == len(raw_records)

    def test_code_zone_preserve(self, raw_records, cfg):
        transformer = GeoTransformer(cfg.geo)
        result = transformer.transform(raw_records)
        codes_source = {r.code_zone for r in raw_records}
        codes_result = {r.code_zone for r in result}
        assert codes_source == codes_result

    def test_aire_positive_apres_transform(self, raw_records, cfg):
        transformer = GeoTransformer(cfg.geo)
        result = transformer.transform(raw_records)
        assert all(r.aire_m2 > 0 for r in result)

    def test_coherence_aire_km2(self, raw_records, cfg):
        transformer = GeoTransformer(cfg.geo)
        result = transformer.transform(raw_records)
        for r in result:
            assert abs(r.aire_km2 - r.aire_m2 / 1_000_000) < 0.001, \
                f"Incohérence aire_km2 pour {r.code_zone}"

    def test_wkt_non_vides(self, raw_records, cfg):
        transformer = GeoTransformer(cfg.geo)
        result = transformer.transform(raw_records)
        for r in result:
            assert r.geom_wkt, f"geom_wkt vide pour {r.code_zone}"
            assert r.buffer_wkt, f"buffer_wkt vide pour {r.code_zone}"
            assert r.geom_simplifiee_wkt, f"geom_simplifiee_wkt vide pour {r.code_zone}"

    def test_liste_vide_retourne_vide(self, cfg):
        transformer = GeoTransformer(cfg.geo)
        assert transformer.transform([]) == []

    def test_geometrie_invalide_corrigee(self, cfg):
        """Une géométrie invalide dans la source ne doit pas planter le pipeline."""
        from tests.conftest import POLYGON_INVALID
        records = [
            RawGeoRecord(
                code_zone="INV-001", nom_zone="Invalide",
                type_zone=None, population=None,
                geom_wkt=POLYGON_INVALID,
            )
        ]
        transformer = GeoTransformer(cfg.geo)
        result = transformer.transform(records)
        assert len(result) == 1
        assert result[0].aire_m2 >= 0

    def test_spatial_join_assigne_region(self, raw_records, regions_ref, cfg):
        transformer = GeoTransformer(cfg.geo, regions_ref)
        result = transformer.transform(raw_records)
        # Les zones test sont dans la bbox Île-de-France du fixture
        assignees = [r for r in result if r.nom_region is not None]
        assert len(assignees) > 0
