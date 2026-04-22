"""
test_pipeline_integration.py
Tests d'intégration du pipeline complet avec un repository en mémoire.
Aucune connexion réseau requise — Trino est remplacé par InMemoryGeoRepository.
"""
import pytest
from tests.conftest import InMemoryGeoRepository
from src.geo_pipeline.pipeline import GeoPipeline
from src.geo_pipeline.models import RawGeoRecord


# =============================================================================
# Tests d'intégration pipeline complet
# =============================================================================

class TestGeoPipelineIntegration:

    def _make_pipeline(self, raw_records, cfg, regions_ref=None):
        repo = InMemoryGeoRepository(source_data=raw_records)
        return GeoPipeline(cfg=cfg, repository=repo, regions_ref=regions_ref), repo

    # ── Cas nominaux ─────────────────────────────────────────────────────────

    def test_pipeline_succes_complet(self, raw_records, cfg, regions_ref):
        pipeline, repo = self._make_pipeline(raw_records, cfg, regions_ref)
        result = pipeline.run()

        assert result.success
        assert result.source_count == len(raw_records)
        assert result.enriched_count == len(raw_records)
        assert repo.count_dest() == len(raw_records)

    def test_toutes_zones_inserees(self, raw_records, cfg):
        pipeline, repo = self._make_pipeline(raw_records, cfg)
        pipeline.run()

        codes_source = {r.code_zone for r in raw_records}
        codes_dest   = {r.code_zone for r in repo.all_records()}
        assert codes_source == codes_dest

    def test_metriques_calculees_en_dest(self, raw_records, cfg):
        pipeline, repo = self._make_pipeline(raw_records, cfg)
        pipeline.run()

        for record in repo.all_records():
            assert record.aire_m2 > 0,      f"Aire nulle : {record.code_zone}"
            assert record.perimetre_m > 0,   f"Périmètre nul : {record.code_zone}"
            assert record.aire_km2 > 0,      f"aire_km2 nul : {record.code_zone}"
            assert record.geom_wkt,          f"geom_wkt vide : {record.code_zone}"
            assert record.buffer_wkt,        f"buffer_wkt vide : {record.code_zone}"
            assert record.geom_simplifiee_wkt, f"geom_simplifiee_wkt vide"

    def test_coherence_aire_km2(self, raw_records, cfg):
        pipeline, _ = self._make_pipeline(raw_records, cfg)
        pipeline.run()
        pipeline_2, repo = self._make_pipeline(raw_records, cfg)
        pipeline_2.run()

        for r in repo.all_records():
            ecart = abs(r.aire_km2 - r.aire_m2 / 1_000_000)
            assert ecart < 0.001, f"Incohérence aire {r.code_zone}: {ecart}"

    def test_centroide_dans_bbox_france(self, raw_records, cfg):
        pipeline, repo = self._make_pipeline(raw_records, cfg)
        pipeline.run()

        for r in repo.all_records():
            assert r.is_in_bbox(-5.5, 41.0, 10.0, 52.0), \
                f"Centroïde hors France : {r.code_zone} ({r.centroide_lon}, {r.centroide_lat})"

    def test_wkt_parseable_apres_pipeline(self, raw_records, cfg):
        """Les WKT stockés doivent être parseable par Shapely."""
        from shapely import wkt
        pipeline, repo = self._make_pipeline(raw_records, cfg)
        pipeline.run()

        for r in repo.all_records():
            parsed = wkt.loads(r.geom_wkt)
            assert parsed is not None
            parsed_buf = wkt.loads(r.buffer_wkt)
            assert parsed_buf is not None

    # ── Idempotence ──────────────────────────────────────────────────────────

    def test_idempotence_meme_data(self, raw_records, cfg):
        """Rejouer le pipeline avec les mêmes données → même nombre de lignes."""
        repo = InMemoryGeoRepository(source_data=raw_records)
        pipeline = GeoPipeline(cfg=cfg, repository=repo)

        pipeline.run()
        count_apres_1 = repo.count_dest()

        pipeline.run()   # second passage
        count_apres_2 = repo.count_dest()

        assert count_apres_1 == count_apres_2, \
            f"Idempotence échouée : {count_apres_1} → {count_apres_2}"

    def test_update_si_population_change(self, raw_records, cfg):
        """Modifier la population en source → le record dest doit être mis à jour."""
        repo = InMemoryGeoRepository(source_data=raw_records)
        GeoPipeline(cfg=cfg, repository=repo).run()

        pop_avant = repo.get_record("ZN-002").population

        # Modifier la source
        updated = [
            r if r.code_zone != "ZN-002"
            else RawGeoRecord(
                code_zone=r.code_zone, nom_zone=r.nom_zone,
                type_zone=r.type_zone, population=99999,
                geom_wkt=r.geom_wkt,
            )
            for r in raw_records
        ]
        repo2 = InMemoryGeoRepository(source_data=updated)
        # Pré-peupler repo2 avec le premier passage
        repo2._dest = dict(repo._dest)

        GeoPipeline(cfg=cfg, repository=repo2).run()
        pop_apres = repo2.get_record("ZN-002").population

        assert pop_apres == 99999
        assert pop_avant != pop_apres

    def test_nouvelle_zone_insertee(self, raw_records, cfg):
        """Ajouter une zone dans la source → elle doit apparaître en dest."""
        repo = InMemoryGeoRepository(source_data=raw_records)
        GeoPipeline(cfg=cfg, repository=repo).run()

        count_avant = repo.count_dest()

        nouvelle_zone = RawGeoRecord(
            code_zone="ZN-NEW", nom_zone="Nouvelle Zone",
            type_zone="naturel", population=0,
            geom_wkt="POLYGON((2.0 48.5, 2.1 48.5, 2.1 48.6, 2.0 48.6, 2.0 48.5))",
        )
        augmented = raw_records + [nouvelle_zone]
        repo2 = InMemoryGeoRepository(source_data=augmented)
        repo2._dest = dict(repo._dest)

        GeoPipeline(cfg=cfg, repository=repo2).run()

        assert repo2.count_dest() == count_avant + 1
        assert repo2.get_record("ZN-NEW") is not None

    # ── Cas limites ──────────────────────────────────────────────────────────

    def test_source_vide_leve_erreur(self, cfg):
        repo = InMemoryGeoRepository(source_data=[])
        pipeline = GeoPipeline(cfg=cfg, repository=repo)
        with pytest.raises(ValueError):
            pipeline.run()

    def test_region_assignee_si_ref_fournie(self, raw_records, cfg, regions_ref):
        pipeline, repo = self._make_pipeline(raw_records, cfg, regions_ref)
        pipeline.run()

        avec_region = [r for r in repo.all_records() if r.nom_region is not None]
        assert len(avec_region) > 0, "Aucune zone n'a reçu de région"

    def test_sans_regions_ref_ne_plante_pas(self, raw_records, cfg):
        pipeline, repo = self._make_pipeline(raw_records, cfg, regions_ref=None)
        result = pipeline.run()
        assert result.success

    def test_geometrie_invalide_corrigee_par_transformer(self, cfg):
        """La stratégie GeometryValidation doit corriger sans planter."""
        from tests.conftest import POLYGON_INVALID
        from src.geo_pipeline.transformer import GeometryValidationStrategy
        import geopandas as gpd
        from shapely import wkt as shp_wkt
        gdf = gpd.GeoDataFrame(
            {"code_zone": ["INV"]},
            geometry=[shp_wkt.loads(POLYGON_INVALID)],
            crs="EPSG:4326",
        )
        assert not gdf.geometry.is_valid.all()
        result = GeometryValidationStrategy().apply(gdf)
        assert result.geometry.is_valid.all()


# =============================================================================
# Tests du repository in-memory
# =============================================================================

class TestInMemoryRepository:

    def test_fetch_source_retourne_records(self, raw_records, in_memory_repo):
        result = in_memory_repo.fetch_source()
        assert len(result) == len(raw_records)

    def test_merge_insert_ajoute_lignes(self, enriched_records, in_memory_repo):
        stats = in_memory_repo.merge_into_dest(enriched_records)
        assert in_memory_repo.count_dest() == len(enriched_records)
        assert stats.nb_inserts == len(enriched_records)

    def test_merge_update_ne_duplique_pas(self, enriched_records, in_memory_repo):
        in_memory_repo.merge_into_dest(enriched_records)
        count_1 = in_memory_repo.count_dest()
        in_memory_repo.merge_into_dest(enriched_records)   # 2e passage
        count_2 = in_memory_repo.count_dest()
        assert count_1 == count_2

    def test_get_record_retrouve_zone(self, enriched_records, in_memory_repo):
        in_memory_repo.merge_into_dest(enriched_records)
        r = in_memory_repo.get_record("ZN-001")
        assert r is not None
        assert r.code_zone == "ZN-001"

    def test_count_dest_initial_zero(self, in_memory_repo):
        assert in_memory_repo.count_dest() == 0
