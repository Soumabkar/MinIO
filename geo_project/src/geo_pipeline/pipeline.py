"""
pipeline.py — Façade du pipeline géospatial
Orchestre les 4 étapes sans table intermédiaire :
  source → transformer → validator → repository (MERGE direct)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import geopandas as gpd
from shapely import wkt

from .config import AppConfig
from .models import EnrichedGeoRecord
from .repository import AbstractGeoRepository, MergeStats, TrinoGeoRepository
from .transformer import GeoTransformer
from .validator import SourceValidator, TransformValidator, ValidationReport

log = logging.getLogger(__name__)


# =============================================================================
# Résultat du pipeline
# =============================================================================

@dataclass
class PipelineResult:
    source_count:      int
    enriched_count:    int
    merge_stats:       MergeStats
    source_validation: ValidationReport
    transform_validation: ValidationReport

    @property
    def success(self) -> bool:
        return (
            self.source_validation.passed
            and self.transform_validation.passed
            and self.enriched_count == self.source_count
        )

    def print_summary(self):
        log.info(self.source_validation.summary())
        log.info(self.transform_validation.summary())
        log.info("\n" + "═" * 55)
        log.info("  RÉSUMÉ PIPELINE")
        log.info(f"  Source lue       : {self.source_count} enregistrements")
        log.info(f"  Enrichis         : {self.enriched_count} enregistrements")
        log.info(f"  MERGE résultat   : {self.merge_stats}")
        log.info(f"  Statut global    : {'✓ SUCCÈS' if self.success else '✗ ECHEC'}")
        log.info("═" * 55)


# =============================================================================
# Façade principale
# =============================================================================

class GeoPipeline:
    """
    Façade qui orchestre le pipeline complet.
    Découple l'appelant des détails d'implémentation de chaque composant.
    """

    def __init__(
        self,
        cfg:        AppConfig,
        repository: Optional[AbstractGeoRepository] = None,
        regions_ref: Optional[gpd.GeoDataFrame]     = None,
    ):
        self._cfg         = cfg
        self._repository  = repository or TrinoGeoRepository(cfg)
        self._transformer = GeoTransformer(cfg.geo, regions_ref)
        self._src_validator  = SourceValidator()
        self._trf_validator  = TransformValidator(cfg.geo.bbox_france)

    def run(self) -> PipelineResult:
        """
        Exécute le pipeline complet :
          [1] Lecture source
          [2] Validation source
          [3] Transformation géo
          [4] Validation post-transformation
          [5] MERGE direct vers dest (sans table intermédiaire)
        """
        log.info("═" * 55)
        log.info("  Pipeline Géospatial — démarrage")
        log.info("═" * 55)

        # ── [1] Lecture ───────────────────────────────────────────────────────
        log.info("[1/5] Lecture source...")
        raw_records = self._repository.fetch_source()

        # ── [2] Validation source ─────────────────────────────────────────────
        log.info("[2/5] Validation source...")
        src_report = self._src_validator.validate(raw_records)
        if not src_report.passed:
            log.error("Validation source échouée — pipeline interrompu")
            raise ValueError(src_report.summary())

        # ── [3] Transformation ────────────────────────────────────────────────
        log.info("[3/5] Transformation géospatiale...")
        enriched = self._transformer.transform(raw_records)

        # ── [4] Validation post-transformation ────────────────────────────────
        log.info("[4/5] Validation post-transformation...")
        trf_report = self._trf_validator.validate(enriched, len(raw_records))
        if not trf_report.passed:
            log.error("Validation transformation échouée — pipeline interrompu")
            raise ValueError(trf_report.summary())

        # ── [5] MERGE direct → destination ────────────────────────────────────
        log.info("[5/5] MERGE vers destination (sans table intermédiaire)...")
        merge_stats = self._repository.merge_into_dest(enriched)

        result = PipelineResult(
            source_count         = len(raw_records),
            enriched_count       = len(enriched),
            merge_stats          = merge_stats,
            source_validation    = src_report,
            transform_validation = trf_report,
        )
        result.print_summary()
        return result

    def dispose(self):
        if hasattr(self._repository, "dispose"):
            self._repository.dispose()


# =============================================================================
# Point d'entrée
# =============================================================================

def run_pipeline(cfg: Optional[AppConfig] = None) -> PipelineResult:
    """Lance le pipeline avec la configuration par défaut ou fournie."""
    cfg = cfg or AppConfig.from_env()

    regions_ref = _load_regions_france()

    pipeline = GeoPipeline(cfg=cfg, regions_ref=regions_ref)
    try:
        return pipeline.run()
    finally:
        pipeline.dispose()


def _load_regions_france() -> Optional[gpd.GeoDataFrame]:
    """Charge un référentiel minimal de régions françaises pour le spatial join."""
    try:
        data = {
            "nom_region":  ["Île-de-France", "Auvergne-Rhône-Alpes", "Occitanie",
                             "Provence-Alpes-Côte d'Azur", "Grand Est"],
            "code_region": ["11", "84", "76", "93", "44"],
            "geometry": [
                wkt.loads("POLYGON((1.5 48.0, 3.5 48.0, 3.5 49.5, 1.5 49.5, 1.5 48.0))"),
                wkt.loads("POLYGON((2.0 44.0, 7.5 44.0, 7.5 47.0, 2.0 47.0, 2.0 44.0))"),
                wkt.loads("POLYGON((-0.5 42.3, 4.5 42.3, 4.5 45.0, -0.5 45.0, -0.5 42.3))"),
                wkt.loads("POLYGON((4.5 43.0, 7.8 43.0, 7.8 45.0, 4.5 45.0, 4.5 43.0))"),
                wkt.loads("POLYGON((5.0 47.0, 8.5 47.0, 8.5 49.5, 5.0 49.5, 5.0 47.0))"),
            ],
        }
        return gpd.GeoDataFrame(data, crs="EPSG:4326")
    except Exception as e:
        log.warning(f"Référentiel régions non chargé : {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    run_pipeline()
