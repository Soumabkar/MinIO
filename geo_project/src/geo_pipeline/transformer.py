"""
transformer.py — Strategy pattern pour les transformations géospatiales
Chaque transformation est encapsulée dans une classe avec une interface commune.
Le GeoTransformer orchestre l'application des stratégies dans l'ordre.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Tuple

import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import Point
from shapely.validation import make_valid

from .config import GeoConfig
from .models import RawGeoRecord, EnrichedGeoRecord

log = logging.getLogger(__name__)


# =============================================================================
# Interface Strategy
# =============================================================================

class GeoTransformStrategy(ABC):
    """Interface commune à toutes les stratégies de transformation."""

    @abstractmethod
    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Applique la transformation et retourne un nouveau GeoDataFrame."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Nom lisible de la transformation (pour les logs)."""


# =============================================================================
# Stratégies concrètes
# =============================================================================

class ReprojectionStrategy(GeoTransformStrategy):
    """Reprojette le GeoDataFrame vers le CRS métrique pour les calculs."""

    def __init__(self, crs_target: str):
        self._crs = crs_target

    @property
    def name(self) -> str:
        return f"Reprojection → {self._crs}"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gdf.to_crs(self._crs)


class GeometryValidationStrategy(GeoTransformStrategy):
    """Corrige les géométries invalides (self-intersections, etc.)."""

    @property
    def name(self) -> str:
        return "Validation géométries"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        invalides = ~gdf.geometry.is_valid
        nb = invalides.sum()
        if nb > 0:
            log.warning(f"  {nb} géométrie(s) invalide(s) → correction make_valid()")
            gdf = gdf.copy()
            gdf.geometry = gdf.geometry.apply(
                lambda g: make_valid(g) if g is not None and not g.is_valid else g
            )
        return gdf


class MetricsStrategy(GeoTransformStrategy):
    """Calcule aire, périmètre, centroïde et bounding box."""

    def __init__(self, crs_source: str):
        self._crs_source = crs_source  # CRS WGS84 pour reconvertir les centroïdes

    @property
    def name(self) -> str:
        return "Calcul métriques (aire, périmètre, centroïde, bbox)"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf = gdf.copy()

        # Aire et périmètre (en unités du CRS courant — mètres si Lambert-93)
        gdf["aire_m2"]     = gdf.geometry.area.round(2)
        gdf["aire_km2"]    = (gdf["aire_m2"] / 1_000_000).round(6)
        gdf["perimetre_m"] = gdf.geometry.length.round(2)

        # Centroïdes reconvertis en WGS84
        centroides_wgs = gdf.geometry.centroid.to_crs(self._crs_source)
        gdf["centroide_lon"] = centroides_wgs.x.round(8)
        gdf["centroide_lat"] = centroides_wgs.y.round(8)

        # Bounding box
        bounds = gdf.geometry.bounds
        gdf["bbox_minx"] = bounds["minx"].round(4)
        gdf["bbox_miny"] = bounds["miny"].round(4)
        gdf["bbox_maxx"] = bounds["maxx"].round(4)
        gdf["bbox_maxy"] = bounds["maxy"].round(4)

        return gdf


class SimplifyStrategy(GeoTransformStrategy):
    """Simplifie la géométrie en préservant la topologie."""

    def __init__(self, tolerance: float, crs_source: str):
        self._tolerance  = tolerance
        self._crs_source = crs_source

    @property
    def name(self) -> str:
        return f"Simplification (tolérance={self._tolerance}°)"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        # Simplification en WGS84 (tolérance en degrés)
        gdf_wgs = gdf.to_crs(self._crs_source)
        gdf["geom_simplifiee_wkt"] = (
            gdf_wgs.geometry
            .simplify(self._tolerance, preserve_topology=True)
            .to_wkt()
        )
        return gdf


class BufferStrategy(GeoTransformStrategy):
    """Calcule une zone tampon (buffer) autour de chaque géométrie."""

    def __init__(self, buffer_metres: float, crs_source: str):
        self._buffer  = buffer_metres
        self._crs_source = crs_source

    @property
    def name(self) -> str:
        return f"Buffer ({self._buffer} m)"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        # Buffer en coordonnées métriques → retour WGS84 pour stockage
        gdf["buffer_wkt"] = (
            gdf.geometry.buffer(self._buffer)
            .to_crs(self._crs_source)
            .to_wkt()
        )
        return gdf


class SpatialJoinStrategy(GeoTransformStrategy):
    """Détecte l'appartenance de chaque zone à une région de référence."""

    def __init__(self, regions_ref: Optional[gpd.GeoDataFrame]):
        self._regions = regions_ref

    @property
    def name(self) -> str:
        return "Spatial join régions"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if self._regions is None or self._regions.empty:
            gdf = gdf.copy()
            gdf["nom_region"]  = None
            gdf["code_region"] = None
            log.info("  Spatial join ignoré (pas de référentiel régions)")
            return gdf

        regions_m = self._regions.to_crs(gdf.crs)
        joined    = gpd.sjoin(
            gdf,
            regions_m[["nom_region", "code_region", "geometry"]],
            how       = "left",
            predicate = "intersects",
        )
        # Dédupliquer si une zone intersecte plusieurs régions
        joined    = joined.drop_duplicates(subset=["code_zone"], keep="first")
        gdf       = gdf.copy()
        gdf["nom_region"]  = joined["nom_region"].values
        gdf["code_region"] = joined["code_region"].values
        log.info(f"  Spatial join : {gdf['nom_region'].notna().sum()} zones assignées")
        return gdf


class WktExportStrategy(GeoTransformStrategy):
    """Convertit la géométrie principale en WKT pour stockage en VARCHAR."""

    def __init__(self, crs_output: str):
        self._crs_output = crs_output

    @property
    def name(self) -> str:
        return f"Export WKT ({self._crs_output})"

    def apply(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        gdf["geom_wkt"] = gdf.to_crs(self._crs_output).geometry.to_wkt()
        return gdf


# =============================================================================
# Orchestrateur — applique les stratégies dans l'ordre
# =============================================================================

class GeoTransformer:
    """
    Orchestre les transformations géospatiales via le pattern Strategy.
    Les stratégies sont définies une fois et appliquées séquentiellement.
    """

    def __init__(self, cfg: GeoConfig,
                 regions_ref: Optional[gpd.GeoDataFrame] = None):
        self._cfg = cfg

        # Ordre fixe des transformations
        self._strategies: List[GeoTransformStrategy] = [
            GeometryValidationStrategy(),
            ReprojectionStrategy(cfg.crs_metric),
            MetricsStrategy(cfg.crs_source),
            SimplifyStrategy(cfg.simplify_tolerance, cfg.crs_source),
            BufferStrategy(cfg.buffer_metres, cfg.crs_source),
            SpatialJoinStrategy(regions_ref),
            WktExportStrategy(cfg.crs_source),
        ]

    def transform(self, records: List[RawGeoRecord]) -> List[EnrichedGeoRecord]:
        """
        Point d'entrée principal :
          [1] RawGeoRecord → GeoDataFrame
          [2] Application de chaque stratégie
          [3] GeoDataFrame → List[EnrichedGeoRecord]
        """
        if not records:
            return []

        log.info(f"Transformation de {len(records)} enregistrements...")

        # [1] Construire le GeoDataFrame source
        gdf = self._to_geodataframe(records)

        # [2] Appliquer les stratégies dans l'ordre
        for strategy in self._strategies:
            log.info(f"  → {strategy.name}")
            gdf = strategy.apply(gdf)

        # [3] Convertir en modèles de domaine enrichis
        enriched = self._to_enriched_records(gdf)
        log.info(f"Transformation terminée : {len(enriched)} enregistrements enrichis")
        return enriched

    # ── Helpers privés ────────────────────────────────────────────────────────

    def _to_geodataframe(self, records: List[RawGeoRecord]) -> gpd.GeoDataFrame:
        """Construit un GeoDataFrame à partir des enregistrements bruts."""
        rows = []
        for r in records:
            try:
                geom = wkt.loads(r.geom_wkt)
                rows.append({
                    "code_zone":  r.code_zone,
                    "nom_zone":   r.nom_zone,
                    "type_zone":  r.type_zone,
                    "population": r.population,
                    "geometry":   geom,
                })
            except Exception as e:
                log.warning(f"  Géométrie ignorée ({r.code_zone}) : {e}")

        if not rows:
            raise ValueError("Aucune géométrie valide dans les enregistrements source")

        df = pd.DataFrame(rows)
        return gpd.GeoDataFrame(df, geometry="geometry", crs=self._cfg.crs_source)

    @staticmethod
    def _to_enriched_records(gdf: gpd.GeoDataFrame) -> List[EnrichedGeoRecord]:
        """Convertit le GeoDataFrame final en liste de EnrichedGeoRecord."""
        records = []
        for _, row in gdf.iterrows():

            def val(col, default=None):
                v = row.get(col, default)
                return None if (v is None or (isinstance(v, float) and pd.isna(v))) else v

            try:
                records.append(EnrichedGeoRecord(
                    code_zone            = str(row["code_zone"]),
                    nom_zone             = val("nom_zone"),
                    type_zone            = val("type_zone"),
                    population           = int(val("population")) if val("population") is not None else None,
                    geom_wkt             = str(row["geom_wkt"]),
                    geom_simplifiee_wkt  = str(row["geom_simplifiee_wkt"]),
                    buffer_wkt           = str(row["buffer_wkt"]),
                    centroide_lon        = float(row["centroide_lon"]),
                    centroide_lat        = float(row["centroide_lat"]),
                    aire_m2              = float(row["aire_m2"]),
                    perimetre_m          = float(row["perimetre_m"]),
                    aire_km2             = float(row["aire_km2"]),
                    bbox_minx            = float(row["bbox_minx"]),
                    bbox_miny            = float(row["bbox_miny"]),
                    bbox_maxx            = float(row["bbox_maxx"]),
                    bbox_maxy            = float(row["bbox_maxy"]),
                    nom_region           = val("nom_region"),
                    code_region          = val("code_region"),
                ))
            except Exception as e:
                log.warning(f"  EnrichedRecord ignoré ({row.get('code_zone')}) : {e}")

        return records
