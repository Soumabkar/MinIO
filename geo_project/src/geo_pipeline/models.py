"""
models.py — Modèles du domaine (Value Objects immuables)
Représentent les entités géospatiales avant et après transformation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class RawGeoRecord:
    """
    Enregistrement brut lu depuis la table source.
    Contient la géométrie en WKT non encore traitée.
    """
    code_zone:  str
    nom_zone:   Optional[str]
    type_zone:  Optional[str]
    population: Optional[int]
    geom_wkt:   str            # WKT brut : 'POLYGON((...))'

    def __post_init__(self):
        if not self.code_zone:
            raise ValueError("code_zone ne peut pas être vide")
        if not self.geom_wkt:
            raise ValueError(f"geom_wkt vide pour {self.code_zone}")


@dataclass(frozen=True)
class EnrichedGeoRecord:
    """
    Enregistrement enrichi après traitement géospatial.
    Prêt pour l'insertion dans dest_geo (pas de table intermédiaire).
    Tous les champs calculés sont immuables une fois construits.
    """
    # Clé métier
    code_zone:  str

    # Attributs descriptifs
    nom_zone:   Optional[str]
    type_zone:  Optional[str]
    population: Optional[int]

    # Géométries (WKT, stockées en VARCHAR dans Iceberg)
    geom_wkt:             str           # géométrie principale WGS84
    geom_simplifiee_wkt:  str           # géométrie simplifiée
    buffer_wkt:           str           # zone tampon

    # Métriques calculées en projection métrique
    centroide_lon:  float
    centroide_lat:  float
    aire_m2:        float
    perimetre_m:    float
    aire_km2:       float

    # Bounding box
    bbox_minx:  float
    bbox_miny:  float
    bbox_maxx:  float
    bbox_maxy:  float

    # Spatial join
    nom_region:   Optional[str] = None
    code_region:  Optional[str] = None

    # Métadonnées CRS
    crs_calcul:   str = "EPSG:2154"
    crs_stockage: str = "EPSG:4326"

    def is_in_bbox(self, lon_min: float, lat_min: float,
                   lon_max: float, lat_max: float) -> bool:
        """Vérifie que le centroïde est dans la bounding box donnée."""
        return (lon_min <= self.centroide_lon <= lon_max and
                lat_min <= self.centroide_lat <= lat_max)
