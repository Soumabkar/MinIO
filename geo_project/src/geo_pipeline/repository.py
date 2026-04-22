"""
repository.py — Repository pattern
Isole tout accès aux données Trino derrière une interface claire.
Le reste du code ne connaît pas SQL.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Generator, List

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from .config import AppConfig, TrinoConfig
from .models import RawGeoRecord, EnrichedGeoRecord

log = logging.getLogger(__name__)


# =============================================================================
# Interface abstraite (contrat)
# =============================================================================

class AbstractGeoRepository(ABC):
    """Interface du repository — permet de substituer une implémentation de test."""

    @abstractmethod
    def fetch_source(self) -> List[RawGeoRecord]:
        """Lit tous les enregistrements bruts depuis la table source."""

    @abstractmethod
    def merge_into_dest(self, records: List[EnrichedGeoRecord]) -> MergeStats:
        """MERGE INSERT/UPDATE vers la table destination."""

    @abstractmethod
    def count_dest(self) -> int:
        """Compte les lignes dans la table destination."""

    @abstractmethod
    def fetch_dest_sample(self, limit: int = 5) -> pd.DataFrame:
        """Lit un échantillon de la destination pour validation."""


# =============================================================================
# Statistiques du MERGE
# =============================================================================

class MergeStats:
    """Résultat du MERGE : nombre de lignes insérées et mises à jour."""

    def __init__(self, nb_total_avant: int, nb_total_apres: int):
        self.nb_total_avant = nb_total_avant
        self.nb_total_apres = nb_total_apres
        self.nb_inserts = max(0, nb_total_apres - nb_total_avant)

    def __repr__(self):
        return (f"MergeStats(avant={self.nb_total_avant}, "
                f"après={self.nb_total_apres}, "
                f"inserts={self.nb_inserts})")


# =============================================================================
# Implémentation Trino
# =============================================================================

class TrinoGeoRepository(AbstractGeoRepository):
    """
    Implémentation concrète du repository pour Trino + Iceberg.
    Utilise SQLAlchemy comme couche d'abstraction DBAPI.
    """

    def __init__(self, cfg: AppConfig):
        self._cfg     = cfg
        self._engine  = self._build_engine(cfg.trino)

    # ── Construction du moteur ────────────────────────────────────────────────

    @staticmethod
    def _build_engine(trino_cfg: TrinoConfig) -> Engine:
        try:
            from trino.sqlalchemy import URL as TrinoURL
            url = TrinoURL(
                host        = trino_cfg.host,
                port        = trino_cfg.port,
                user        = trino_cfg.user,
                catalog     = trino_cfg.catalog,
                schema      = trino_cfg.schema,
            )
            engine = sa.create_engine(
                url,
                connect_args={"http_scheme": trino_cfg.http_scheme},
            )
            log.info(f"Moteur Trino : {trino_cfg.host}:{trino_cfg.port}"
                     f"/{trino_cfg.catalog}/{trino_cfg.schema}")
            return engine
        except ImportError:
            raise RuntimeError(
                "Le package 'trino' est requis : pip install trino"
            )

    # ── Context manager connexion ─────────────────────────────────────────────

    @contextmanager
    def _connect(self) -> Generator[Connection, None, None]:
        conn = self._engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            log.exception("Erreur lors de l'exécution Trino")
            raise
        finally:
            conn.close()

    # ── Lecture source ────────────────────────────────────────────────────────

    def fetch_source(self) -> List[RawGeoRecord]:
        """
        Lit la table source et retourne une liste de RawGeoRecord.
        Filtre les géométries nulles ou vides dès la requête SQL.
        """
        geo    = self._cfg.geo
        tables = self._cfg.tables

        sql = text(f"""
            SELECT
                {geo.col_id_metier}      AS code_zone,
                nom_zone,
                type_zone,
                population,
                {geo.col_geom_wkt}       AS geom_wkt
            FROM {tables.source}
            WHERE {geo.col_geom_wkt} IS NOT NULL
              AND TRIM({geo.col_geom_wkt}) != ''
        """)

        with self._connect() as conn:
            df = pd.read_sql(sql, conn)

        log.info(f"Source lue : {len(df)} lignes depuis '{tables.source}'")

        records = []
        for _, row in df.iterrows():
            try:
                records.append(RawGeoRecord(
                    code_zone  = str(row["code_zone"]),
                    nom_zone   = row.get("nom_zone"),
                    type_zone  = row.get("type_zone"),
                    population = int(row["population"]) if pd.notna(row.get("population")) else None,
                    geom_wkt   = str(row["geom_wkt"]),
                ))
            except ValueError as e:
                log.warning(f"Ligne ignorée ({row.get('code_zone')}) : {e}")

        return records

    # ── MERGE vers destination ────────────────────────────────────────────────

    def merge_into_dest(self, records: List[EnrichedGeoRecord]) -> MergeStats:
        """
        MERGE direct source_enrichie → dest_geo.
        Construit le USING depuis les records Python (pas de table intermédiaire).
        Traite les records par batch pour éviter les requêtes SQL trop grandes.
        """
        if not records:
            log.warning("Aucun enregistrement à merger")
            return MergeStats(0, 0)

        nb_avant = self.count_dest()
        table    = self._cfg.tables.dest
        batch_sz = self._cfg.load.batch_size

        with self._connect() as conn:
            for start in range(0, len(records), batch_sz):
                batch = records[start:start + batch_sz]
                sql   = self._build_merge_sql(table, batch)
                conn.execute(text(sql))
                log.info(f"  Batch mergé : {start + len(batch)}/{len(records)} lignes")

        nb_apres = self.count_dest()
        stats    = MergeStats(nb_avant, nb_apres)
        log.info(f"MERGE terminé : {stats}")
        return stats

    def _build_merge_sql(self, table: str, records: List[EnrichedGeoRecord]) -> str:
        """
        Construit le SQL MERGE avec VALUES inline (pas de table staging).
        Chaque record devient une ligne dans la clause USING (VALUES ...).
        """
        col_id = self._cfg.geo.col_id_metier

        # Construire les lignes VALUES
        values_rows = ",\n            ".join(
            self._record_to_values(r) for r in records
        )

        return f"""
            MERGE INTO {table} AS tgt
            USING (
                SELECT
                    code_zone,
                    nom_zone,
                    type_zone,
                    CAST(population AS BIGINT)    AS population,
                    geom_wkt,
                    geom_simplifiee_wkt,
                    buffer_wkt,
                    CAST(centroide_lon AS DOUBLE)  AS centroide_lon,
                    CAST(centroide_lat AS DOUBLE)  AS centroide_lat,
                    CAST(aire_m2      AS DOUBLE)   AS aire_m2,
                    CAST(perimetre_m  AS DOUBLE)   AS perimetre_m,
                    CAST(aire_km2     AS DOUBLE)   AS aire_km2,
                    CAST(bbox_minx    AS DOUBLE)   AS bbox_minx,
                    CAST(bbox_miny    AS DOUBLE)   AS bbox_miny,
                    CAST(bbox_maxx    AS DOUBLE)   AS bbox_maxx,
                    CAST(bbox_maxy    AS DOUBLE)   AS bbox_maxy,
                    nom_region,
                    code_region,
                    crs_calcul,
                    crs_stockage
                FROM (
                    VALUES
                        {values_rows}
                ) AS v (
                    code_zone, nom_zone, type_zone, population,
                    geom_wkt, geom_simplifiee_wkt, buffer_wkt,
                    centroide_lon, centroide_lat,
                    aire_m2, perimetre_m, aire_km2,
                    bbox_minx, bbox_miny, bbox_maxx, bbox_maxy,
                    nom_region, code_region, crs_calcul, crs_stockage
                )
            ) AS src

            ON (tgt.{col_id} = src.{col_id})

            WHEN MATCHED
              AND (
                  tgt.geom_wkt            IS DISTINCT FROM src.geom_wkt
               OR tgt.geom_simplifiee_wkt IS DISTINCT FROM src.geom_simplifiee_wkt
               OR tgt.buffer_wkt          IS DISTINCT FROM src.buffer_wkt
               OR tgt.centroide_lon       IS DISTINCT FROM src.centroide_lon
               OR tgt.centroide_lat       IS DISTINCT FROM src.centroide_lat
               OR tgt.aire_m2             IS DISTINCT FROM src.aire_m2
               OR tgt.perimetre_m         IS DISTINCT FROM src.perimetre_m
               OR tgt.population          IS DISTINCT FROM src.population
               OR tgt.nom_region          IS DISTINCT FROM src.nom_region
              )
            THEN UPDATE SET
                nom_zone            = src.nom_zone,
                type_zone           = src.type_zone,
                population          = src.population,
                geom_wkt            = src.geom_wkt,
                geom_simplifiee_wkt = src.geom_simplifiee_wkt,
                buffer_wkt          = src.buffer_wkt,
                centroide_lon       = src.centroide_lon,
                centroide_lat       = src.centroide_lat,
                aire_m2             = src.aire_m2,
                perimetre_m         = src.perimetre_m,
                aire_km2            = src.aire_km2,
                bbox_minx           = src.bbox_minx,
                bbox_miny           = src.bbox_miny,
                bbox_maxx           = src.bbox_maxx,
                bbox_maxy           = src.bbox_maxy,
                nom_region          = src.nom_region,
                code_region         = src.code_region,
                crs_stockage        = src.crs_stockage

            WHEN NOT MATCHED
            THEN INSERT (
                dest_geo_pk,
                {col_id},
                nom_zone, type_zone, population,
                geom_wkt, geom_simplifiee_wkt, buffer_wkt,
                centroide_lon, centroide_lat,
                aire_m2, perimetre_m, aire_km2,
                bbox_minx, bbox_miny, bbox_maxx, bbox_maxy,
                nom_region, code_region, crs_calcul, crs_stockage
            )
            VALUES (
                uuid(),
                src.code_zone,
                src.nom_zone, src.type_zone, src.population,
                src.geom_wkt, src.geom_simplifiee_wkt, src.buffer_wkt,
                src.centroide_lon, src.centroide_lat,
                src.aire_m2, src.perimetre_m, src.aire_km2,
                src.bbox_minx, src.bbox_miny, src.bbox_maxx, src.bbox_maxy,
                src.nom_region, src.code_region,
                src.crs_calcul, src.crs_stockage
            )
        """

    @staticmethod
    def _record_to_values(r: EnrichedGeoRecord) -> str:
        """Sérialise un EnrichedGeoRecord en tuple SQL VALUES."""
        def s(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return str(v)
            return "'" + str(v).replace("'", "''") + "'"

        return (
            f"({s(r.code_zone)}, {s(r.nom_zone)}, {s(r.type_zone)}, "
            f"{s(r.population)}, "
            f"{s(r.geom_wkt)}, {s(r.geom_simplifiee_wkt)}, {s(r.buffer_wkt)}, "
            f"{s(r.centroide_lon)}, {s(r.centroide_lat)}, "
            f"{s(r.aire_m2)}, {s(r.perimetre_m)}, {s(r.aire_km2)}, "
            f"{s(r.bbox_minx)}, {s(r.bbox_miny)}, "
            f"{s(r.bbox_maxx)}, {s(r.bbox_maxy)}, "
            f"{s(r.nom_region)}, {s(r.code_region)}, "
            f"{s(r.crs_calcul)}, {s(r.crs_stockage)})"
        )

    # ── Helpers de validation ─────────────────────────────────────────────────

    def count_dest(self) -> int:
        with self._connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {self._cfg.tables.dest}")
            ).fetchone()
            return result[0] if result else 0

    def fetch_dest_sample(self, limit: int = 5) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql(
                text(f"SELECT * FROM {self._cfg.tables.dest} LIMIT {limit}"),
                conn,
            )

    def dispose(self):
        self._engine.dispose()
