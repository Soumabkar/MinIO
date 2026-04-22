"""
geo_pipeline.py
===============
Pipeline géospatial complet :

  [1] Lecture des données brutes depuis Trino (table source)
  [2] Traitement géospatial avec GeoPandas + Shapely
      - Reprojection CRS
      - Calcul centroïde, aire, périmètre
      - Simplification géométrie
      - Détection points dans polygone (spatial join)
      - Buffer zones
  [3] Écriture du résultat dans une table intermédiaire via SQLAlchemy
  [4] MERGE Trino : source_geo_enrichie → dest_geo (INSERT/UPDATE)
  [5] Tests de validation post-chargement

Dépendances :
    pip install geopandas shapely sqlalchemy trino pyarrow pandas
"""

import logging
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb
from shapely.geometry import Point, Polygon, mapping
from shapely.ops import unary_union
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy import text, event
from sqlalchemy.engine import Engine
from trino.sqlalchemy import URL as TrinoURL

warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("geo_pipeline")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """Paramètres de connexion et noms de tables."""

    # Connexion Trino
    trino_host:     str = "localhost"
    trino_port:     int = 8080
    trino_user:     str = "admin"
    trino_catalog:  str = "iceberg"
    trino_schema:   str = "geo_schema"

    # Tables
    table_source:       str = "source_geo"          # table d'entrée brute
    table_staging:      str = "source_geo_enrichie" # résultat après traitement géo
    table_dest:         str = "dest_geo"            # table finale

    # CRS
    crs_source:  str = "EPSG:4326"   # WGS84 — coordonnées GPS
    crs_calcul:  str = "EPSG:2154"   # Lambert-93 — pour surfaces en m²
    crs_sortie:  str = "EPSG:4326"   # WGS84 — pour le stockage final

    # Géo
    buffer_metres:      float = 500.0    # rayon du buffer en mètres
    simplify_tolerance: float = 0.0001  # tolérance simplification (degrés)

    # Colonne géométrie dans la source
    col_geometry:  str = "geom_wkt"    # WKT ou WKB selon ta source
    col_id_metier: str = "code_zone"   # clé métier de déduplication


cfg = Config()


# =============================================================================
# MOTEURS DE BASE DE DONNÉES
# =============================================================================

def make_trino_engine() -> Engine:
    """Crée le moteur SQLAlchemy→Trino."""
    url = TrinoURL(
        host     = cfg.trino_host,
        port     = cfg.trino_port,
        user     = cfg.trino_user,
        catalog  = cfg.trino_catalog,
        schema   = cfg.trino_schema,
    )
    engine = sa.create_engine(
        url,
        connect_args={"http_scheme": "http"},
    )
    log.info(f"Moteur Trino créé : {cfg.trino_host}:{cfg.trino_port}")
    return engine


@contextmanager
def trino_connection(engine: Engine):
    """Context manager pour connexion Trino avec gestion d'erreur."""
    conn = engine.connect()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        log.error(f"Erreur Trino : {e}")
        raise
    finally:
        conn.close()


# =============================================================================
# ÉTAPE 1 — LECTURE DEPUIS TRINO
# =============================================================================

def lire_source(engine: Engine) -> gpd.GeoDataFrame:
    """
    Lit la table source depuis Trino et construit un GeoDataFrame.
    La géométrie est stockée en WKT dans la colonne cfg.col_geometry.
    """
    log.info(f"[1] Lecture source : {cfg.table_source}")

    sql = f"""
        SELECT
            {cfg.col_id_metier},
            nom_zone,
            type_zone,
            population,
            {cfg.col_geometry}          -- WKT : 'POLYGON((...))'
        FROM {cfg.table_source}
        WHERE {cfg.col_geometry} IS NOT NULL
          AND {cfg.col_geometry} != ''
    """

    with trino_connection(engine) as conn:
        df = pd.read_sql(text(sql), conn)

    log.info(f"  {len(df)} lignes lues depuis {cfg.table_source}")

    # Convertir WKT → objet Shapely
    df["geometry"] = df[cfg.col_geometry].apply(
        lambda x: wkt.loads(x) if x and isinstance(x, str) else None
    )

    # Créer le GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=cfg.crs_source)
    gdf = gdf.drop(columns=[cfg.col_geometry])  # plus besoin de la colonne brute

    # Filtrer les géométries invalides
    invalides = gdf[~gdf.geometry.is_valid]
    if len(invalides) > 0:
        log.warning(f"  {len(invalides)} géométries invalides → correction avec buffer(0)")
        gdf.geometry = gdf.geometry.apply(
            lambda g: g.buffer(0) if g is not None and not g.is_valid else g
        )

    log.info(f"  CRS source : {gdf.crs}")
    return gdf


# =============================================================================
# ÉTAPE 2 — TRAITEMENT GÉOSPATIAL
# =============================================================================

def traiter_geo(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Applique l'ensemble des transformations géospatiales :

    a) Reprojection en Lambert-93 pour les calculs métriques
    b) Calcul centroïde, aire, périmètre
    c) Simplification de la géométrie
    d) Buffer (zone tampon)
    e) Détection appartenance à une région de référence (spatial join)
    f) Retour en WGS84 pour le stockage
    """
    log.info("[2] Traitement géospatial...")

    # ── a) Reprojection → Lambert-93 (calculs en mètres) ─────────────────────
    gdf_m = gdf.to_crs(cfg.crs_calcul)
    log.info(f"  Reprojection : {cfg.crs_source} → {cfg.crs_calcul}")

    # ── b) Métriques géométriques ─────────────────────────────────────────────
    gdf_m["aire_m2"]        = gdf_m.geometry.area.round(2)
    gdf_m["perimetre_m"]    = gdf_m.geometry.length.round(2)
    gdf_m["aire_km2"]       = (gdf_m["aire_m2"] / 1_000_000).round(6)

    # Centroïde (en Lambert-93 puis reconverti)
    centroides_m            = gdf_m.geometry.centroid
    centroides_wgs          = centroides_m.to_crs(cfg.crs_source)
    gdf_m["centroide_lon"]  = centroides_wgs.x.round(8)
    gdf_m["centroide_lat"]  = centroides_wgs.y.round(8)

    # Bounding box
    gdf_m["bbox_minx"]      = gdf_m.geometry.bounds["minx"].round(4)
    gdf_m["bbox_miny"]      = gdf_m.geometry.bounds["miny"].round(4)
    gdf_m["bbox_maxx"]      = gdf_m.geometry.bounds["maxx"].round(4)
    gdf_m["bbox_maxy"]      = gdf_m.geometry.bounds["maxy"].round(4)

    log.info(f"  Aires calculées : min={gdf_m['aire_km2'].min():.3f} km²"
             f"  max={gdf_m['aire_km2'].max():.3f} km²")

    # ── c) Simplification ─────────────────────────────────────────────────────
    # En WGS84 pour la simplification (tolérance en degrés)
    gdf_wgs_tmp   = gdf_m.to_crs(cfg.crs_source)
    gdf_m["geom_simplifiee_wkt"] = gdf_wgs_tmp.geometry.simplify(
        cfg.simplify_tolerance, preserve_topology=True
    ).to_wkt()

    log.info(f"  Simplification : tolérance={cfg.simplify_tolerance}°")

    # ── d) Buffer (zone tampon autour de chaque géométrie) ────────────────────
    gdf_buffer = gdf_m.copy()
    gdf_buffer.geometry = gdf_m.geometry.buffer(cfg.buffer_metres)
    gdf_m["buffer_wkt"] = gdf_buffer.to_crs(cfg.crs_source).geometry.to_wkt()

    log.info(f"  Buffer : {cfg.buffer_metres} m")

    # ── e) Spatial join : appartenance à une région de référence ──────────────
    # Exemple : on charge des régions administratives de référence
    # et on détermine dans quelle région tombe chaque zone
    regions_ref = _charger_regions_reference()

    if regions_ref is not None:
        regions_ref_m = regions_ref.to_crs(cfg.crs_calcul)
        joined = gpd.sjoin(
            gdf_m,
            regions_ref_m[["nom_region", "code_region", "geometry"]],
            how="left",
            predicate="intersects",   # ou "within", "contains"
        )
        # Dédupliquer si une zone intersecte plusieurs régions
        joined = joined.drop_duplicates(subset=[cfg.col_id_metier], keep="first")
        gdf_m["nom_region"]  = joined["nom_region"].values
        gdf_m["code_region"] = joined["code_region"].values
        log.info("  Spatial join régions : OK")
    else:
        gdf_m["nom_region"]  = None
        gdf_m["code_region"] = None
        log.info("  Spatial join régions : ignoré (pas de référentiel)")

    # ── f) Retour en WGS84 pour le stockage ───────────────────────────────────
    gdf_final = gdf_m.to_crs(cfg.crs_sortie)

    # Stocker la géométrie principale en WKT (pour insertion SQL)
    gdf_final["geom_wkt"]  = gdf_final.geometry.to_wkt()

    # Métadonnées de traitement
    gdf_final["crs_calcul"]   = cfg.crs_calcul
    gdf_final["crs_stockage"] = cfg.crs_sortie

    log.info(f"  Traitement terminé : {len(gdf_final)} zones enrichies")
    return gdf_final


def _charger_regions_reference() -> Optional[gpd.GeoDataFrame]:
    """
    Charge un référentiel géo de régions pour le spatial join.
    Ici en dur pour l'exemple — remplace par lecture depuis ta source.
    """
    try:
        # Exemple : 3 régions fictives (en production : lire depuis BDD ou fichier)
        data = {
            "nom_region":  ["Île-de-France", "Auvergne-Rhône-Alpes", "Provence-PACA"],
            "code_region": ["11",             "84",                    "93"],
            "geometry":    [
                wkt.loads("POLYGON((1.5 48.0, 3.5 48.0, 3.5 49.5, 1.5 49.5, 1.5 48.0))"),
                wkt.loads("POLYGON((2.0 44.0, 7.0 44.0, 7.0 47.0, 2.0 47.0, 2.0 44.0))"),
                wkt.loads("POLYGON((4.0 43.0, 8.0 43.0, 8.0 45.0, 4.0 45.0, 4.0 43.0))"),
            ]
        }
        return gpd.GeoDataFrame(data, crs="EPSG:4326")
    except Exception as e:
        log.warning(f"Impossible de charger les régions de référence : {e}")
        return None


# =============================================================================
# ÉTAPE 3 — ÉCRITURE DU STAGING VIA SQLALCHEMY
# =============================================================================

def ecrire_staging(gdf: gpd.GeoDataFrame, engine: Engine) -> int:
    """
    Écrit le GeoDataFrame enrichi dans la table de staging Trino/Iceberg.
    On écrit les géométries en WKT (colonne VARCHAR dans Iceberg).
    Stratégie : truncate + insert (la table staging est temporaire).
    """
    log.info(f"[3] Écriture staging : {cfg.table_staging}")

    # Colonnes à écrire (on exclut l'objet geometry Shapely — non sérialisable SQL)
    colonnes = [
        cfg.col_id_metier,
        "nom_zone",
        "type_zone",
        "population",
        "geom_wkt",              # géométrie principale WKT
        "geom_simplifiee_wkt",   # géométrie simplifiée WKT
        "buffer_wkt",            # buffer WKT
        "centroide_lon",
        "centroide_lat",
        "aire_m2",
        "perimetre_m",
        "aire_km2",
        "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy",
        "nom_region",
        "code_region",
        "crs_calcul",
        "crs_stockage",
    ]

    # Filtrer les colonnes existantes dans le DataFrame
    colonnes_presentes = [c for c in colonnes if c in gdf.columns]
    df_export = gdf[colonnes_presentes].copy()

    # Convertir les types Python non-SQL en str
    for col in df_export.select_dtypes(include=["object"]).columns:
        df_export[col] = df_export[col].astype(str).where(
            df_export[col].notna(), other=None
        )

    with trino_connection(engine) as conn:

        # Vider la table staging avant rechargement
        conn.execute(text(f"DELETE FROM {cfg.table_staging}"))
        log.info(f"  Table {cfg.table_staging} vidée")

        # Insertion par batch de 500 lignes (Trino préfère les petits batches)
        batch_size = 500
        total      = 0

        for debut in range(0, len(df_export), batch_size):
            batch = df_export.iloc[debut : debut + batch_size]

            # Construire l'INSERT VALUES dynamiquement
            rows_sql = _build_insert_values(batch, colonnes_presentes)

            insert_sql = f"""
                INSERT INTO {cfg.table_staging}
                ({', '.join(colonnes_presentes)})
                VALUES {rows_sql}
            """
            conn.execute(text(insert_sql))
            total += len(batch)
            log.info(f"  Batch inséré : {total}/{len(df_export)} lignes")

    log.info(f"  ✓ {total} lignes écrites dans {cfg.table_staging}")
    return total


def _build_insert_values(df: pd.DataFrame, colonnes: list) -> str:
    """
    Construit la clause VALUES d'un INSERT multi-lignes.
    Gère les NULL, les chaînes avec quotes, et les nombres.
    """
    rows = []
    for _, row in df.iterrows():
        vals = []
        for col in colonnes:
            v = row[col]
            if v is None or (isinstance(v, float) and pd.isna(v)):
                vals.append("NULL")
            elif isinstance(v, (int, float)):
                vals.append(str(v))
            else:
                # Échapper les quotes simples dans les WKT et les noms
                escaped = str(v).replace("'", "''")
                vals.append(f"'{escaped}'")
        rows.append(f"({', '.join(vals)})")
    return ",\n".join(rows)


# =============================================================================
# ÉTAPE 4 — MERGE TRINO : staging → dest_geo
# =============================================================================

def merge_vers_dest(engine: Engine) -> dict:
    """
    MERGE Trino : source_geo_enrichie → dest_geo.
    - INSERT si la zone (code_zone) est nouvelle
    - UPDATE si les données géo ont changé
    """
    log.info(f"[4] MERGE Trino : {cfg.table_staging} → {cfg.table_dest}")

    sql_merge = f"""
        MERGE INTO {cfg.table_dest} AS tgt
        USING (
            SELECT
                {cfg.col_id_metier},
                nom_zone,
                type_zone,
                population,
                geom_wkt,
                geom_simplifiee_wkt,
                buffer_wkt,
                centroide_lon,
                centroide_lat,
                aire_m2,
                perimetre_m,
                aire_km2,
                bbox_minx, bbox_miny, bbox_maxx, bbox_maxy,
                nom_region,
                code_region,
                crs_calcul,
                crs_stockage
            FROM {cfg.table_staging}
        ) AS src

        -- Clé de déduplication métier
        ON (tgt.{cfg.col_id_metier} = src.{cfg.col_id_metier})

        -- Ligne existante ET au moins un champ géo ou attribut a changé
        WHEN MATCHED
          AND (
              tgt.geom_wkt              IS DISTINCT FROM src.geom_wkt
           OR tgt.geom_simplifiee_wkt   IS DISTINCT FROM src.geom_simplifiee_wkt
           OR tgt.buffer_wkt            IS DISTINCT FROM src.buffer_wkt
           OR tgt.centroide_lon         IS DISTINCT FROM src.centroide_lon
           OR tgt.centroide_lat         IS DISTINCT FROM src.centroide_lat
           OR tgt.aire_m2               IS DISTINCT FROM src.aire_m2
           OR tgt.perimetre_m           IS DISTINCT FROM src.perimetre_m
           OR tgt.population            IS DISTINCT FROM src.population
           OR tgt.nom_region            IS DISTINCT FROM src.nom_region
          )
        THEN UPDATE SET
            nom_zone             = src.nom_zone,
            type_zone            = src.type_zone,
            population           = src.population,
            geom_wkt             = src.geom_wkt,
            geom_simplifiee_wkt  = src.geom_simplifiee_wkt,
            buffer_wkt           = src.buffer_wkt,
            centroide_lon        = src.centroide_lon,
            centroide_lat        = src.centroide_lat,
            aire_m2              = src.aire_m2,
            perimetre_m          = src.perimetre_m,
            aire_km2             = src.aire_km2,
            bbox_minx            = src.bbox_minx,
            bbox_miny            = src.bbox_miny,
            bbox_maxx            = src.bbox_maxx,
            bbox_maxy            = src.bbox_maxy,
            nom_region           = src.nom_region,
            code_region          = src.code_region,
            crs_stockage         = src.crs_stockage

        -- Nouvelle zone → INSERT avec UUID généré
        WHEN NOT MATCHED
        THEN INSERT (
            dest_geo_cle_primaire_auto_incremente,
            {cfg.col_id_metier},
            nom_zone,
            type_zone,
            population,
            geom_wkt,
            geom_simplifiee_wkt,
            buffer_wkt,
            centroide_lon,
            centroide_lat,
            aire_m2,
            perimetre_m,
            aire_km2,
            bbox_minx, bbox_miny, bbox_maxx, bbox_maxy,
            nom_region,
            code_region,
            crs_calcul,
            crs_stockage
        )
        VALUES (
            uuid(),                 -- PK générée par Trino
            src.{cfg.col_id_metier},
            src.nom_zone,
            src.type_zone,
            src.population,
            src.geom_wkt,
            src.geom_simplifiee_wkt,
            src.buffer_wkt,
            src.centroide_lon,
            src.centroide_lat,
            src.aire_m2,
            src.perimetre_m,
            src.aire_km2,
            src.bbox_minx,
            src.bbox_miny,
            src.bbox_maxx,
            src.bbox_maxy,
            src.nom_region,
            src.code_region,
            src.crs_calcul,
            src.crs_stockage
        )
    """

    with trino_connection(engine) as conn:
        conn.execute(text(sql_merge))

    log.info(f"  ✓ MERGE terminé : {cfg.table_staging} → {cfg.table_dest}")

    # Compter le résultat après MERGE
    with trino_connection(engine) as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {cfg.table_dest}")
        ).fetchone()
        nb_total = result[0] if result else 0

    return {"nb_total_dest": nb_total}


# =============================================================================
# ÉTAPE 5 — TESTS DE VALIDATION POST-CHARGEMENT
# =============================================================================

def valider_chargement(engine: Engine) -> bool:
    """
    Exécute les tests de qualité post-MERGE et retourne True si tout est OK.
    """
    log.info("[5] Validation du chargement...")

    tests = []

    with trino_connection(engine) as conn:

        def run(nom: str, sql: str, valeur_attendue, comparateur="eq"):
            result = conn.execute(text(sql)).fetchone()
            valeur = result[0] if result else None
            ok = (
                valeur == valeur_attendue   if comparateur == "eq"  else
                valeur == 0                 if comparateur == "zero" else
                valeur >= valeur_attendue   if comparateur == "gte" else
                True
            )
            statut = "OK" if ok else "ECHEC"
            tests.append({"test": nom, "statut": statut,
                          "valeur": valeur, "attendu": valeur_attendue})
            log.info(f"  [{statut}] {nom} : {valeur} (attendu {valeur_attendue})")
            return ok

        # ── Volumétrie ────────────────────────────────────────────────────────
        run(
            "Toutes les zones source sont dans dest",
            f"""
            SELECT COUNT(*) FROM (
                SELECT {cfg.col_id_metier} FROM {cfg.table_staging}
                EXCEPT
                SELECT {cfg.col_id_metier} FROM {cfg.table_dest}
            )
            """,
            0, "zero"
        )

        # ── PK non null et unique ─────────────────────────────────────────────
        run(
            "PK non NULL",
            f"""
            SELECT COUNT(*) FROM {cfg.table_dest}
            WHERE dest_geo_cle_primaire_auto_incremente IS NULL
            """,
            0, "zero"
        )

        run(
            "PK unique (pas de doublons)",
            f"""
            SELECT COUNT(*) FROM (
                SELECT dest_geo_cle_primaire_auto_incremente, COUNT(*) AS nb
                FROM {cfg.table_dest}
                GROUP BY dest_geo_cle_primaire_auto_incremente
                HAVING COUNT(*) > 1
            )
            """,
            0, "zero"
        )

        # ── Qualité géo ───────────────────────────────────────────────────────
        run(
            "Geometrie WKT non NULL",
            f"""
            SELECT COUNT(*) FROM {cfg.table_dest}
            WHERE geom_wkt IS NULL OR TRIM(geom_wkt) = ''
            """,
            0, "zero"
        )

        run(
            "Aires > 0",
            f"""
            SELECT COUNT(*) FROM {cfg.table_dest}
            WHERE aire_m2 IS NOT NULL AND aire_m2 <= 0
            """,
            0, "zero"
        )

        run(
            "Centroïdes dans plage coordonnées France",
            f"""
            SELECT COUNT(*) FROM {cfg.table_dest}
            WHERE centroide_lon IS NOT NULL
              AND (centroide_lon < -5.5 OR centroide_lon > 10.0
               OR centroide_lat < 41.0  OR centroide_lat > 52.0)
            """,
            0, "zero"
        )

        # ── Exactitude : valeurs identiques entre staging et dest ─────────────
        run(
            "Concordance aire_m2 staging vs dest",
            f"""
            SELECT COUNT(*) FROM {cfg.table_staging} s
            INNER JOIN {cfg.table_dest} d
                ON d.{cfg.col_id_metier} = s.{cfg.col_id_metier}
            WHERE s.aire_m2 IS DISTINCT FROM d.aire_m2
            """,
            0, "zero"
        )

        run(
            "Concordance geom_wkt staging vs dest",
            f"""
            SELECT COUNT(*) FROM {cfg.table_staging} s
            INNER JOIN {cfg.table_dest} d
                ON d.{cfg.col_id_metier} = s.{cfg.col_id_metier}
            WHERE s.geom_wkt IS DISTINCT FROM d.geom_wkt
            """,
            0, "zero"
        )

    # ── Rapport synthèse ──────────────────────────────────────────────────────
    nb_ok     = sum(1 for t in tests if t["statut"] == "OK")
    nb_echec  = sum(1 for t in tests if t["statut"] == "ECHEC")

    log.info("─" * 50)
    log.info(f"  Résultat validation : {nb_ok}/{len(tests)} tests OK")

    if nb_echec > 0:
        log.error(f"  ⚠ {nb_echec} test(s) en ECHEC :")
        for t in tests:
            if t["statut"] == "ECHEC":
                log.error(f"    ✗ {t['test']} : valeur={t['valeur']}")
        return False

    log.info("  ✓ Tous les tests de validation sont OK")
    return True


# =============================================================================
# POINT D'ENTRÉE PRINCIPAL
# =============================================================================

def run_pipeline():
    """Orchestre les 5 étapes du pipeline géospatial."""

    log.info("=" * 60)
    log.info("  Pipeline Géospatial : Trino + GeoPandas + SQLAlchemy")
    log.info("=" * 60)

    engine = make_trino_engine()

    try:
        # [1] Lecture
        gdf_source = lire_source(engine)

        # [2] Traitement géo
        gdf_enrichi = traiter_geo(gdf_source)

        # [3] Écriture staging
        nb_ecrits = ecrire_staging(gdf_enrichi, engine)

        # [4] MERGE vers dest
        stats = merge_vers_dest(engine)

        # [5] Validation
        ok = valider_chargement(engine)

        log.info("=" * 60)
        log.info(f"  Pipeline terminé")
        log.info(f"  Zones traitées  : {nb_ecrits}")
        log.info(f"  Total dans dest : {stats['nb_total_dest']}")
        log.info(f"  Validation      : {'✓ OK' if ok else '✗ ECHEC'}")
        log.info("=" * 60)

        return ok

    except Exception as e:
        log.error(f"Pipeline interrompu : {e}", exc_info=True)
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    run_pipeline()