```sql
-- =============================================================================
-- Pipeline MERGE : source → dest2/dest3/dest4 → dest1
-- Trino + Apache Iceberg (pas de PK native, UUID générés manuellement)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- PRÉREQUIS : structure des tables Iceberg
-- (à exécuter une seule fois à la création)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dest2 (
    dest2_cle_primaire_auto_incremente  VARCHAR       NOT NULL,  -- UUID
    source21                            VARCHAR       NOT NULL,  -- clé métier
    source22                            VARCHAR,
    source23                            VARCHAR
)
WITH (
    format         = 'PARQUET',
    partitioning   = ARRAY[],
    location       = 's3://warehouse/dest2'
);

CREATE TABLE IF NOT EXISTS dest3 (
    dest3_cle_primaire_auto_incremente  VARCHAR       NOT NULL,
    source31                            VARCHAR       NOT NULL,
    source32                            VARCHAR,
    source33                            VARCHAR
)
WITH (
    format         = 'PARQUET',
    location       = 's3://warehouse/dest3'
);

CREATE TABLE IF NOT EXISTS dest4 (
    dest4_cle_primaire_auto_incremente  VARCHAR       NOT NULL,
    source41                            VARCHAR       NOT NULL,
    source42                            VARCHAR
)
WITH (
    format         = 'PARQUET',
    location       = 's3://warehouse/dest4'
);

CREATE TABLE IF NOT EXISTS dest1 (
    dest1_cle_primaire_auto_incremente              VARCHAR   NOT NULL,
    dest2_cle_primaire_auto_incremente_cle_etrangere VARCHAR,  -- FK → dest2
    dest3_cle_primaire_auto_incremente_cle_etrangere VARCHAR,  -- FK → dest3
    dest4_cle_primaire_auto_incremente_cle_etrangere VARCHAR,  -- FK → dest4
    source11                                         VARCHAR  NOT NULL,
    source12                                         VARCHAR
)
WITH (
    format         = 'PARQUET',
    location       = 's3://warehouse/dest1'
);


-- =============================================================================
-- ÉTAPE 1 — MERGE dest2
--
-- Logique UUID :
--   INSERT → uuid() génère un nouvel identifiant unique
--   UPDATE → on NE TOUCHE PAS la PK existante (elle est immuable)
--
-- Clé de déduplication métier : source21
-- =============================================================================

MERGE INTO dest2 AS tgt
USING (
    SELECT
        uuid()   AS new_pk,   -- UUID généré pour les nouvelles lignes uniquement
        source21,
        source22,
        source23
    FROM source
) AS src

ON (tgt.source21 = src.source21)   -- rapprochement métier source ↔ dest2

-- Ligne existante ET au moins un champ a changé → UPDATE
WHEN MATCHED
  AND (
      tgt.source22 IS DISTINCT FROM src.source22
   OR tgt.source23 IS DISTINCT FROM src.source23
  )
THEN UPDATE SET
    -- dest2_cle_primaire_auto_incremente : NON modifié (PK immuable)
    source22 = src.source22,
    source23 = src.source23

-- Nouvelle ligne → INSERT avec UUID généré
WHEN NOT MATCHED
THEN INSERT (
    dest2_cle_primaire_auto_incremente,
    source21,
    source22,
    source23
)
VALUES (
    src.new_pk,     -- UUID pré-calculé dans le USING
    src.source21,
    src.source22,
    src.source23
);


-- =============================================================================
-- ÉTAPE 2 — MERGE dest3
-- Clé de déduplication métier : source31
-- =============================================================================

MERGE INTO dest3 AS tgt
USING (
    SELECT
        uuid()   AS new_pk,
        source31,
        source32,
        source33
    FROM source
) AS src

ON (tgt.source31 = src.source31)

WHEN MATCHED
  AND (
      tgt.source32 IS DISTINCT FROM src.source32
   OR tgt.source33 IS DISTINCT FROM src.source33
  )
THEN UPDATE SET
    source32 = src.source32,
    source33 = src.source33

WHEN NOT MATCHED
THEN INSERT (
    dest3_cle_primaire_auto_incremente,
    source31,
    source32,
    source33
)
VALUES (
    src.new_pk,
    src.source31,
    src.source32,
    src.source33
);


-- =============================================================================
-- ÉTAPE 3 — MERGE dest4
-- Clé de déduplication métier : source41
-- =============================================================================

MERGE INTO dest4 AS tgt
USING (
    SELECT
        uuid()   AS new_pk,
        source41,
        source42
    FROM source
) AS src

ON (tgt.source41 = src.source41)

WHEN MATCHED
  AND (
      tgt.source42 IS DISTINCT FROM src.source42
  )
THEN UPDATE SET
    source42 = src.source42

WHEN NOT MATCHED
THEN INSERT (
    dest4_cle_primaire_auto_incremente,
    source41,
    source42
)
VALUES (
    src.new_pk,
    src.source41,
    src.source42
);


-- =============================================================================
-- ÉTAPE 4 — MERGE dest1
--
-- Exécuté APRÈS les étapes 1/2/3 (commit Iceberg effectué).
-- Les PK de dest2/3/4 sont maintenant lisibles.
--
-- Stratégie :
--   Le USING relit les PK depuis dest2/3/4 via jointure sur les clés métier.
--   Ainsi les FK de dest1 sont toujours synchronisées avec la réalité.
--
-- Clé de déduplication métier de dest1 : source11
-- =============================================================================

MERGE INTO dest1 AS tgt
USING (
    SELECT
        -- UUID pour les nouvelles lignes de dest1
        uuid()  AS new_pk,

        -- Données propres à dest1
        s.source11,
        s.source12,

        -- ── Résolution FK dest2 ──────────────────────────────────────────────
        -- On relit la PK réelle stockée dans dest2 après le MERGE de l'étape 1.
        -- La jointure se fait sur la clé métier commune source ↔ dest2.
        d2.dest2_cle_primaire_auto_incremente   AS fk_dest2,

        -- ── Résolution FK dest3 ──────────────────────────────────────────────
        d3.dest3_cle_primaire_auto_incremente   AS fk_dest3,

        -- ── Résolution FK dest4 ──────────────────────────────────────────────
        d4.dest4_cle_primaire_auto_incremente   AS fk_dest4

    FROM source s

    -- Jointure dest2 → on retrouve la PK via la clé métier source21
    INNER JOIN dest2 d2
        ON d2.source21 = s.source21

    -- Jointure dest3 → on retrouve la PK via la clé métier source31
    INNER JOIN dest3 d3
        ON d3.source31 = s.source31

    -- Jointure dest4 → on retrouve la PK via la clé métier source41
    INNER JOIN dest4 d4
        ON d4.source41 = s.source41

) AS src

-- Rapprochement métier source ↔ dest1
ON (tgt.source11 = src.source11)

-- Ligne existante ET au moins un champ a changé → UPDATE
WHEN MATCHED
  AND (
      tgt.source12 IS DISTINCT FROM src.source12

      -- Les FK peuvent aussi changer si les lignes de dest2/3/4
      -- ont été réassignées (rare mais possible)
   OR tgt.dest2_cle_primaire_auto_incremente_cle_etrangere
          IS DISTINCT FROM src.fk_dest2
   OR tgt.dest3_cle_primaire_auto_incremente_cle_etrangere
          IS DISTINCT FROM src.fk_dest3
   OR tgt.dest4_cle_primaire_auto_incremente_cle_etrangere
          IS DISTINCT FROM src.fk_dest4
  )
THEN UPDATE SET
    source12                                          = src.source12,
    dest2_cle_primaire_auto_incremente_cle_etrangere  = src.fk_dest2,
    dest3_cle_primaire_auto_incremente_cle_etrangere  = src.fk_dest3,
    dest4_cle_primaire_auto_incremente_cle_etrangere  = src.fk_dest4
    -- dest1_cle_primaire_auto_incremente : NON modifié (PK immuable)

-- Nouvelle ligne → INSERT
WHEN NOT MATCHED
THEN INSERT (
    dest1_cle_primaire_auto_incremente,
    dest2_cle_primaire_auto_incremente_cle_etrangere,
    dest3_cle_primaire_auto_incremente_cle_etrangere,
    dest4_cle_primaire_auto_incremente_cle_etrangere,
    source11,
    source12
)
VALUES (
    src.new_pk,
    src.fk_dest2,
    src.fk_dest3,
    src.fk_dest4,
    src.source11,
    src.source12
);
```