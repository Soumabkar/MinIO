# Les 5 niveaux de tests

* Niveau 1 — Volumétrie        : le bon nombre de lignes est arrivé ?
* Niveau 2 — Intégrité         : les FK pointent vers des PK qui existent ?
* Niveau 3 — Qualité           : les données sont-elles propres ?
* Niveau 4 — Exactitude        : les valeurs sont-elles identiques à la source ?
* Niveau 5 — Idempotence       : rejouer le MERGE ne duplique pas de données ?

## Niveau 1 — Volumétrie

```sql
-- ── Nombre de lignes chargées vs source ──────────────────────────────────────

-- Lignes distinctes dans source par clé métier
SELECT COUNT(DISTINCT source21) AS nb_source_dest2
FROM source;
-- → Résultat attendu : même valeur que ci-dessous

SELECT COUNT(*) AS nb_dest2
FROM dest2;

-- ── Test combiné : écart entre source et dest ────────────────────────────────
SELECT
    COUNT(DISTINCT s.source21)              AS nb_lignes_source,
    COUNT(DISTINCT d.dest2_cle_primaire_auto_incremente) AS nb_lignes_dest2,
    COUNT(DISTINCT s.source21)
      - COUNT(DISTINCT d.dest2_cle_primaire_auto_incremente) AS ecart
FROM source s
FULL OUTER JOIN dest2 d ON d.source21 = s.source21;
-- ecart = 0 → OK

-- ── Lignes de source non arrivées dans dest2 (manquantes) ────────────────────
SELECT s.source21
FROM source s
LEFT JOIN dest2 d ON d.source21 = s.source21
WHERE d.dest2_cle_primaire_auto_incremente IS NULL;
-- Résultat vide → OK

-- ── Lignes dans dest2 sans correspondance dans source (orphelines) ────────────
SELECT d.source21
FROM dest2 d
LEFT JOIN source s ON s.source21 = d.source21
WHERE s.source21 IS NULL;
-- Résultat vide → OK (sauf si suppression volontaire autorisée)
```

## Niveau 2 — Intégrité référentielle

```sql
-- ── FK de dest1 vers dest2 : toutes les FK pointent vers une PK existante ────
SELECT
    d1.dest1_cle_primaire_auto_incremente,
    d1.dest2_cle_primaire_auto_incremente_cle_etrangere AS fk_dest2
FROM dest1 d1
LEFT JOIN dest2 d2
    ON d2.dest2_cle_primaire_auto_incremente
     = d1.dest2_cle_primaire_auto_incremente_cle_etrangere
WHERE d2.dest2_cle_primaire_auto_incremente IS NULL
  AND d1.dest2_cle_primaire_auto_incremente_cle_etrangere IS NOT NULL;
-- Résultat vide → OK (aucune FK cassée vers dest2)

-- ── Même test pour FK → dest3 ─────────────────────────────────────────────────
SELECT
    d1.dest1_cle_primaire_auto_incremente,
    d1.dest3_cle_primaire_auto_incremente_cle_etrangere AS fk_dest3
FROM dest1 d1
LEFT JOIN dest3 d3
    ON d3.dest3_cle_primaire_auto_incremente
     = d1.dest3_cle_primaire_auto_incremente_cle_etrangere
WHERE d3.dest3_cle_primaire_auto_incremente IS NULL
  AND d1.dest3_cle_primaire_auto_incremente_cle_etrangere IS NOT NULL;
-- Résultat vide → OK

-- ── Même test pour FK → dest4 ─────────────────────────────────────────────────
SELECT
    d1.dest1_cle_primaire_auto_incremente,
    d1.dest4_cle_primaire_auto_incremente_cle_etrangere AS fk_dest4
FROM dest1 d1
LEFT JOIN dest4 d4
    ON d4.dest4_cle_primaire_auto_incremente
     = d1.dest4_cle_primaire_auto_incremente_cle_etrangere
WHERE d4.dest4_cle_primaire_auto_incremente IS NULL
  AND d1.dest4_cle_primaire_auto_incremente_cle_etrangere IS NOT NULL;
-- Résultat vide → OK

-- ── Unicité des PK : pas de doublons ─────────────────────────────────────────
SELECT
    dest2_cle_primaire_auto_incremente,
    COUNT(*) AS nb
FROM dest2
GROUP BY dest2_cle_primaire_auto_incremente
HAVING COUNT(*) > 1;
-- Résultat vide → OK

-- ── Idem dest1 ────────────────────────────────────────────────────────────────
SELECT
    dest1_cle_primaire_auto_incremente,
    COUNT(*) AS nb
FROM dest1
GROUP BY dest1_cle_primaire_auto_incremente
HAVING COUNT(*) > 1;
-- Résultat vide → OK

-- ── PK NULL interdite ─────────────────────────────────────────────────────────
SELECT COUNT(*) AS pk_nulles
FROM dest2
WHERE dest2_cle_primaire_auto_incremente IS NULL;
-- Résultat : 0 → OK
```

## Niveau 3 — Qualité des données

```sql
-- ── Colonnes NOT NULL respectées ─────────────────────────────────────────────
SELECT
    COUNT(*) FILTER (WHERE source21 IS NULL) AS source21_nulles,
    COUNT(*) FILTER (WHERE source22 IS NULL) AS source22_nulles,
    COUNT(*) FILTER (WHERE source23 IS NULL) AS source23_nulles
FROM dest2;
-- Tout à 0 si NOT NULL attendu

-- ── Doublons sur la clé métier (source21 doit être unique dans dest2) ─────────
SELECT source21, COUNT(*) AS nb
FROM dest2
GROUP BY source21
HAVING COUNT(*) > 1;
-- Résultat vide → OK

-- ── Valeurs aberrantes (exemples) ────────────────────────────────────────────
-- Chaînes vides
SELECT COUNT(*) AS chaines_vides
FROM dest2
WHERE TRIM(source22) = '';

-- Longueur anormale
SELECT source21, LENGTH(source22) AS lg
FROM dest2
WHERE LENGTH(source22) > 255;

-- FK nulles alors qu'elles devraient être renseignées
SELECT COUNT(*) AS fk_dest2_nulles
FROM dest1
WHERE dest2_cle_primaire_auto_incremente_cle_etrangere IS NULL;
```

## Niveau 4 — Exactitude (source vs destination)

```sql
-- ── Toutes les valeurs sont-elles identiques entre source et dest2 ? ──────────
SELECT
    s.source21,
    s.source22                     AS source22_source,
    d.source22                     AS source22_dest2,
    s.source23                     AS source23_source,
    d.source23                     AS source23_dest2
FROM source s
INNER JOIN dest2 d ON d.source21 = s.source21
WHERE
    s.source22 IS DISTINCT FROM d.source22
 OR s.source23 IS DISTINCT FROM d.source23;
-- Résultat vide → OK (toutes les valeurs concordent)

-- ── Vérification croisée dest1 ← source + FK ─────────────────────────────────
SELECT
    s.source11,
    s.source12                     AS source12_source,
    d1.source12                    AS source12_dest1,
    d2.dest2_cle_primaire_auto_incremente AS pk_dest2_attendue,
    d1.dest2_cle_primaire_auto_incremente_cle_etrangere AS fk_dest2_presente
FROM source s
INNER JOIN dest1 d1 ON d1.source11    = s.source11
INNER JOIN dest2 d2 ON d2.source21    = s.source21
INNER JOIN dest3 d3 ON d3.source31    = s.source31
INNER JOIN dest4 d4 ON d4.source41    = s.source41
WHERE
    s.source12 IS DISTINCT FROM d1.source12
 OR d2.dest2_cle_primaire_auto_incremente
        IS DISTINCT FROM d1.dest2_cle_primaire_auto_incremente_cle_etrangere
 OR d3.dest3_cle_primaire_auto_incremente
        IS DISTINCT FROM d1.dest3_cle_primaire_auto_incremente_cle_etrangere
 OR d4.dest4_cle_primaire_auto_incremente
        IS DISTINCT FROM d1.dest4_cle_primaire_auto_incremente_cle_etrangere;
-- Résultat vide → OK
```

## Niveau 5 — Idempotence

Le MERGE doit être rejouable sans effet de bord : rejouer deux fois le même pipeline ne doit pas créer de doublons ni modifier des lignes déjà correctes.

```sql
-- ── Compter avant le second passage ──────────────────────────────────────────
SELECT COUNT(*) AS nb_avant FROM dest2;

-- ── Rejouer le MERGE dest2 (identique au premier passage) ────────────────────
MERGE INTO dest2 AS tgt
USING (
    SELECT uuid() AS new_pk, source21, source22, source23
    FROM source
) AS src
ON (tgt.source21 = src.source21)
WHEN MATCHED
  AND (
      tgt.source22 IS DISTINCT FROM src.source22
   OR tgt.source23 IS DISTINCT FROM src.source23
  )
THEN UPDATE SET source22 = src.source22, source23 = src.source23
WHEN NOT MATCHED
THEN INSERT (dest2_cle_primaire_auto_incremente, source21, source22, source23)
VALUES (src.new_pk, src.source21, src.source22, src.source23);

-- ── Compter après : le nombre doit être identique ────────────────────────────
SELECT COUNT(*) AS nb_apres FROM dest2;
-- nb_avant = nb_apres → OK (aucune ligne dupliquée)

-- ── Vérifier que les PK n'ont pas changé (stabilité des UUID) ────────────────
-- Snapshot avant (à sauvegarder dans une table temporaire avant le 2e passage)
SELECT dest2_cle_primaire_auto_incremente, source21
FROM dest2
EXCEPT
SELECT dest2_cle_primaire_auto_incremente, source21
FROM dest2
    FOR VERSION AS OF <snapshot_id_avant>;
-- Résultat vide → les UUID sont stables entre deux passages
```
## Requête de synthèse — rapport de contrôle global

```sql
-- ── Tableau de bord de validation complet ────────────────────────────────────
SELECT
    'dest2 - volumétrie'       AS test,
    CASE WHEN
        (SELECT COUNT(DISTINCT source21) FROM source)
      = (SELECT COUNT(*) FROM dest2)
    THEN 'OK' ELSE 'ECHEC' END AS statut

UNION ALL SELECT
    'dest2 - unicité PK',
    CASE WHEN
        (SELECT COUNT(*) FROM (
            SELECT dest2_cle_primaire_auto_incremente
            FROM dest2
            GROUP BY dest2_cle_primaire_auto_incremente
            HAVING COUNT(*) > 1
        )) = 0
    THEN 'OK' ELSE 'ECHEC' END

UNION ALL SELECT
    'dest2 - PK non NULL',
    CASE WHEN
        (SELECT COUNT(*) FROM dest2
         WHERE dest2_cle_primaire_auto_incremente IS NULL) = 0
    THEN 'OK' ELSE 'ECHEC' END

UNION ALL SELECT
    'dest1 - FK dest2 valides',
    CASE WHEN
        (SELECT COUNT(*) FROM dest1 d1
         LEFT JOIN dest2 d2
           ON d2.dest2_cle_primaire_auto_incremente
            = d1.dest2_cle_primaire_auto_incremente_cle_etrangere
         WHERE d2.dest2_cle_primaire_auto_incremente IS NULL
           AND d1.dest2_cle_primaire_auto_incremente_cle_etrangere IS NOT NULL
        ) = 0
    THEN 'OK' ELSE 'ECHEC' END

UNION ALL SELECT
    'dest1 - FK dest3 valides',
    CASE WHEN
        (SELECT COUNT(*) FROM dest1 d1
         LEFT JOIN dest3 d3
           ON d3.dest3_cle_primaire_auto_incremente
            = d1.dest3_cle_primaire_auto_incremente_cle_etrangere
         WHERE d3.dest3_cle_primaire_auto_incremente IS NULL
           AND d1.dest3_cle_primaire_auto_incremente_cle_etrangere IS NOT NULL
        ) = 0
    THEN 'OK' ELSE 'ECHEC' END

UNION ALL SELECT
    'dest1 - FK dest4 valides',
    CASE WHEN
        (SELECT COUNT(*) FROM dest1 d1
         LEFT JOIN dest4 d4
           ON d4.dest4_cle_primaire_auto_incremente
            = d1.dest4_cle_primaire_auto_incremente_cle_etrangere
         WHERE d4.dest4_cle_primaire_auto_incremente IS NULL
           AND d1.dest4_cle_primaire_auto_incremente_cle_etrangere IS NOT NULL
        ) = 0
    THEN 'OK' ELSE 'ECHEC' END

UNION ALL SELECT
    'dest1 - exactitude source12',
    CASE WHEN
        (SELECT COUNT(*) FROM source s
         INNER JOIN dest1 d1 ON d1.source11 = s.source11
         WHERE s.source12 IS DISTINCT FROM d1.source12
        ) = 0
    THEN 'OK' ELSE 'ECHEC' END

ORDER BY test;
```


Récapitulatif des niveaux
--------------------------------------------------------------------------------------------
|| Niveau       || Ce qu'on vérifie                             || Résultat attendu       ||
--------------------------------------------------------------------------------------------
| Volumétrie    | Nombre de lignes source = destination         | Écart = 0                |
--------------------------------------------------------------------------------------------
| Intégrité     | FK pointent vers PK existantes, PK uniques    | 0 ligne orpheline        |
--------------------------------------------------------------------------------------------
| Qualité       | Nulls, doublons, formats                      | 0 anomalie               |
---------------------------------------------------------------------------------------------
| Exactitude    | Valeurs identiques source ↔ dest              | 0 divergence             |
--------------------------------------------------------------------------------------------
| Idempotence   | Rejouer le MERGE = aucun effet                | COUNT stable, PK stables |
--------------------------------------------------------------------------------------------