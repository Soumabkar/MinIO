"""
validator.py — Validation des données (avant et après chargement)
Chaque vérification retourne un ValidationResult typé, pas juste un booléen.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from .models import EnrichedGeoRecord, RawGeoRecord

log = logging.getLogger(__name__)


# =============================================================================
# Résultat de validation
# =============================================================================

@dataclass
class CheckResult:
    """Résultat d'un test de validation individuel."""
    name:     str
    passed:   bool
    message:  str
    value:    Optional[object] = None
    expected: Optional[object] = None

    @property
    def status(self) -> str:
        return "OK" if self.passed else "ECHEC"

    def __str__(self):
        base = f"[{self.status}] {self.name}"
        if not self.passed:
            base += f" — {self.message}"
            if self.value is not None:
                base += f" (valeur={self.value}, attendu={self.expected})"
        return base


@dataclass
class ValidationReport:
    """Agrège tous les CheckResult d'une validation complète."""
    title:   str
    checks:  List[CheckResult] = field(default_factory=list)

    def add(self, check: CheckResult) -> None:
        self.checks.append(check)
        log.info(f"  {check}")

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def nb_ok(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def nb_failed(self) -> int:
        return sum(1 for c in self.checks if not c.passed)

    def summary(self) -> str:
        lines = [
            f"\n{'═' * 55}",
            f"  {self.title}",
            f"  {self.nb_ok}/{len(self.checks)} tests OK"
            + (" ✓" if self.passed else f"  — {self.nb_failed} ECHEC(S) ✗"),
            f"{'═' * 55}",
        ]
        for c in self.checks:
            lines.append(f"  {c}")
        return "\n".join(lines)


# =============================================================================
# Validations pre-transformation (données source)
# =============================================================================

class SourceValidator:
    """Valide les enregistrements bruts avant transformation."""

    def validate(self, records: List[RawGeoRecord]) -> ValidationReport:
        report = ValidationReport("Validation source")
        log.info("Validation source...")

        # 1. Au moins un enregistrement
        report.add(CheckResult(
            name    = "Source non vide",
            passed  = len(records) > 0,
            message = "Aucun enregistrement source",
            value   = len(records),
        ))

        # 2. Pas de code_zone vide
        vides = [r for r in records if not r.code_zone.strip()]
        report.add(CheckResult(
            name    = "code_zone non vide",
            passed  = len(vides) == 0,
            message = f"{len(vides)} enregistrement(s) sans code_zone",
            value   = len(vides),
            expected= 0,
        ))

        # 3. Pas de doublons sur code_zone
        codes = [r.code_zone for r in records]
        nb_doublons = len(codes) - len(set(codes))
        report.add(CheckResult(
            name    = "Unicité code_zone",
            passed  = nb_doublons == 0,
            message = f"{nb_doublons} doublon(s) sur code_zone",
            value   = nb_doublons,
            expected= 0,
        ))

        # 4. Géométries WKT non vides
        sans_geom = [r for r in records if not r.geom_wkt.strip()]
        report.add(CheckResult(
            name    = "geom_wkt non vide",
            passed  = len(sans_geom) == 0,
            message = f"{len(sans_geom)} enregistrement(s) sans géométrie",
            value   = len(sans_geom),
            expected= 0,
        ))

        return report


# =============================================================================
# Validations post-transformation (données enrichies)
# =============================================================================

class TransformValidator:
    """Valide les enregistrements enrichis après transformation géo."""

    def __init__(self, bbox_valide: tuple = (-5.5, 41.0, 10.0, 52.0)):
        self._bbox = bbox_valide  # (lon_min, lat_min, lon_max, lat_max)

    def validate(self, records: List[EnrichedGeoRecord],
                 source_count: int) -> ValidationReport:
        report = ValidationReport("Validation post-transformation")
        log.info("Validation post-transformation...")

        if not records:
            report.add(CheckResult(
                name="Records non vides", passed=False,
                message="Liste enrichie vide"
            ))
            return report

        # 1. Volumétrie conservée
        report.add(CheckResult(
            name    = "Volumétrie conservée",
            passed  = len(records) == source_count,
            message = f"Attendu {source_count} records, obtenu {len(records)}",
            value   = len(records),
            expected= source_count,
        ))

        # 2. Aires > 0
        aires_nulles = [r for r in records if r.aire_m2 <= 0]
        report.add(CheckResult(
            name    = "Aires > 0",
            passed  = len(aires_nulles) == 0,
            message = f"{len(aires_nulles)} zone(s) avec aire ≤ 0",
            value   = len(aires_nulles),
            expected= 0,
        ))

        # 3. Périmètres > 0
        perim_nuls = [r for r in records if r.perimetre_m <= 0]
        report.add(CheckResult(
            name    = "Périmètres > 0",
            passed  = len(perim_nuls) == 0,
            message = f"{len(perim_nuls)} zone(s) avec périmètre ≤ 0",
            value   = len(perim_nuls),
            expected= 0,
        ))

        # 4. Cohérence aire_km2 = aire_m2 / 1_000_000
        incoherents = [
            r for r in records
            if abs(r.aire_km2 - r.aire_m2 / 1_000_000) > 0.001
        ]
        report.add(CheckResult(
            name    = "Cohérence aire_m2 ↔ aire_km2",
            passed  = len(incoherents) == 0,
            message = f"{len(incoherents)} incohérence(s) aire_m2/aire_km2",
            value   = len(incoherents),
            expected= 0,
        ))

        # 5. Centroïdes dans bbox valide
        hors_bbox = [r for r in records if not r.is_in_bbox(*self._bbox)]
        report.add(CheckResult(
            name    = f"Centroïdes dans bbox {self._bbox}",
            passed  = len(hors_bbox) == 0,
            message = f"{len(hors_bbox)} centroïde(s) hors bbox attendue",
            value   = len(hors_bbox),
            expected= 0,
        ))

        # 6. WKT non vides
        sans_wkt = [r for r in records
                    if not r.geom_wkt or not r.buffer_wkt or not r.geom_simplifiee_wkt]
        report.add(CheckResult(
            name    = "WKT complets (geom, buffer, simplifié)",
            passed  = len(sans_wkt) == 0,
            message = f"{len(sans_wkt)} enregistrement(s) avec WKT manquant",
            value   = len(sans_wkt),
            expected= 0,
        ))

        # 7. Bbox cohérente (minx < maxx, miny < maxy)
        bbox_ko = [r for r in records
                   if r.bbox_minx >= r.bbox_maxx or r.bbox_miny >= r.bbox_maxy]
        report.add(CheckResult(
            name    = "Bounding box cohérente (min < max)",
            passed  = len(bbox_ko) == 0,
            message = f"{len(bbox_ko)} bbox incohérente(s)",
            value   = len(bbox_ko),
            expected= 0,
        ))

        return report
