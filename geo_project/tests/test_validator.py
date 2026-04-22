"""
test_validator.py — Tests unitaires du validateur.
"""
import pytest
from src.geo_pipeline.models import RawGeoRecord
from src.geo_pipeline.validator import SourceValidator, TransformValidator


class TestSourceValidator:

    def setup_method(self):
        self.validator = SourceValidator()

    def test_source_valide_passe(self, raw_records):
        report = self.validator.validate(raw_records)
        assert report.passed

    def test_source_vide_echoue(self):
        report = self.validator.validate([])
        assert not report.passed
        assert any("vide" in c.name.lower() for c in report.checks if not c.passed)

    def test_doublon_code_zone_detecte(self, single_raw_record):
        doublons = [single_raw_record, single_raw_record]
        report = self.validator.validate(doublons)
        assert not report.passed
        failed_names = [c.name for c in report.checks if not c.passed]
        assert any("unicité" in n.lower() or "Unicité" in n for n in failed_names)

    def test_rapport_a_tous_les_checks(self, raw_records):
        report = self.validator.validate(raw_records)
        assert len(report.checks) >= 4

    def test_summary_contient_titre(self, raw_records):
        report = self.validator.validate(raw_records)
        summary = report.summary()
        assert "Validation source" in summary

    def test_nb_ok_correct(self, raw_records):
        report = self.validator.validate(raw_records)
        assert report.nb_ok == len(report.checks)
        assert report.nb_failed == 0


class TestTransformValidator:

    def setup_method(self):
        self.validator = TransformValidator(bbox_valide=(-5.5, 41.0, 10.0, 52.0))

    def test_records_valides_passent(self, enriched_records):
        report = self.validator.validate(enriched_records, source_count=2)
        assert report.passed, report.summary()

    def test_volumetrie_incorrecte_echoue(self, enriched_records):
        report = self.validator.validate(enriched_records, source_count=99)
        assert not report.passed
        failed = [c.name for c in report.checks if not c.passed]
        assert any("volumétrie" in n.lower() or "Volumétrie" in n for n in failed)

    def test_aire_negative_detectee(self, enriched_record):
        from dataclasses import replace
        bad = replace(enriched_record, aire_m2=-1.0, aire_km2=-0.000001)
        report = self.validator.validate([bad], source_count=1)
        assert not report.passed
        failed = [c.name for c in report.checks if not c.passed]
        assert any("aire" in n.lower() for n in failed)

    def test_centroide_hors_bbox_detecte(self, enriched_record):
        from dataclasses import replace
        bad = replace(enriched_record, centroide_lon=150.0, centroide_lat=70.0)
        report = self.validator.validate([bad], source_count=1)
        assert not report.passed
        failed = [c.name for c in report.checks if not c.passed]
        assert any("bbox" in n.lower() or "Centroïdes" in n for n in failed)

    def test_incoherence_aire_km2_detectee(self, enriched_record):
        from dataclasses import replace
        bad = replace(enriched_record, aire_m2=81_000_000.0, aire_km2=999.0)
        report = self.validator.validate([bad], source_count=1)
        assert not report.passed

    def test_wkt_vide_detecte(self, enriched_record):
        from dataclasses import replace
        bad = replace(enriched_record, geom_wkt="")
        report = self.validator.validate([bad], source_count=1)
        assert not report.passed

    def test_bbox_incoherente_detectee(self, enriched_record):
        from dataclasses import replace
        # minx > maxx → incohérent
        bad = replace(enriched_record, bbox_minx=5.0, bbox_maxx=2.0)
        report = self.validator.validate([bad], source_count=1)
        assert not report.passed
        failed = [c.name for c in report.checks if not c.passed]
        assert any("Bounding box" in n or "bbox" in n.lower() for n in failed)

    def test_liste_vide_echoue(self):
        report = self.validator.validate([], source_count=3)
        assert not report.passed
