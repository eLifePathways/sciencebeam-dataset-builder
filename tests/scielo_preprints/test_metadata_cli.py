"""Tests for scielo_preprints.metadata_cli module."""

import csv
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from sciencebeam_dataset_builder.scielo_preprints.metadata_cli import (
    LANGUAGE_NORMALISATION,
    extract_metadata,
    main,
    parse_args,
    _extract_language,
)


def _write_xml(path: Path, lang: str | None = "pt", lang_attr: str = "xml:lang") -> None:
    """Write a minimal article XML file with optional language attribute."""
    lang_part = f' {lang_attr}="{lang}"' if lang is not None else ""
    path.write_text(
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<article{lang_part}>"
        f"<front><article-meta/></front>"
        f"</article>",
        encoding="utf-8",
    )


class TestExtractLanguage:
    def _root(self, lang: str | None) -> ET.Element:
        lang_part = f' xml:lang="{lang}"' if lang is not None else ""
        return ET.fromstring(f"<article{lang_part}/>")

    def test_returns_standard_pt(self):
        language, raw = _extract_language(self._root("pt"))
        assert language == "pt"
        assert raw == "pt"

    def test_normalises_po_to_pt(self):
        language, _ = _extract_language(self._root("po"))
        assert language == "pt"

    def test_normalises_uppercase_pt(self):
        language, _ = _extract_language(self._root("PT"))
        assert language == "pt"

    def test_normalises_sp_to_es(self):
        language, _ = _extract_language(self._root("sp"))
        assert language == "es"

    def test_preserves_raw_code(self):
        _, raw = _extract_language(self._root("po"))
        assert raw == "po"

    def test_returns_empty_string_when_no_lang(self):
        language, raw = _extract_language(self._root(None))
        assert language == ""
        assert raw == ""

    def test_unknown_code_lowercased(self):
        language, _ = _extract_language(self._root("FR"))
        assert language == "fr"


class TestLanguageNormalisation:
    def test_all_standard_codes_map_to_themselves(self):
        for code in ("pt", "es", "en", "fr", "de"):
            assert LANGUAGE_NORMALISATION[code] == code

    def test_non_standard_variants_covered(self):
        assert LANGUAGE_NORMALISATION["po"] == "pt"
        assert LANGUAGE_NORMALISATION["sp"] == "es"
        assert LANGUAGE_NORMALISATION["PT"] == "pt"


class TestExtractMetadata:
    def test_returns_ppr_id_from_filename(self, tmp_path):
        xml_path = tmp_path / "PPR_123.xml"
        _write_xml(xml_path, lang="pt")
        row = extract_metadata(xml_path)
        assert row["ppr_id"] == "PPR_123"

    def test_returns_normalised_language(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, lang="po")
        row = extract_metadata(xml_path)
        assert row["language"] == "pt"
        assert row["language_raw"] == "po"

    def test_has_pdf_true_when_pdf_exists(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path)
        (tmp_path / "PPR_1.pdf").write_bytes(b"%PDF")
        assert extract_metadata(xml_path)["has_pdf"] is True

    def test_has_pdf_false_when_pdf_missing(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path)
        assert extract_metadata(xml_path)["has_pdf"] is False


class TestParseArgs:
    def test_requires_input_dir_and_output_csv(self):
        with pytest.raises(SystemExit):
            parse_args([])

    def test_parses_positional_args(self, tmp_path):
        args = parse_args([str(tmp_path), str(tmp_path / "out.csv")])
        assert args.input_dir == tmp_path
        assert args.output_csv == tmp_path / "out.csv"

    def test_debug_default_false(self, tmp_path):
        args = parse_args([str(tmp_path), str(tmp_path / "out.csv")])
        assert args.debug is False


class TestMain:
    def test_writes_csv_with_one_row_per_xml(self, tmp_path):
        for i in (1, 2, 3):
            _write_xml(tmp_path / f"PPR_{i}.xml", lang="pt")
        out = tmp_path / "metadata.csv"
        main([str(tmp_path), str(out)])
        rows = list(csv.DictReader(out.open()))
        assert len(rows) == 3

    def test_csv_contains_expected_fields(self, tmp_path):
        _write_xml(tmp_path / "PPR_42.xml", lang="es")
        out = tmp_path / "metadata.csv"
        main([str(tmp_path), str(out)])
        row = list(csv.DictReader(out.open()))[0]
        assert set(row.keys()) == {"ppr_id", "language", "language_raw", "has_pdf"}

    def test_exits_when_no_xml_files(self, tmp_path):
        with pytest.raises(SystemExit):
            main([str(tmp_path), str(tmp_path / "out.csv")])

    def test_skips_unparseable_xml(self, tmp_path):
        (tmp_path / "PPR_1.xml").write_text("not xml", encoding="utf-8")
        _write_xml(tmp_path / "PPR_2.xml", lang="pt")
        out = tmp_path / "metadata.csv"
        main([str(tmp_path), str(out)])
        rows = list(csv.DictReader(out.open()))
        assert len(rows) == 1
        assert rows[0]["ppr_id"] == "PPR_2"
