"""Tests for scielo_preprints.metadata_cli module."""

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from sciencebeam_dataset_builder.scielo_preprints.metadata_cli import (
    LANGUAGE_NORMALISATION,
    extract_metadata,
    main,
    parse_args,
    _extract_language,
    _extract_article_meta,
)

ALI_NS = "http://www.niso.org/schemas/ali/1.0/"


def _write_xml(
    path: Path,
    lang: str | None = "pt",
    lang_attr: str = "xml:lang",
    article_type: str = "preprint",
    doi: str = "",
    version: str = "",
    title: str = "",
    authors: list[str] | None = None,
    pub_date: str = "",
    license_url: str = "",
    categories: dict[str, str] | None = None,
    keywords: list[str] | None = None,
) -> None:
    """Write a minimal article XML file with optional metadata.

    authors is a list of "Given Surname" strings (simple form, no ORCID/affiliations).
    For richer author tests use inline XML via _extract_article_meta directly.
    """
    lang_part = f' {lang_attr}="{lang}"' if lang is not None else ""
    type_part = f' article-type="{article_type}"' if article_type else ""

    doi_xml = f'<article-id pub-id-type="doi">{doi}</article-id>' if doi else ""
    version_xml = (
        f"<article-version-alternatives>"
        f'<article-version article-version-type="number">{version}</article-version>'
        f"</article-version-alternatives>"
        if version
        else ""
    )
    title_xml = (
        f"<title-group><article-title>{title}</article-title></title-group>"
        if title
        else ""
    )
    authors_xml = ""
    if authors:
        contribs = "".join(
            f"<contrib contrib-type='author'><name>"
            f"<given-names>{a.split()[0]}</given-names>"
            f"<surname>{' '.join(a.split()[1:])}</surname>"
            f"</name></contrib>"
            for a in authors
        )
        authors_xml = f"<contrib-group>{contribs}</contrib-group>"
    pub_date_xml = ""
    if pub_date:
        y, m, d = pub_date.split("-")
        pub_date_xml = (
            f'<pub-date pub-type="preprint">'
            f"<year>{y}</year><month>{m}</month><day>{d}</day>"
            f"</pub-date>"
        )
    license_xml = ""
    if license_url:
        license_xml = (
            f'<permissions xmlns:ali="{ALI_NS}">'
            f"<license>"
            f"<ali:license_ref>{license_url}</ali:license_ref>"
            f"</license>"
            f"</permissions>"
        )
    categories_xml = ""
    if categories:
        groups = "".join(
            f'<subj-group subj-group-type="{sg_type}">'
            f"<subject>{subject}</subject>"
            f"</subj-group>"
            for sg_type, subject in categories.items()
        )
        categories_xml = f"<article-categories>{groups}</article-categories>"
    keywords_xml = ""
    if keywords:
        kwds = "".join(f"<kwd>{kw}</kwd>" for kw in keywords)
        keywords_xml = f'<kwd-group xml:lang="en">{kwds}</kwd-group>'

    path.write_text(
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<article{lang_part}{type_part}>"
        f"<front><article-meta>"
        f"{doi_xml}{version_xml}{title_xml}{authors_xml}{pub_date_xml}"
        f"{license_xml}{categories_xml}{keywords_xml}"
        f"</article-meta></front>"
        f"</article>",
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


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


class TestExtractArticleMeta:
    def _meta(self, xml: str) -> ET.Element:
        return ET.fromstring(
            f"<article><front><article-meta>{xml}</article-meta></front></article>"
        )

    def test_extracts_doi(self):
        root = self._meta('<article-id pub-id-type="doi">10.1234/test</article-id>')
        assert _extract_article_meta(root)["doi"] == "10.1234/test"

    def test_extracts_version(self):
        root = self._meta(
            "<article-version-alternatives>"
            '<article-version article-version-type="number">2</article-version>'
            "</article-version-alternatives>"
        )
        assert _extract_article_meta(root)["version"] == "2"

    def test_extracts_title(self):
        root = self._meta(
            "<title-group><article-title>My Title</article-title></title-group>"
        )
        assert _extract_article_meta(root)["title"] == "My Title"

    def test_extracts_authors_as_list_of_dicts(self):
        root = self._meta(
            "<contrib-group>"
            "<contrib contrib-type='author'><name>"
            "<given-names>Jane</given-names><surname>Doe</surname>"
            "</name></contrib>"
            "<contrib contrib-type='author'><name>"
            "<given-names>John</given-names><surname>Smith</surname>"
            "</name></contrib>"
            "</contrib-group>"
        )
        authors = _extract_article_meta(root)["authors"]
        assert [a["name"] for a in authors] == ["Jane Doe", "John Smith"]

    def test_extracts_author_orcid(self):
        root = self._meta(
            "<contrib-group>"
            "<contrib contrib-type='author'>"
            "<contrib-id contrib-id-type='orcid'>https://orcid.org/0000-0001-2345-6789</contrib-id>"
            "<name><given-names>Jane</given-names><surname>Doe</surname></name>"
            "</contrib>"
            "</contrib-group>"
        )
        author = _extract_article_meta(root)["authors"][0]
        assert author["orcid"] == "https://orcid.org/0000-0001-2345-6789"

    def test_author_orcid_empty_when_absent(self):
        root = self._meta(
            "<contrib-group>"
            "<contrib contrib-type='author'><name>"
            "<given-names>Jane</given-names><surname>Doe</surname>"
            "</name></contrib>"
            "</contrib-group>"
        )
        assert _extract_article_meta(root)["authors"][0]["orcid"] == ""

    def test_extracts_author_affiliations_via_xref(self):
        root = self._meta(
            '<aff id="A1"><label>1</label>University of Somewhere</aff>'
            '<aff id="A2"><label>2</label>Institute of Things</aff>'
            "<contrib-group>"
            "<contrib contrib-type='author'>"
            "<name><given-names>Jane</given-names><surname>Doe</surname></name>"
            '<xref ref-type="aff" rid="A1">1</xref>'
            "</contrib>"
            "<contrib contrib-type='author'>"
            "<name><given-names>John</given-names><surname>Smith</surname></name>"
            '<xref ref-type="aff" rid="A1">1</xref>'
            '<xref ref-type="aff" rid="A2">2</xref>'
            "</contrib>"
            "</contrib-group>"
        )
        authors = _extract_article_meta(root)["authors"]
        assert authors[0]["affiliations"] == ["University of Somewhere"]
        assert authors[1]["affiliations"] == [
            "University of Somewhere",
            "Institute of Things",
        ]

    def test_extracts_affiliation_directly_inside_contrib(self):
        """<aff> as a direct child of <contrib> (no xref needed)."""
        root = self._meta(
            "<contrib-group>"
            "<contrib contrib-type='author'>"
            "<name><given-names>Jane</given-names><surname>Doe</surname></name>"
            "<aff>University of Somewhere</aff>"
            "</contrib>"
            "</contrib-group>"
        )
        assert _extract_article_meta(root)["authors"][0]["affiliations"] == [
            "University of Somewhere"
        ]

    def test_author_affiliations_empty_when_no_xref(self):
        root = self._meta(
            "<contrib-group>"
            "<contrib contrib-type='author'><name>"
            "<given-names>Jane</given-names><surname>Doe</surname>"
            "</name></contrib>"
            "</contrib-group>"
        )
        assert _extract_article_meta(root)["authors"][0]["affiliations"] == []

    def test_extracts_pub_date(self):
        root = self._meta(
            '<pub-date pub-type="preprint">'
            "<year>2022</year><month>3</month><day>5</day>"
            "</pub-date>"
        )
        assert _extract_article_meta(root)["pub_date"] == "2022-03-05"

    def test_extracts_license_url(self):
        root = self._meta(
            f'<permissions xmlns:ali="{ALI_NS}">'
            f"<license>"
            f"<ali:license_ref>https://creativecommons.org/licenses/by/4.0/</ali:license_ref>"
            f"</license>"
            f"</permissions>"
        )
        assert (
            _extract_article_meta(root)["license"]
            == "https://creativecommons.org/licenses/by/4.0/"
        )

    def test_license_empty_when_absent(self):
        root = ET.fromstring("<article><front><article-meta/></front></article>")
        assert _extract_article_meta(root)["license"] == ""

    def test_extracts_subject_categories(self):
        root = self._meta(
            "<article-categories>"
            '<subj-group subj-group-type="heading"><subject>Article</subject></subj-group>'
            '<subj-group subj-group-type="europepmc-category"><subject>Covid-19</subject></subj-group>'
            "</article-categories>"
        )
        result = _extract_article_meta(root)
        assert result["subject_heading"] == "Article"
        assert result["subject_europepmc_category"] == "Covid-19"

    def test_no_subject_fields_when_categories_absent(self):
        root = ET.fromstring("<article><front><article-meta/></front></article>")
        result = _extract_article_meta(root)
        assert "subject_heading" not in result
        assert "subject_europepmc_category" not in result

    def test_returns_empty_values_when_fields_missing(self):
        root = ET.fromstring("<article><front><article-meta/></front></article>")
        result = _extract_article_meta(root)
        assert result["doi"] == ""
        assert result["version"] == ""
        assert result["title"] == ""
        assert result["authors"] == []
        assert result["pub_date"] == ""
        assert result["license"] == ""
        assert result["keywords"] == []


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

    def test_returns_article_type(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, article_type="preprint")
        assert extract_metadata(xml_path)["article_type"] == "preprint"

    def test_returns_doi(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, doi="10.1234/test")
        assert extract_metadata(xml_path)["doi"] == "10.1234/test"

    def test_returns_version(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, version="3")
        assert extract_metadata(xml_path)["version"] == "3"

    def test_returns_title(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, title="My Article")
        assert extract_metadata(xml_path)["title"] == "My Article"

    def test_returns_authors_as_list_of_dicts(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, authors=["Jane Doe", "John Smith"])
        authors = extract_metadata(xml_path)["authors"]
        assert [a["name"] for a in authors] == ["Jane Doe", "John Smith"]

    def test_returns_pub_date(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, pub_date="2022-03-05")
        assert extract_metadata(xml_path)["pub_date"] == "2022-03-05"

    def test_returns_license(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, license_url="https://creativecommons.org/licenses/by/4.0/")
        assert (
            extract_metadata(xml_path)["license"]
            == "https://creativecommons.org/licenses/by/4.0/"
        )

    def test_returns_subject_categories(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(
            xml_path,
            categories={"heading": "Article", "europepmc-category": "Covid-19"},
        )
        row = extract_metadata(xml_path)
        assert row["subject_heading"] == "Article"
        assert row["subject_europepmc_category"] == "Covid-19"

    def test_returns_keywords(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path, keywords=["COVID-19", "Paraguay", "Attitudes"])
        assert extract_metadata(xml_path)["keywords"] == [
            "COVID-19",
            "Paraguay",
            "Attitudes",
        ]

    def test_returns_empty_keywords_when_absent(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path)
        assert extract_metadata(xml_path)["keywords"] == []


class TestExtractMetadataProvenance:
    def test_provenance_fields_none_when_no_sidecar(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path)
        row = extract_metadata(xml_path)
        assert row["xml_source_url"] is None
        assert row["xml_downloaded_at"] is None
        assert row["xml_ftfy_applied"] is None
        assert row["pdf_source_url"] is None
        assert row["pdf_downloaded_at"] is None

    def test_provenance_fields_populated_from_sidecar(self, tmp_path):
        xml_path = tmp_path / "PPR_1.xml"
        _write_xml(xml_path)
        sidecar = {
            "xml_source_url": "https://europepmc.org/ftp/preprint_fulltext/PPR1_PPR100.xml.gz",
            "xml_downloaded_at": "2024-01-15T10:30:00+00:00",
            "xml_ftfy_applied": True,
            "pdf_source_url": "https://example.com/PPR1.pdf",
            "pdf_downloaded_at": "2024-01-15T10:30:05+00:00",
        }
        (tmp_path / "PPR_1.provenance.json").write_text(
            json.dumps(sidecar), encoding="utf-8"
        )
        row = extract_metadata(xml_path)
        assert row["xml_source_url"] == sidecar["xml_source_url"]
        assert row["xml_downloaded_at"] == sidecar["xml_downloaded_at"]
        assert row["xml_ftfy_applied"] is True
        assert row["pdf_source_url"] == sidecar["pdf_source_url"]
        assert row["pdf_downloaded_at"] == sidecar["pdf_downloaded_at"]


class TestParseArgs:
    def test_requires_input_dir_and_output_jsonl(self):
        with pytest.raises(SystemExit):
            parse_args([])

    def test_parses_positional_args(self, tmp_path):
        args = parse_args([str(tmp_path), str(tmp_path / "out.jsonl")])
        assert args.input_dir == tmp_path
        assert args.output_jsonl == tmp_path / "out.jsonl"

    def test_debug_default_false(self, tmp_path):
        args = parse_args([str(tmp_path), str(tmp_path / "out.jsonl")])
        assert args.debug is False


class TestMain:
    def test_writes_jsonl_with_one_record_per_xml(self, tmp_path):
        for i in (1, 2, 3):
            _write_xml(tmp_path / f"PPR_{i}.xml", lang="pt")
        out = tmp_path / "metadata.jsonl"
        main([str(tmp_path), str(out)])
        records = _read_jsonl(out)
        assert len(records) == 3

    def test_jsonl_contains_expected_fields(self, tmp_path):
        _write_xml(
            tmp_path / "PPR_42.xml",
            lang="es",
            categories={"heading": "Article", "europepmc-category": "Covid-19"},
        )
        out = tmp_path / "metadata.jsonl"
        main([str(tmp_path), str(out)])
        record = _read_jsonl(out)[0]
        assert set(record.keys()) == {
            "ppr_id",
            "doi",
            "version",
            "article_type",
            "language",
            "language_raw",
            "title",
            "authors",
            "pub_date",
            "license",
            "keywords",
            "subject_heading",
            "subject_europepmc_category",
            "xml_source_url",
            "xml_downloaded_at",
            "xml_ftfy_applied",
            "pdf_source_url",
            "pdf_downloaded_at",
        }

    def test_authors_is_list_of_dicts_in_jsonl(self, tmp_path):
        _write_xml(tmp_path / "PPR_1.xml", authors=["Jane Doe", "John Smith"])
        out = tmp_path / "metadata.jsonl"
        main([str(tmp_path), str(out)])
        record = _read_jsonl(out)[0]
        authors = record["authors"]
        assert isinstance(authors, list)
        assert [a["name"] for a in authors] == ["Jane Doe", "John Smith"]

    def test_exits_when_no_xml_files(self, tmp_path):
        with pytest.raises(SystemExit):
            main([str(tmp_path), str(tmp_path / "out.jsonl")])

    def test_skips_unparseable_xml(self, tmp_path):
        (tmp_path / "PPR_1.xml").write_text("not xml", encoding="utf-8")
        _write_xml(tmp_path / "PPR_2.xml", lang="pt")
        out = tmp_path / "metadata.jsonl"
        main([str(tmp_path), str(out)])
        records = _read_jsonl(out)
        assert len(records) == 1
        assert records[0]["ppr_id"] == "PPR_2"
