"""Tests for corpus.attributions — whom to credit for each document."""

import csv

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.corpus.attributions import (
    AttributionError,
    attribution,
    render_attributions,
)


def _xml(meta: str, back: str = "") -> str:
    return (
        '<article xmlns:xlink="http://www.w3.org/1999/xlink"><front><article-meta>'
        f"{meta}</article-meta></front><back>{back}</back></article>"
    )


def _author(name: str, **attributes: str) -> str:
    attrs = "".join(f' {k.replace("_", "-")}="{v}"' for k, v in attributes.items())
    return f'<contrib contrib-type="author"{attrs}>{name}</contrib>'


def _name(given: str, surname: str, style: str = "") -> str:
    attr = f' name-style="{style}"' if style else ""
    return (
        f"<name{attr}><surname>{surname}</surname>"
        f"<given-names>{given}</given-names></name>"
    )


def _row(xml: str, doi: str | None = "10.1/a") -> dict:
    return {"id": "a", "doi": doi, "licence": "CC BY 4.0", "xml": xml}


class TestAttribution:
    def test_the_title_flattens_its_markup(self):
        meta = (
            "<title-group><article-title>The <italic>H</italic><sub>2</sub>O\n"
            "  cycle</article-title></title-group>"
        )
        assert attribution(_row(_xml(meta))).title == "The H2O cycle"

    def test_authors_are_given_name_first_in_document_order(self):
        meta = (
            "<contrib-group>"
            + _author(_name("Ana", "Silva"))
            + _author(_name("Wei", "Zhang", style="eastern"))
            + "</contrib-group>"
        )
        assert attribution(_row(_xml(meta))).authors == "Ana Silva; Zhang Wei"

    def test_string_names_and_groups_are_authors_too(self):
        meta = (
            "<contrib-group>"
            + _author("<string-name>J. Doe</string-name>")
            + _author("<collab>The Consortium</collab>")
            + "</contrib-group>"
        )
        assert attribution(_row(_xml(meta))).authors == "J. Doe; The Consortium"

    def test_editors_and_cited_authors_are_not(self):
        meta = (
            "<contrib-group>"
            + _author(_name("Ana", "Silva"))
            + '<contrib contrib-type="editor">'
            + _name("Ed", "Itor")
            + "</contrib></contrib-group>"
        )
        back = f"<ref-list><ref>{_author(_name('Cited', 'Person'))}</ref></ref-list>"
        assert attribution(_row(_xml(meta, back))).authors == "Ana Silva"

    def test_xml_that_cannot_be_parsed_gives_empty_fields(self):
        credit = attribution(_row("<article><front>"))
        assert (credit.title, credit.authors, credit.licence) == ("", "", "CC BY 4.0")

    def test_a_missing_doi_is_empty(self):
        assert attribution(_row(_xml(""), doi=None)).doi == ""


class TestRender:
    def _table(self, ids: list[str]) -> pa.Table:
        meta = "<title-group><article-title>Growth, form</article-title></title-group>"
        return pa.table(
            {
                "id": ids,
                "doi": [f"10.1/{i}" for i in ids],
                "licence": ["CC BY 4.0"] * len(ids),
                "xml": [_xml(meta)] * len(ids),
                "pdf": [b"pdf"] * len(ids),
            }
        )

    def test_rows_follow_the_release_order_across_tables(self):
        text = render_attributions(
            ["c", "a", "b"], [self._table(["a", "b"]), self._table(["c"])]
        )
        rows = list(csv.DictReader(text.splitlines()))
        assert [r["id"] for r in rows] == ["c", "a", "b"]
        assert list(rows[0]) == ["id", "doi", "title", "authors", "licence"]

    def test_a_comma_in_a_field_is_quoted(self):
        text = render_attributions(["a"], [self._table(["a"])])
        assert '"Growth, form"' in text

    def test_a_member_with_no_row_is_refused(self):
        with pytest.raises(AttributionError, match="no row for 1"):
            render_attributions(["a", "z"], [self._table(["a"])])
