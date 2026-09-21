import pytest

from sciencebeam_dataset_builder.dataset.license.extract_license import (
    ALL_RIGHTS_RESERVED,
    CONFLICTING_LICENCES,
    NO_LICENCE_ABSENT,
    NO_LICENCE_COPYRIGHT_ONLY,
    NO_LICENCE_PLACEHOLDER,
    UNPARSEABLE,
    creative_commons_label,
    extract_licence,
    parse_xml,
)

JATS = """<article xmlns:xlink="http://www.w3.org/1999/xlink">
<front><article-meta><permissions>{permissions}</permissions></article-meta></front>
<body><p>text</p></body></article>"""

DUBLIN_CORE = """<oai_dc:dc
 xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
 xmlns:dc="http://purl.org/dc/elements/1.1/">{rights}</oai_dc:dc>"""


def jats(permissions: str) -> str:
    return JATS.format(permissions=permissions)


class TestCreativeCommonsLabel:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://creativecommons.org/licenses/by/4.0/", "CC BY 4.0"),
            ("http://creativecommons.org/licenses/by-nc-nd/4.0", "CC BY-NC-ND 4.0"),
            ("https://creativecommons.org/publicdomain/zero/1.0/", "CC0 1.0"),
            ("https://creativecommons.org/publicdomain/mark/1.0/", "CC0 1.0"),
            ("https://creativecommons.org/licenses/by", "CC BY"),
        ],
    )
    def test_reads_the_cc_url(self, url, expected):
        assert creative_commons_label(url) == expected

    def test_returns_none_without_a_cc_url(self):
        assert creative_commons_label("© 2020 The Authors") is None

    def test_returns_none_for_empty_text(self):
        assert creative_commons_label("") is None


class TestExtractLicence:
    def test_reads_a_jats_license_href(self):
        xml = jats(
            '<license xlink:href="https://creativecommons.org/licenses/by/4.0/">'
            "<license-p>Open access.</license-p></license>"
        )
        assert extract_licence(xml).label == "CC BY 4.0"

    def test_reads_an_ali_license_ref(self):
        xml = jats(
            '<license><ali:license_ref xmlns:ali="http://www.niso.org/schemas/ali/1.0/">'
            "https://creativecommons.org/licenses/by-nc/4.0/</ali:license_ref></license>"
        )
        assert extract_licence(xml).label == "CC BY-NC 4.0"

    def test_reads_a_dublin_core_rights_url(self):
        xml = DUBLIN_CORE.format(
            rights="<dc:rights>https://creativecommons.org/licenses/by/4.0</dc:rights>"
        )
        assert extract_licence(xml).label == "CC BY 4.0"

    def test_a_copyright_line_beside_the_url_is_not_a_conflict(self):
        xml = DUBLIN_CORE.format(
            rights=(
                "<dc:rights>https://creativecommons.org/licenses/by/4.0</dc:rights>"
                "<dc:rights>Copyright (c) 2020 A. Author</dc:rights>"
            )
        )
        assert extract_licence(xml).label == "CC BY 4.0"

    def test_duplicated_agreeing_statements_collapse(self):
        xml = jats(
            '<license xlink:href="https://creativecommons.org/licenses/by/4.0/"/>'
            '<license xlink:href="https://creativecommons.org/licenses/by/4.0/"/>'
        )
        assert extract_licence(xml).label == "CC BY 4.0"

    def test_genuinely_different_licences_conflict(self):
        xml = jats(
            '<license xlink:href="https://creativecommons.org/licenses/by/4.0/"/>'
            '<license xlink:href="https://creativecommons.org/licenses/by-nc/4.0/"/>'
        )
        licence = extract_licence(xml)
        assert licence.label == CONFLICTING_LICENCES
        assert "CC BY 4.0" in licence.evidence
        assert "CC BY-NC 4.0" in licence.evidence

    def test_reads_an_explicit_reservation_of_rights(self):
        xml = jats(
            "<copyright-statement>© 2023, Cold Spring Harbor Laboratory"
            "</copyright-statement><license><p>The copyright holder is the author. "
            "All rights reserved. The material may not be redistributed.</p></license>"
        )
        assert extract_licence(xml).label == ALL_RIGHTS_RESERVED

    def test_an_unsubstituted_placeholder_is_not_a_copyright_statement(self):
        xml = jats(
            "<copyright-statement>&#x00A9; 2015 copyright-statement"
            "</copyright-statement><copyright-year>2015</copyright-year>"
        )
        licence = extract_licence(xml)
        assert licence.label == NO_LICENCE_PLACEHOLDER
        assert not licence.is_stated

    def test_a_real_copyright_statement_without_a_licence(self):
        xml = jats("<copyright-statement>© 2019 The Authors</copyright-statement>")
        licence = extract_licence(xml)
        assert licence.label == NO_LICENCE_COPYRIGHT_ONLY
        assert licence.evidence == "© 2019 The Authors"

    def test_no_permissions_element_at_all(self):
        xml = "<article><front/><body><p>text</p></body></article>"
        assert extract_licence(xml).label == NO_LICENCE_ABSENT

    def test_recovers_from_an_undefined_html_entity(self):
        xml = jats(
            '<license xlink:href="https://creativecommons.org/licenses/by/4.0/"/>'
        ).replace("<p>text</p>", "<p>37&deg;C and &alpha;-helix</p>")
        assert extract_licence(xml).label == "CC BY 4.0"

    def test_reports_xml_that_cannot_be_recovered(self):
        assert extract_licence("<article><unclosed></article>").label == UNPARSEABLE


class TestParseXml:
    def test_returns_none_for_unrecoverable_xml(self):
        assert parse_xml("<a><b></a>") is None

    def test_strips_illegal_control_characters(self):
        assert parse_xml("<a>text\x0bmore</a>") is not None
