"""Tests for europepmc_ftp module."""

import gzip
import io
from unittest.mock import MagicMock, patch

from sciencebeam_dataset_builder.europepmc_ftp import (
    BatchFile,
    _iter_articles_from_stream,
    get_batch_files,
    iter_articles_for_ids,
)

SAMPLE_INDEX_HTML = """
<html><body>
<a href="PPR1_PPR500.xml.gz">batch 1</a>
<a href="PPR501_PPR1000.xml.gz">batch 2</a>
</body></html>
"""


def _make_articles_xml(*ppr_ids: int) -> bytes:
    articles = "".join(
        f"<article><front><article-meta>"
        f'<article-id pub-id-type="archive">PPR{ppr_id}</article-id>'
        f"<title-group><article-title>Title {ppr_id}</article-title></title-group>"
        f"</article-meta></front></article>"
        for ppr_id in ppr_ids
    )
    return f"<articles>{articles}</articles>".encode()


def _gzip_xml(*ppr_ids: int) -> io.BytesIO:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(_make_articles_xml(*ppr_ids))
    buf.seek(0)
    return buf


class TestGetBatchFiles:
    def _mock_get(self, html: str):
        mock = MagicMock()
        mock.raise_for_status = MagicMock()
        mock.text = html
        return mock

    def test_parses_filenames_from_index(self):
        with patch("requests.get", return_value=self._mock_get(SAMPLE_INDEX_HTML)):
            result = get_batch_files()
        assert len(result) == 2

    def test_parses_start_and_end_ids(self):
        with patch("requests.get", return_value=self._mock_get(SAMPLE_INDEX_HTML)):
            result = get_batch_files()
        assert result[0].start_id == 1
        assert result[0].end_id == 500
        assert result[1].start_id == 501
        assert result[1].end_id == 1000

    def test_sorted_by_start_id(self):
        html = (
            '<a href="PPR501_PPR1000.xml.gz">batch 2</a>\n'
            '<a href="PPR1_PPR500.xml.gz">batch 1</a>'
        )
        with patch("requests.get", return_value=self._mock_get(html)):
            result = get_batch_files()
        assert result[0].start_id == 1
        assert result[1].start_id == 501

    def test_url_constructed_from_base_and_filename(self):
        html = '<a href="PPR1_PPR500.xml.gz">PPR1_PPR500.xml.gz</a>'
        with patch("requests.get", return_value=self._mock_get(html)):
            result = get_batch_files()
        assert result[0].url.endswith("/PPR1_PPR500.xml.gz")

    def test_empty_index_returns_empty_list(self):
        with patch("requests.get", return_value=self._mock_get("<html></html>")):
            assert get_batch_files() == []


class TestIterArticlesFromStream:
    def test_yields_matching_article(self):
        stream = io.BytesIO(_make_articles_xml(123))
        results = list(_iter_articles_from_stream(stream, {123}))
        assert len(results) == 1
        ppr_id, xml_str = results[0]
        assert ppr_id == 123

    def test_xml_output_contains_article_content(self):
        stream = io.BytesIO(_make_articles_xml(42))
        _, xml_str = next(iter(_iter_articles_from_stream(stream, {42})))
        assert "PPR42" in xml_str
        assert "Title 42" in xml_str

    def test_xml_output_has_declaration(self):
        stream = io.BytesIO(_make_articles_xml(1))
        _, xml_str = next(iter(_iter_articles_from_stream(stream, {1})))
        assert xml_str.startswith('<?xml version="1.0" encoding="UTF-8"?>')

    def test_skips_non_matching_articles(self):
        stream = io.BytesIO(_make_articles_xml(100, 200, 300))
        results = list(_iter_articles_from_stream(stream, {200}))
        assert len(results) == 1
        assert results[0][0] == 200

    def test_yields_multiple_matches(self):
        stream = io.BytesIO(_make_articles_xml(1, 2, 3))
        results = list(_iter_articles_from_stream(stream, {1, 3}))
        assert {r[0] for r in results} == {1, 3}

    def test_yields_nothing_when_no_match(self):
        stream = io.BytesIO(_make_articles_xml(100))
        assert list(_iter_articles_from_stream(stream, {999})) == []

    def test_stops_early_when_all_targets_found(self):
        # Article 1 matches; articles 2 and 3 would follow but should not be needed.
        stream = io.BytesIO(_make_articles_xml(1, 2, 3))
        results = list(_iter_articles_from_stream(stream, {1}))
        assert len(results) == 1


class TestIterArticlesForIds:
    def test_skips_batch_when_id_out_of_range(self):
        batch = [BatchFile(url="http://x/PPR1_PPR100.xml.gz", start_id=1, end_id=100)]
        with patch("requests.get") as mock_get:
            list(iter_articles_for_ids(batch, {500}))
        mock_get.assert_not_called()

    def test_returns_empty_when_no_batches(self):
        assert list(iter_articles_for_ids([], {42})) == []

    def test_streams_matching_batch_and_yields_article(self):
        batch = [BatchFile(url="http://x/PPR1_PPR100.xml.gz", start_id=1, end_id=100)]
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.raw = _gzip_xml(42)

        with patch("requests.get", return_value=mock_response):
            results = list(iter_articles_for_ids(batch, {42}))

        assert len(results) == 1
        assert results[0][0] == 42

    def test_only_requests_batches_containing_target_ids(self):
        batches = [
            BatchFile(url="http://x/PPR1_PPR100.xml.gz", start_id=1, end_id=100),
            BatchFile(url="http://x/PPR101_PPR200.xml.gz", start_id=101, end_id=200),
        ]
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.raw = _gzip_xml(150)

        with patch("requests.get", return_value=mock_response) as mock_get:
            list(iter_articles_for_ids(batches, {150}))

        assert mock_get.call_count == 1
        assert "PPR101_PPR200" in mock_get.call_args[0][0]
