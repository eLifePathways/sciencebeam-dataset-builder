"""Tests for europepmc_api module."""

from unittest.mock import MagicMock, patch


from sciencebeam_dataset_builder.scielo_preprints.europepmc_api import (
    count_scielo_preprints,
    get_pdf_url,
    iter_scielo_preprints,
)


class TestGetPdfUrl:
    def test_returns_url_for_europe_pmc_pdf(self):
        article = {
            "fullTextUrlList": {
                "fullTextUrl": [
                    {
                        "documentStyle": "pdf",
                        "site": "Europe_PMC",
                        "url": "https://example.com/article.pdf",
                    }
                ]
            }
        }
        assert get_pdf_url(article) == "https://example.com/article.pdf"

    def test_ignores_non_pdf_entries(self):
        article = {
            "fullTextUrlList": {
                "fullTextUrl": [
                    {
                        "documentStyle": "html",
                        "site": "Europe_PMC",
                        "url": "https://example.com/article.html",
                    }
                ]
            }
        }
        assert get_pdf_url(article) is None

    def test_ignores_non_europe_pmc_site(self):
        article = {
            "fullTextUrlList": {
                "fullTextUrl": [
                    {
                        "documentStyle": "pdf",
                        "site": "OtherSite",
                        "url": "https://example.com/article.pdf",
                    }
                ]
            }
        }
        assert get_pdf_url(article) is None

    def test_returns_none_for_empty_article(self):
        assert get_pdf_url({}) is None

    def test_returns_none_when_full_text_url_list_is_empty(self):
        assert get_pdf_url({"fullTextUrlList": {"fullTextUrl": []}}) is None

    def test_returns_first_matching_url_when_multiple_entries(self):
        article = {
            "fullTextUrlList": {
                "fullTextUrl": [
                    {
                        "documentStyle": "html",
                        "site": "Europe_PMC",
                        "url": "https://example.com/article.html",
                    },
                    {
                        "documentStyle": "pdf",
                        "site": "Europe_PMC",
                        "url": "https://example.com/article.pdf",
                    },
                ]
            }
        }
        assert get_pdf_url(article) == "https://example.com/article.pdf"


class TestCountScieloPreprints:
    def _mock_get(self, hit_count: int) -> MagicMock:
        mock = MagicMock()
        mock.raise_for_status = MagicMock()
        mock.json.return_value = {"hitCount": hit_count}
        return mock

    def test_returns_hit_count(self):
        with patch("requests.get", return_value=self._mock_get(42)):
            assert count_scielo_preprints() == 42

    def test_returns_zero_when_missing(self):
        mock = MagicMock()
        mock.raise_for_status = MagicMock()
        mock.json.return_value = {}
        with patch("requests.get", return_value=mock):
            assert count_scielo_preprints() == 0

    def test_base_query_contains_scielo_preprints(self):
        with patch("requests.get", return_value=self._mock_get(0)) as mock_get:
            count_scielo_preprints()
        query = mock_get.call_args[1]["params"]["query"]
        assert "SciELO Preprints" in query

    def test_extra_query_is_appended(self):
        with patch("requests.get", return_value=self._mock_get(0)) as mock_get:
            count_scielo_preprints(extra_query="IN_EPMC:Y")
        query = mock_get.call_args[1]["params"]["query"]
        assert "SciELO Preprints" in query
        assert "IN_EPMC:Y" in query

    def test_no_extra_query_omits_and_clause(self):
        with patch("requests.get", return_value=self._mock_get(0)) as mock_get:
            count_scielo_preprints()
        query = mock_get.call_args[1]["params"]["query"]
        assert " AND " not in query


class TestIterScieloPreprints:
    def _make_response(
        self, results: list[dict[str, str]], next_cursor: str | None = None
    ) -> MagicMock:
        mock = MagicMock()
        mock.raise_for_status = MagicMock()
        data: dict = {"resultList": {"result": results}}
        if next_cursor is not None:
            data["nextCursorMark"] = next_cursor
        mock.json.return_value = data
        return mock

    def test_yields_results_from_single_page(self):
        articles = [{"id": "PPR1"}, {"id": "PPR2"}]
        with patch("requests.get", return_value=self._make_response(articles)):
            assert list(iter_scielo_preprints()) == articles

    def test_follows_cursor_pagination(self):
        page1 = [{"id": "PPR1"}]
        page2 = [{"id": "PPR2"}]
        responses = [
            self._make_response(page1, next_cursor="cursor1"),
            self._make_response(page2),
        ]
        with patch("requests.get", side_effect=responses):
            assert list(iter_scielo_preprints()) == page1 + page2

    def test_stops_when_next_cursor_unchanged(self):
        page1 = [{"id": "PPR1"}]
        with patch(
            "requests.get", return_value=self._make_response(page1, next_cursor="*")
        ):
            assert list(iter_scielo_preprints()) == page1

    def test_stops_when_results_empty(self):
        with patch("requests.get", return_value=self._make_response([])):
            assert list(iter_scielo_preprints()) == []

    def test_extra_query_included_in_request(self):
        with patch("requests.get", return_value=self._make_response([])) as mock_get:
            list(iter_scielo_preprints(extra_query="LANG:eng"))
        query = mock_get.call_args[1]["params"]["query"]
        assert "LANG:eng" in query
        assert "SciELO Preprints" in query
