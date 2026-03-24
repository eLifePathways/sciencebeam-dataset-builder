"""Tests for scielo_preprints_cli module."""

from unittest.mock import MagicMock, patch

import pytest

from sciencebeam_dataset_builder.scielo_preprints_cli import main, parse_args

MODULE = "sciencebeam_dataset_builder.scielo_preprints_cli"


def _patch_api(articles=(), count=None, batch_files=(), xml_pairs=()):
    """Return a context manager that patches all external I/O in the CLI."""
    if count is None:
        count = len(list(articles))
    return [
        patch(f"{MODULE}.count_scielo_preprints", return_value=count),
        patch(f"{MODULE}.iter_scielo_preprints", return_value=list(articles)),
        patch(f"{MODULE}.get_batch_files", return_value=list(batch_files)),
        patch(f"{MODULE}.iter_articles_for_ids", return_value=list(xml_pairs)),
    ]


def _article(ppr_id: int, *, has_pdf: bool = False) -> dict:
    article: dict = {"id": f"PPR{ppr_id}", "title": f"Article {ppr_id}"}
    if has_pdf:
        article["fullTextUrlList"] = {
            "fullTextUrl": [
                {
                    "documentStyle": "pdf",
                    "site": "Europe_PMC",
                    "url": f"https://example.com/PPR{ppr_id}.pdf",
                }
            ]
        }
    return article


class TestParseArgs:
    def test_output_dir_is_required(self):
        with pytest.raises(SystemExit):
            parse_args([])

    def test_output_dir_is_converted_to_path(self, tmp_path):
        args = parse_args([str(tmp_path)])
        assert args.output_dir == tmp_path

    def test_defaults(self, tmp_path):
        args = parse_args([str(tmp_path)])
        assert args.limit is None
        assert args.query == ""
        assert args.page_size == 100
        assert args.skip_existing is True
        assert args.debug is False

    def test_limit_option(self, tmp_path):
        args = parse_args([str(tmp_path), "--limit", "5"])
        assert args.limit == 5

    def test_no_skip_existing_flag(self, tmp_path):
        args = parse_args([str(tmp_path), "--no-skip-existing"])
        assert args.skip_existing is False

    def test_debug_flag(self, tmp_path):
        args = parse_args([str(tmp_path), "--debug"])
        assert args.debug is True

    def test_query_option(self, tmp_path):
        args = parse_args([str(tmp_path), "--query", "LANG:eng"])
        assert args.query == "LANG:eng"

    def test_page_size_option(self, tmp_path):
        args = parse_args([str(tmp_path), "--page-size", "50"])
        assert args.page_size == 50


class TestMain:
    def test_creates_scielo_preprints_subdir(self, tmp_path):
        patches = _patch_api()
        with patches[0], patches[1], patches[2], patches[3]:
            main([str(tmp_path)])
        assert (tmp_path / "scielo-preprints").is_dir()

    def test_no_batch_fetch_when_nothing_to_download(self, tmp_path):
        patches = _patch_api()
        with patches[0], patches[1], patches[2] as mock_batch, patches[3]:
            main([str(tmp_path)])
        mock_batch.assert_not_called()

    def test_writes_xml_file(self, tmp_path):
        xml_content = "<article><front/></article>"
        patches = _patch_api(
            articles=[_article(123)],
            batch_files=["batch"],
            xml_pairs=[(123, xml_content)],
        )
        with patches[0], patches[1], patches[2], patches[3]:
            main([str(tmp_path)])
        xml_path = tmp_path / "scielo-preprints" / "PPR_123.xml"
        assert xml_path.exists()
        assert xml_path.read_text() == xml_content

    def test_skips_existing_xml(self, tmp_path):
        output_dir = tmp_path / "scielo-preprints"
        output_dir.mkdir()
        existing = output_dir / "PPR_123.xml"
        existing.write_text("original")

        patches = _patch_api(articles=[_article(123)])
        with patches[0], patches[1], patches[2] as mock_batch, patches[3]:
            main([str(tmp_path)])

        mock_batch.assert_not_called()
        assert existing.read_text() == "original"

    def test_no_skip_existing_still_fetches_batch_for_existing_files(self, tmp_path):
        # --no-skip-existing keeps the article in the queue so the batch index is fetched,
        # but the write is still guarded by _xml_needed (file exists → no overwrite).
        output_dir = tmp_path / "scielo-preprints"
        output_dir.mkdir()
        existing = output_dir / "PPR_123.xml"
        existing.write_text("original")

        patches = _patch_api(
            articles=[_article(123)],
            batch_files=["batch"],
            xml_pairs=[(123, "<article>new</article>")],
        )
        with patches[0], patches[1], patches[2] as mock_batch, patches[3]:
            main([str(tmp_path), "--no-skip-existing"])

        mock_batch.assert_called_once()
        assert existing.read_text() == "original"

    def test_limit_caps_number_of_downloads(self, tmp_path):
        articles = [_article(i) for i in range(1, 4)]
        xml_pairs = [(i, f"<article>{i}</article>") for i in range(1, 4)]
        patches = _patch_api(
            articles=articles, batch_files=["batch"], xml_pairs=xml_pairs
        )
        with patches[0], patches[1], patches[2], patches[3]:
            main([str(tmp_path), "--limit", "1"])

        written = list((tmp_path / "scielo-preprints").glob("*.xml"))
        assert len(written) == 1

    def test_downloads_pdf_when_available(self, tmp_path):
        xml_content = "<article/>"
        mock_pdf_response = MagicMock()
        mock_pdf_response.content = b"%PDF-fake"
        mock_pdf_response.raise_for_status = MagicMock()

        patches = _patch_api(
            articles=[_article(123, has_pdf=True)],
            batch_files=["batch"],
            xml_pairs=[(123, xml_content)],
        )
        with patches[0], patches[1], patches[2], patches[3]:
            with patch("requests.get", return_value=mock_pdf_response):
                main([str(tmp_path)])

        pdf_path = tmp_path / "scielo-preprints" / "PPR_123.pdf"
        assert pdf_path.exists()
        assert pdf_path.read_bytes() == b"%PDF-fake"

    def test_continues_after_pdf_download_failure(self, tmp_path):
        import requests

        xml_content = "<article/>"
        patches = _patch_api(
            articles=[_article(123, has_pdf=True)],
            batch_files=["batch"],
            xml_pairs=[(123, xml_content)],
        )
        with patches[0], patches[1], patches[2], patches[3]:
            with patch(
                "requests.get", side_effect=requests.RequestException("timeout")
            ):
                main([str(tmp_path)])  # should not raise

        xml_path = tmp_path / "scielo-preprints" / "PPR_123.xml"
        assert xml_path.exists()

    def test_extra_query_is_forwarded(self, tmp_path):
        patches = _patch_api()
        with patches[0] as mock_count, patches[1], patches[2], patches[3]:
            main([str(tmp_path), "--query", "LANG:eng"])
        call_query = mock_count.call_args[0][0]
        assert "LANG:eng" in call_query

    def test_non_ppr_ids_are_ignored(self, tmp_path):
        articles = [{"id": "MED12345", "title": "Not a preprint"}]
        patches = _patch_api(articles=articles)
        with patches[0], patches[1], patches[2] as mock_batch, patches[3]:
            main([str(tmp_path)])
        mock_batch.assert_not_called()
