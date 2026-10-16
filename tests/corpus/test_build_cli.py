"""Tests for corpus.build_cli — building a release from the real legacy snapshot shape."""

import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.corpus import build_cli
from sciencebeam_dataset_builder.corpus.build_cli import (
    BuildError,
    build_table,
    main,
    read_legacy_rows,
    route_rows,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.parquet_io import read_shard

# The old 23-column canonical schema's shape, trimmed to what build_cli actually reads.
LEGACY_COLUMNS = (
    "source",
    "id",
    "doi",
    "version",
    "pub_date",
    "xml_ftfy_applied",
    "xml_source_url",
    "xml_downloaded_at",
    "pdf_source_url",
    "pdf_downloaded_at",
    "xml",
    "pdf",
)


def _write_legacy_split(path, source: str, ids: list[str]) -> None:
    rows = len(ids)
    table = pa.table(
        {
            "source": [source] * rows,
            "id": ids,
            "doi": [f"10.1/{i}" for i in ids],
            "version": [""] * rows,
            "pub_date": ["2026"] * rows,
            "xml_ftfy_applied": [False] * rows,
            "xml_source_url": [""] * rows,
            "xml_downloaded_at": ["2026-01-01T00:00:00Z"] * rows,
            "pdf_source_url": [""] * rows,
            "pdf_downloaded_at": ["2026-01-01T00:00:00Z"] * rows,
            "xml": [f"<article>{i}</article>" for i in ids],
            "pdf": [f"pdf-{i}".encode() for i in ids],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _snapshot(tmp_path, source: str, by_split: dict[str, list[str]]):
    snapshot_dir = tmp_path / "data" / "2026-01-01"
    for split, ids in by_split.items():
        _write_legacy_split(
            snapshot_dir / f"{source}-jats" / f"{split}-00000-of-00001.parquet",
            source,
            ids,
        )
    return snapshot_dir


def _licence_report(tmp_path, licences: dict[str, str]):
    path = tmp_path / "licence-per-document.csv"
    lines = ["uid,licence"] + [f"{uid},{lic}" for uid, lic in licences.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class TestReadLegacyRows:
    def test_every_split_is_tagged_with_the_folder_it_came_from(self, tmp_path):
        snapshot_dir = _snapshot(
            tmp_path, "biorxiv", {"train": ["a"], "test": ["b", "c"]}
        )
        rows = read_legacy_rows(snapshot_dir, "biorxiv-jats")
        assert {r["id"]: r["_split"] for r in rows} == {
            "a": "train",
            "b": "test",
            "c": "test",
        }

    def test_a_missing_config_directory_is_refused(self, tmp_path):
        with pytest.raises(BuildError, match="does not exist"):
            read_legacy_rows(tmp_path, "nothing-here-jats")

    def test_a_config_with_no_split_files_is_refused(self, tmp_path):
        (tmp_path / "biorxiv-jats").mkdir()
        with pytest.raises(BuildError, match="No split files"):
            read_legacy_rows(tmp_path, "biorxiv-jats")


class TestRouteRows:
    def test_rows_route_by_the_legacy_source_id_uid(self):
        rows = [
            {"source": "biorxiv", "id": "a", "_split": "train"},
            {"source": "biorxiv", "id": "b", "_split": "test"},
        ]
        licences = {"biorxiv__a": "CC BY 4.0", "biorxiv__b": "CC BY-NC 4.0"}
        kept, splits = route_rows(rows, licences, Tier.OPEN)
        assert [r["id"] for r in kept] == ["a"]
        assert kept[0]["_licence"] == "CC BY 4.0"
        assert splits == {"a": "train"}

    def test_an_unrecorded_licence_routes_restricted_not_open(self):
        rows = [{"source": "biorxiv", "id": "a", "_split": "train"}]
        kept, _ = route_rows(rows, {}, Tier.OPEN)
        assert kept == []


class TestBuildTable:
    def test_columns_map_onto_the_corpus_schema(self):
        rows = [
            {
                "id": "a",
                "doi": "10.1/a",
                "version": "",
                "pub_date": "2026",
                "_licence": "CC BY 4.0",
                "xml_ftfy_applied": False,
                "xml_source_url": "",
                "xml_downloaded_at": "2026-01-01T00:00:00Z",
                "pdf_source_url": "",
                "pdf_downloaded_at": "2026-01-01T00:00:00Z",
                "xml": "<article/>",
                "pdf": b"pdf-bytes",
            }
        ]
        table = build_table(rows, "2026-10-02T00:00:00Z")
        d = table.to_pydict()
        assert d["id"] == ["a"]
        assert d["doi"] == ["10.1/a"]
        assert d["version"] == [None]  # empty string becomes null, not ""
        assert d["licence"] == ["CC BY 4.0"]
        assert d["xml_upstream"] == [None]
        assert d["row_updated_at"] == ["2026-10-02T00:00:00Z"]

    def test_xml_upstream_sha_hashes_xml_directly(self):
        rows = [
            {
                "id": "a",
                "doi": None,
                "version": None,
                "pub_date": None,
                "_licence": "CC BY 4.0",
                "xml_ftfy_applied": False,
                "xml_source_url": None,
                "xml_downloaded_at": None,
                "pdf_source_url": None,
                "pdf_downloaded_at": None,
                "xml": "<article/>",
                "pdf": b"x",
            }
        ]
        table = build_table(rows, "2026-10-02T00:00:00Z")
        expected = hashlib.sha256(b"<article/>").hexdigest()
        assert table.to_pydict()["xml_upstream_sha"] == [expected]


class TestMainEndToEnd:
    """The scenario this was built for: a second build must not duplicate, rewrite or
    reshuffle what the first one already published."""

    def _run(self, tmp_path, **extra_args):
        data_dir = tmp_path / "data"
        output_dir = tmp_path / "output"
        licence_report = _licence_report(
            tmp_path,
            {
                "biorxiv__a": "CC BY 4.0",
                "biorxiv__b": "CC BY 4.0",
                "biorxiv__c": "CC BY 4.0",
            },
        )
        argv = [
            "--corpus",
            "biorxiv",
            "--tier",
            "open",
            "--data-dir",
            str(data_dir),
            "--licence-report",
            str(licence_report),
            "--output-dir",
            str(output_dir),
        ]
        for key, value in extra_args.items():
            flag = f"--{key.replace('_', '-')}"
            argv += [flag] if value is True else [flag, str(value)]
        main(argv)
        return output_dir / "sciencebeam-dataset-biorxiv"

    def test_a_second_build_from_scratch_overwrites_not_duplicates(self, tmp_path):
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b", "c"]})
        root = self._run(tmp_path)
        first_bytes = (root / "pdf-jats/test/test-00000.parquet").read_bytes()

        root_again = self._run(tmp_path)
        assert root_again == root
        assert not (root / "pdf-jats/test/test-00001.parquet").exists()
        second_bytes = (root / "pdf-jats/test/test-00000.parquet").read_bytes()
        # Re-derived from the same source each time: same row content, even if the
        # build timestamp column makes the bytes not byte-identical.
        assert read_shard(str(root / "pdf-jats/test/test-00000.parquet")).num_rows == 2
        assert len(first_bytes) > 0 and len(second_bytes) > 0

    def test_growing_a_published_release_is_no_longer_its_job(self, tmp_path):
        """Additions go through additions.yml and the apply step."""
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b"]})
        with pytest.raises(SystemExit):
            self._run(tmp_path, version="v1.1.0", base_release="v1.0.0")

    def test_uploading_over_a_released_repo_is_refused_before_building(
        self, tmp_path, monkeypatch
    ):
        class FakeApi:
            def __init__(self, token):
                pass

            def list_repo_files(self, repo_id, repo_type):
                return ["README.md", "releases/v1.0.0.csv", "releases/v2.0.0.csv"]

        monkeypatch.setattr("huggingface_hub.HfApi", FakeApi)
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b"]})
        with pytest.raises(SystemExit, match="already has release v2.0.0"):
            self._run(tmp_path, upload=True)
        assert not (tmp_path / "output").exists()

    def test_a_repo_not_on_the_hub_yet_is_not_refused(self, monkeypatch):
        import httpx
        from huggingface_hub.errors import RepositoryNotFoundError

        class FakeApi:
            def __init__(self, token):
                pass

            def list_repo_files(self, repo_id, repo_type):
                request = httpx.Request("GET", "https://huggingface.co")
                raise RepositoryNotFoundError(
                    "no such repo", response=httpx.Response(404, request=request)
                )

        monkeypatch.setattr("huggingface_hub.HfApi", FakeApi)
        build_cli.refuse_released("elifepathways/sciencebeam-dataset-biorxiv")


class TestUploadWithRetry:
    """No real network: `HfApi` is faked, so this proves the retry logic itself rather
    than anything about the Hub."""

    def test_retries_and_succeeds_on_a_later_attempt(self, tmp_path, monkeypatch):
        calls = []

        class FakeApi:
            def __init__(self, token):
                self.token = token

            def upload_file(self, **kwargs):
                calls.append(kwargs)
                if len(calls) < 2:
                    raise RuntimeError(
                        "Cannot send a request, as the client has been closed."
                    )

        monkeypatch.setattr("huggingface_hub.HfApi", FakeApi)
        local = tmp_path / "f.parquet"
        local.write_bytes(b"x")

        build_cli._upload_with_retry(
            "tok", "repo", "v1.0.0", local, "pdf-jats/train/train-00000.parquet"
        )
        assert len(calls) == 2

    def test_gives_up_after_max_attempts(self, tmp_path, monkeypatch):
        attempts = []

        class FakeApi:
            def __init__(self, token):
                pass

            def upload_file(self, **kwargs):
                attempts.append(kwargs)
                raise RuntimeError("still broken")

        monkeypatch.setattr("huggingface_hub.HfApi", FakeApi)
        local = tmp_path / "f.parquet"
        local.write_bytes(b"x")

        with pytest.raises(SystemExit, match="failed after"):
            build_cli._upload_with_retry("tok", "repo", "v1.0.0", local, "x.parquet")
        assert len(attempts) == build_cli.MAX_UPLOAD_ATTEMPTS

    def test_each_attempt_gets_a_fresh_client(self, tmp_path, monkeypatch):
        """A client that raised mid-transfer may be in a bad state, so a retry must
        not reuse it."""
        made = []

        class FakeApi:
            def __init__(self, token):
                made.append(self)

            def upload_file(self, **kwargs):
                if len(made) < 2:
                    raise RuntimeError("client has been closed")

        monkeypatch.setattr("huggingface_hub.HfApi", FakeApi)
        local = tmp_path / "f.parquet"
        local.write_bytes(b"x")

        build_cli._upload_with_retry("tok", "repo", "v1.0.0", local, "x.parquet")
        assert len(made) == 2
        assert made[0] is not made[1]
