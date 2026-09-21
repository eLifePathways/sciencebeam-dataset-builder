import csv

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.dataset.license.audit_cli import (
    EXCLUSIONS_FILENAME,
    NON_COMMERCIAL_FILENAME,
    PER_DOCUMENT_FILENAME,
    PER_DOCUMENT_HEADER,
    AuditError,
    audit_config,
    audit_snapshot,
    latest_snapshot,
    main,
    write_reports,
)

CC_BY = "https://creativecommons.org/licenses/by/4.0/"
CC_BY_NC = "https://creativecommons.org/licenses/by-nc/4.0/"
CC_BY_NC_ND = "https://creativecommons.org/licenses/by-nc-nd/4.0/"


def jats(href: str | None) -> str:
    permissions = f'<license xlink:href="{href}"/>' if href else ""
    return (
        '<article xmlns:xlink="http://www.w3.org/1999/xlink"><front><article-meta>'
        f"<permissions>{permissions}</permissions>"
        "</article-meta></front></article>"
    )


def write_config(dataset_dir, config: str, rows: dict[str, list[dict[str, str]]]):
    """Write one config directory, one Parquet file per split in `rows`."""
    config_dir = dataset_dir / config
    config_dir.mkdir(parents=True, exist_ok=True)
    for split, records in rows.items():
        table = pa.Table.from_pylist(records)
        pq.write_table(table, config_dir / f"{split}-00000-of-00001.parquet")
    return config_dir


@pytest.fixture
def snapshot(tmp_path):
    dataset_dir = tmp_path / "2026-09-18"
    write_config(
        dataset_dir,
        "biorxiv-jats",
        {
            "train": [
                {"uid": "biorxiv__a", "id": "a", "doi": "10.1/a", "xml": jats(CC_BY)},
                {
                    "uid": "biorxiv__b",
                    "id": "b",
                    "doi": "10.1/b",
                    "xml": jats(CC_BY_NC_ND),
                },
            ],
            "test": [
                {
                    "uid": "biorxiv__c",
                    "id": "c",
                    "doi": "10.1/c",
                    "xml": jats(CC_BY_NC),
                },
                {"uid": "biorxiv__d", "id": "d", "doi": "", "xml": jats(None)},
            ],
        },
    )
    return dataset_dir


class TestLatestSnapshot:
    def test_picks_the_newest_dated_directory(self, tmp_path):
        for name in ("2026-01-01", "2026-09-18", "2025-12-31"):
            (tmp_path / name).mkdir()
        assert latest_snapshot(tmp_path).name == "2026-09-18"

    def test_ignores_directories_that_are_not_dated(self, tmp_path):
        (tmp_path / "2026-09-18").mkdir()
        (tmp_path / "removals").mkdir()
        (tmp_path / "output").mkdir()
        assert latest_snapshot(tmp_path).name == "2026-09-18"

    def test_errors_when_there_is_no_dated_directory(self, tmp_path):
        (tmp_path / "output").mkdir()
        with pytest.raises(AuditError, match="No dated snapshot"):
            latest_snapshot(tmp_path)

    def test_errors_when_the_data_directory_is_missing(self, tmp_path):
        with pytest.raises(AuditError, match="does not exist"):
            latest_snapshot(tmp_path / "nope")


class TestAuditConfig:
    def test_reads_every_split(self, snapshot):
        rows = audit_config(snapshot / "biorxiv-jats")
        assert len(rows) == 4
        assert {row["split"] for row in rows} == {"train", "test"}

    def test_classifies_each_document(self, snapshot):
        rows = {row["uid"]: row for row in audit_config(snapshot / "biorxiv-jats")}
        assert rows["biorxiv__a"]["training_use"] == "permissive"
        assert rows["biorxiv__a"]["excluded"] == "no"
        assert rows["biorxiv__b"]["training_use"] == "no-derivatives"
        assert rows["biorxiv__b"]["excluded"] == "yes"
        assert rows["biorxiv__c"]["training_use"] == "non-commercial"
        assert rows["biorxiv__d"]["training_use"] == "unknown"
        assert rows["biorxiv__d"]["excluded"] == "yes"

    def test_skips_a_split_that_is_not_present(self, tmp_path):
        dataset_dir = tmp_path / "2026-09-18"
        write_config(
            dataset_dir,
            "ore-jats",
            {"train": [{"uid": "ore__a", "id": "a", "doi": "", "xml": jats(CC_BY)}]},
        )
        assert len(audit_config(dataset_dir / "ore-jats")) == 1

    def test_derives_the_uid_where_the_table_has_no_uid_column(self, tmp_path):
        dataset_dir = tmp_path / "2026-09-18"
        write_config(
            dataset_dir,
            "scielo-preprints-metadata",
            {"train": [{"id": "oai_preprint_7", "xml": jats(CC_BY)}]},
        )
        rows = audit_config(dataset_dir / "scielo-preprints-metadata")
        assert rows[0]["uid"] == "scielo_preprints_dublin_core__oai_preprint_7"

    def test_rejects_a_directory_that_is_not_a_registered_config(self, tmp_path):
        (tmp_path / "not-a-config").mkdir()
        with pytest.raises(AuditError, match="not a registered config"):
            audit_config(tmp_path / "not-a-config")


class TestAuditSnapshot:
    def test_covers_every_config(self, snapshot):
        write_config(
            snapshot,
            "ore-jats",
            {"train": [{"uid": "ore__a", "id": "a", "doi": "", "xml": jats(CC_BY)}]},
        )
        rows = audit_snapshot(snapshot)
        assert {row["config"] for row in rows} == {"biorxiv-jats", "ore-jats"}
        assert len(rows) == 5

    def test_errors_on_a_missing_directory(self, tmp_path):
        with pytest.raises(AuditError, match="does not exist"):
            audit_snapshot(tmp_path / "nope")

    def test_errors_when_no_config_directory_is_present(self, tmp_path):
        (tmp_path / "2026-09-18").mkdir()
        with pytest.raises(AuditError, match="no registered config"):
            audit_snapshot(tmp_path / "2026-09-18")


class TestWriteReports:
    def test_writes_all_three_reports(self, snapshot, tmp_path):
        out = tmp_path / "reports"
        write_reports(audit_snapshot(snapshot), out)
        assert (out / PER_DOCUMENT_FILENAME).exists()
        assert (out / EXCLUSIONS_FILENAME).exists()
        assert (out / NON_COMMERCIAL_FILENAME).exists()

    def test_per_document_carries_every_row_and_column(self, snapshot, tmp_path):
        out = tmp_path / "reports"
        write_reports(audit_snapshot(snapshot), out)
        with (out / PER_DOCUMENT_FILENAME).open(newline="") as handle:
            rows = list(csv.reader(handle))
        assert tuple(rows[0]) == PER_DOCUMENT_HEADER
        assert len(rows) == 5

    def test_exclusions_hold_only_excluded_documents(self, snapshot, tmp_path):
        out = tmp_path / "reports"
        write_reports(audit_snapshot(snapshot), out)
        with (out / EXCLUSIONS_FILENAME).open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["uid"] for row in rows] == ["biorxiv__b", "biorxiv__d"]
        assert "no-derivatives" in rows[0]["reason"]

    def test_non_commercial_is_listed_separately(self, snapshot, tmp_path):
        out = tmp_path / "reports"
        write_reports(audit_snapshot(snapshot), out)
        with (out / NON_COMMERCIAL_FILENAME).open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert [row["uid"] for row in rows] == ["biorxiv__c"]

    def test_creates_the_output_directory(self, snapshot, tmp_path):
        out = tmp_path / "does" / "not" / "exist"
        write_reports(audit_snapshot(snapshot), out)
        assert (out / PER_DOCUMENT_FILENAME).exists()


class TestMain:
    def test_audits_the_newest_snapshot_by_default(self, snapshot, tmp_path, capsys):
        out = tmp_path / "reports"
        assert main(["--data-dir", str(snapshot.parent), "--output-dir", str(out)]) == 0
        assert "2026-09-18" in capsys.readouterr().out
        assert (out / PER_DOCUMENT_FILENAME).exists()

    def test_audits_the_snapshot_it_is_given(self, snapshot, tmp_path):
        out = tmp_path / "reports"
        assert main(["--dataset-dir", str(snapshot), "--output-dir", str(out)]) == 0
        assert (out / PER_DOCUMENT_FILENAME).exists()

    def test_reports_the_excluded_count(self, snapshot, tmp_path, capsys):
        out = tmp_path / "reports"
        main(["--dataset-dir", str(snapshot), "--output-dir", str(out)])
        assert "excluded from training: 2 of 4" in capsys.readouterr().out

    def test_returns_a_failure_code_without_writing(self, tmp_path, capsys):
        out = tmp_path / "reports"
        assert main(["--dataset-dir", str(tmp_path / "nope"), "--output-dir", str(out)])
        assert "does not exist" in capsys.readouterr().err
        assert not out.exists()
