"""Resuming a cut or a render that was interrupted.

Reading a shard costs real bytes and rendering one costs real minutes, so what matters is
that a second run does not redo finished shards, and that it never treats an unfinished
one as done.
"""

import stat
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.archive_cut.cut_cli import main as cut_main
from sciencebeam_dataset_builder.archive_cut.layout import (
    ADDED_DIRECTORY,
    RENDERED_DIRECTORY,
    completed_path,
)
from sciencebeam_dataset_builder.archive_cut.progress import (
    read_completed,
    record_completed,
    write_atomically,
)
from sciencebeam_dataset_builder.archive_cut.render_cli import main as render_main

from tests.archive_cut._helpers import (
    added_table,
    archive_config,
    rendered_table,
    stage_files,
    write_archive,
    write_config,
)

FAKE_CONVERTER = Path(__file__).parent / "_fake_converter.py"
SPLITS = ["test", "validation"]


@pytest.fixture
def converter(tmp_path):
    path = tmp_path / "fake-lowriter"
    path.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CONVERTER}" "$@"\n',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


class CountingArchive:
    """A local archive that records which shards were opened."""

    def __init__(self, directory: Path) -> None:
        from sciencebeam_dataset_builder.archive_cut.source import LocalArchiveSource

        self._inner = LocalArchiveSource(directory)
        self.opened: list[str] = []

    @property
    def revision(self):
        return self._inner.revision

    def read_text(self, name):
        return self._inner.read_text(name)

    def open_parquet(self, name):
        self.opened.append(name)
        return self._inner.open_parquet(name)


class TestProgressRecord:
    def test_a_recorded_shard_round_trips(self, tmp_path):
        path = tmp_path / "completed.jsonl"
        record_completed(path, "alpha-00000.parquet", {"test": 2, "validation": 1})
        assert read_completed(path) == {
            "alpha-00000.parquet": {"test": 2, "validation": 1}
        }

    def test_records_accumulate(self, tmp_path):
        path = tmp_path / "completed.jsonl"
        record_completed(path, "a.parquet", {"test": 1})
        record_completed(path, "b.parquet", {"test": 2})
        assert sorted(read_completed(path)) == ["a.parquet", "b.parquet"]

    def test_an_absent_record_is_empty(self, tmp_path):
        assert read_completed(tmp_path / "nope.jsonl") == {}

    def test_a_truncated_final_line_keeps_the_earlier_records(self, tmp_path):
        """A crash mid-append must not discard the shards already recorded."""
        path = tmp_path / "completed.jsonl"
        record_completed(path, "a.parquet", {"test": 1})
        with path.open("a", encoding="utf-8") as f:
            f.write('{"shard": "b.parquet", "rows": {"te')
        assert list(read_completed(path)) == ["a.parquet"]

    def test_an_interrupted_write_leaves_no_file(self, tmp_path):
        target = tmp_path / "out.parquet"

        def failing(_path: Path) -> None:
            raise RuntimeError("interrupted")

        with pytest.raises(RuntimeError):
            write_atomically(target, failing)
        assert not target.exists()
        assert not target.with_suffix(".parquet.partial").exists()

    def test_a_successful_write_leaves_no_partial(self, tmp_path):
        target = tmp_path / "out.parquet"

        def write(path: Path) -> None:
            path.write_bytes(b"data")

        write_atomically(target, write)
        assert target.read_bytes() == b"data"
        assert not target.with_suffix(".parquet.partial").exists()


class TestResumingACut:
    def _cut(self, tmp_path, archive, cfg, label="v1"):
        config_path = write_config(tmp_path / f"{label}.yml", cfg)
        version_dir = tmp_path / label
        cut_main([str(config_path), str(version_dir), "--source-dir", str(archive)])
        return version_dir

    def test_one_file_is_written_per_shard(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 12}, rows_per_shard=4)
        cfg = archive_config(splits=SPLITS, default={"test": 6, "validation": 0})
        version_dir = self._cut(tmp_path, archive, cfg)
        # Ranks 0-5 span the first two shards, so two files, not one.
        assert len(stage_files(version_dir, ADDED_DIRECTORY, "test")) == 2

    def test_every_shard_is_recorded_as_completed(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 12}, rows_per_shard=4)
        cfg = archive_config(splits=SPLITS, default={"test": 6, "validation": 0})
        version_dir = self._cut(tmp_path, archive, cfg)
        completed = read_completed(completed_path(version_dir, ADDED_DIRECTORY))
        assert sorted(completed) == [
            "alpha-00000-of-00003.parquet",
            "alpha-00001-of-00003.parquet",
        ]

    def test_a_second_run_reads_nothing_again(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 12}, rows_per_shard=4)
        cfg = archive_config(splits=SPLITS, default={"test": 6, "validation": 0})
        version_dir = self._cut(tmp_path, archive, cfg)

        from sciencebeam_dataset_builder.archive_cut import cut_cli

        counting = CountingArchive(archive)
        original = cut_cli.build_source
        cut_cli.build_source = lambda args, config: counting
        try:
            self._cut(tmp_path, archive, cfg)
        finally:
            cut_cli.build_source = original
        assert counting.opened == []
        assert len(added_table(version_dir, "test")) == 6

    def test_an_interrupted_cut_keeps_its_finished_shards(self, tmp_path):
        """The point of the whole thing: a killed run does not pay twice."""
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 12}, rows_per_shard=4)
        cfg = archive_config(splits=SPLITS, default={"test": 6, "validation": 0})
        config_path = write_config(tmp_path / "v1.yml", cfg)
        version_dir = tmp_path / "v1"

        from sciencebeam_dataset_builder.archive_cut import source as source_module

        real_read = source_module._read_shard_rows_with_retries
        calls: list[str] = []

        def fail_on_second(source, filename, selected, config, **kwargs):
            calls.append(filename)
            if len(calls) == 2:
                raise KeyboardInterrupt("interrupted part way")
            return real_read(source, filename, selected, config, **kwargs)

        source_module._read_shard_rows_with_retries = fail_on_second  # type: ignore[assignment]
        try:
            with pytest.raises(KeyboardInterrupt):
                cut_main(
                    [str(config_path), str(version_dir), "--source-dir", str(archive)]
                )
        finally:
            source_module._read_shard_rows_with_retries = real_read

        # The first shard survived, the second did not, and nothing claims otherwise.
        completed = read_completed(completed_path(version_dir, ADDED_DIRECTORY))
        assert list(completed) == ["alpha-00000-of-00003.parquet"]
        assert len(stage_files(version_dir, ADDED_DIRECTORY, "test")) == 1

        # Resuming reads only what is left, and the result is whole.
        cut_main([str(config_path), str(version_dir), "--source-dir", str(archive)])
        assert len(added_table(version_dir, "test")) == 6
        assert sorted(read_completed(completed_path(version_dir, ADDED_DIRECTORY))) == [
            "alpha-00000-of-00003.parquet",
            "alpha-00001-of-00003.parquet",
        ]


class TestResumingARender:
    def _added(self, version_dir, split, ids, shard):
        import pyarrow as pa

        directory = version_dir / ADDED_DIRECTORY / split
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "id": ids,
                    "stratum": ["alpha"] * len(ids),
                    "doc": [f"source:{i}".encode() for i in ids],
                    "doc_ext": ["docx"] * len(ids),
                    "xml": [f"<a>{i}</a>" for i in ids],
                }
            ),
            directory / f"{shard}.parquet",
        )

    def test_a_second_render_redoes_nothing(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        self._added(version_dir, "test", ["alpha-000"], "alpha-00000")
        self._added(version_dir, "test", ["alpha-004"], "alpha-00001")
        render_main([str(version_dir), "--converter", converter])
        first = {
            path: path.read_bytes()
            for path in stage_files(version_dir, RENDERED_DIRECTORY, "test")
        }

        render_main([str(version_dir), "--converter", converter])
        after = {
            path: path.read_bytes()
            for path in stage_files(version_dir, RENDERED_DIRECTORY, "test")
        }
        assert after == first

    def test_a_shard_added_later_is_rendered_on_the_next_run(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        self._added(version_dir, "test", ["alpha-000"], "alpha-00000")
        render_main([str(version_dir), "--converter", converter])
        assert len(rendered_table(version_dir, "test")) == 1

        self._added(version_dir, "test", ["alpha-004"], "alpha-00001")
        render_main([str(version_dir), "--converter", converter])
        assert len(rendered_table(version_dir, "test")) == 2

    def test_failures_from_a_skipped_shard_are_not_forgotten(self, tmp_path, converter):
        """A resumed run renders fewer shards, so it must not rewrite the failure list."""
        version_dir = tmp_path / "v1"
        self._added(version_dir, "test", ["alpha-000-fails"], "alpha-00000")
        render_main([str(version_dir), "--converter", converter])

        self._added(version_dir, "test", ["alpha-004"], "alpha-00001")
        render_main([str(version_dir), "--converter", converter])

        from sciencebeam_dataset_builder.archive_cut.render_cli import read_failures
        from sciencebeam_dataset_builder.archive_cut.layout import FAILURES_FILENAME

        failures = read_failures(version_dir / FAILURES_FILENAME)
        assert [f.id for f in failures] == ["alpha-000-fails"]
