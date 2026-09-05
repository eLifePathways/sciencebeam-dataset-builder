"""Corpus configuration — the data that travels with a published corpus.

A config is read from the previous version beside the corpus it produced, and the
new version's config is written back the same way, so the corpus repo is the record
of how every version was made. Nothing here is corpus-specific: the stratum column,
the split names and the counts all arrive as data.
"""

import dataclasses
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    """A config that cannot be used as written."""


@dataclasses.dataclass(frozen=True)
class SourceConfig:
    """Where the archive is read from, and which archive it was."""

    metadata_file: str
    shard_manifest_file: str
    # Absent when cutting from a local shard directory given on the command line.
    repo_id: str | None = None
    # Recorded by a run rather than hand-written: which archive revision it read, so a
    # later version can tell it is extending a version cut from the same archive.
    revision: str | None = None


@dataclasses.dataclass(frozen=True)
class AllocationConfig:
    """Per-split counts per stratum: a default, with per-stratum overrides."""

    default: Mapping[str, int]
    overrides: Mapping[str, Mapping[str, int]] = dataclasses.field(default_factory=dict)

    def counts_for(self, stratum: str) -> dict[str, int]:
        """Resolved per-split counts for one stratum.

        An override is merged per split key rather than replacing the whole mapping, so
        naming one split in an override leaves the others at the default.
        """
        return {**self.default, **self.overrides.get(stratum, {})}


@dataclasses.dataclass(frozen=True)
class CorpusConfig:
    """A complete description of one corpus version."""

    # What this corpus is called. It names the published manifest and config, so a repo
    # can hold more than one corpus without their versions colliding, and a reader can
    # tell which corpus a version file belongs to.
    name: str
    version: int
    # Both the set of splits and the order they are served in when a stratum cannot
    # fill them all. First served wins a scarce stratum, so this order is a real
    # choice and belongs in the config rather than in the allocation loop.
    splits: tuple[str, ...]
    allocation: AllocationConfig
    source: SourceConfig
    columns: tuple[str, ...]
    target_repo_id: str | None = None
    id_column: str = "id"
    stratum_column: str = "stratum"
    rank_column: str = "rank"
    # Documents that must not be selected, by id — recorded so that a document which
    # cannot be rendered is not rediscovered on every run.
    exclude: tuple[str, ...] = ()

    def counts_for(self, stratum: str) -> dict[str, int]:
        return self.allocation.counts_for(stratum)


@dataclasses.dataclass(frozen=True)
class CountRegression:
    """A per-stratum-split count that a new config asks for less of than the old one."""

    split: str
    previous: int
    current: int
    # None where the fall is in the default, so it applies to every stratum without
    # an override for this split.
    stratum: str | None = None

    def describe(self) -> str:
        where = f"stratum {self.stratum!r}" if self.stratum else "the default"
        return f"{where}, split {self.split!r}: {self.previous} -> {self.current}"


_TOP_LEVEL_KEYS = {
    "name",
    "version",
    "splits",
    "allocation",
    "source",
    "columns",
    "target",
    "id_column",
    "stratum_column",
    "rank_column",
    "exclude",
}
_SOURCE_KEYS = {"repo_id", "metadata_file", "shard_manifest_file", "revision"}
_ALLOCATION_KEYS = {"default", "overrides"}


def _require_mapping(value: Any, what: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{what} must be a mapping, got {type(value).__name__}")
    return value


def _reject_unknown_keys(
    value: Mapping[str, Any], allowed: set[str], what: str
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigError(
            f"{what} has unknown key(s): {', '.join(unknown)}. "
            f"Allowed: {', '.join(sorted(allowed))}"
        )


def _counts(value: Any, what: str, splits: Sequence[str]) -> dict[str, int]:
    mapping = _require_mapping(value, what)
    unknown = sorted(set(mapping) - set(splits))
    if unknown:
        raise ConfigError(f"{what} names split(s) not in splits: {', '.join(unknown)}")
    counts: dict[str, int] = {}
    for split, count in mapping.items():
        if not isinstance(count, int) or isinstance(count, bool):
            raise ConfigError(f"{what}[{split!r}] must be an integer, got {count!r}")
        if count < 0:
            raise ConfigError(f"{what}[{split!r}] must not be negative, got {count}")
        counts[split] = count
    return counts


def _parse_splits(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError("splits must be a non-empty list of split names")
    splits = tuple(str(split) for split in value)
    duplicates = sorted({s for s in splits if splits.count(s) > 1})
    if duplicates:
        raise ConfigError(f"splits contains duplicate(s): {', '.join(duplicates)}")
    return splits


def config_from_dict(data: Mapping[str, Any]) -> CorpusConfig:
    """Build a config from parsed YAML, rejecting anything unusable.

    Unknown keys are an error rather than being ignored: this file is the record of how
    a published corpus was made, and a silently ignored typo there is a wrong record.
    """
    _reject_unknown_keys(data, _TOP_LEVEL_KEYS, "config")

    name = str(data.get("name", "")).strip()
    if not name:
        raise ConfigError(
            "name is required: it names the published manifest and config, so a repo "
            "can hold more than one corpus"
        )
    if "/" in name or name.startswith("."):
        raise ConfigError(f"name {name!r} must be usable as a filename component")

    version = data.get("version", 1)
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ConfigError(f"version must be an integer >= 1, got {version!r}")

    splits = _parse_splits(data.get("splits"))

    allocation_data = _require_mapping(data.get("allocation", {}), "allocation")
    _reject_unknown_keys(allocation_data, _ALLOCATION_KEYS, "allocation")
    default = _counts(allocation_data.get("default", {}), "allocation.default", splits)
    missing = sorted(set(splits) - set(default))
    if missing:
        raise ConfigError(
            f"allocation.default must give a count for every split; missing: "
            f"{', '.join(missing)}"
        )
    overrides_data = _require_mapping(
        allocation_data.get("overrides", {}), "allocation.overrides"
    )
    overrides = {
        stratum: _counts(counts, f"allocation.overrides[{stratum!r}]", splits)
        for stratum, counts in overrides_data.items()
    }

    source_data = _require_mapping(data.get("source", {}), "source")
    _reject_unknown_keys(source_data, _SOURCE_KEYS, "source")
    metadata_file = source_data.get("metadata_file")
    shard_manifest_file = source_data.get("shard_manifest_file")
    if not metadata_file or not shard_manifest_file:
        raise ConfigError(
            "source.metadata_file and source.shard_manifest_file are both required"
        )

    columns_data = data.get("columns")
    if not isinstance(columns_data, list) or not columns_data:
        raise ConfigError("columns must be a non-empty list of column names")
    columns = tuple(str(column) for column in columns_data)

    id_column = str(data.get("id_column", "id"))
    if id_column not in columns:
        raise ConfigError(
            f"columns must include the id column {id_column!r}, so the published "
            f"corpus can be read by id"
        )

    exclude_data = data.get("exclude", []) or []
    if not isinstance(exclude_data, list):
        raise ConfigError("exclude must be a list of ids")
    exclude = tuple(str(paper_id) for paper_id in exclude_data)
    duplicates = sorted({i for i in exclude if exclude.count(i) > 1})
    if duplicates:
        raise ConfigError(f"exclude contains duplicate id(s): {', '.join(duplicates)}")

    target_data = _require_mapping(data.get("target", {}), "target")
    _reject_unknown_keys(target_data, {"repo_id"}, "target")

    return CorpusConfig(
        name=name,
        version=version,
        splits=splits,
        allocation=AllocationConfig(default=default, overrides=overrides),
        source=SourceConfig(
            metadata_file=str(metadata_file),
            shard_manifest_file=str(shard_manifest_file),
            repo_id=_optional_str(source_data.get("repo_id")),
            revision=_optional_str(source_data.get("revision")),
        ),
        columns=columns,
        target_repo_id=_optional_str(target_data.get("repo_id")),
        id_column=id_column,
        stratum_column=str(data.get("stratum_column", "stratum")),
        rank_column=str(data.get("rank_column", "rank")),
        exclude=exclude,
    )


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def config_to_dict(config: CorpusConfig) -> dict[str, Any]:
    """Return a plain-data form of the config, suitable for YAML."""
    source: dict[str, Any] = {
        "metadata_file": config.source.metadata_file,
        "shard_manifest_file": config.source.shard_manifest_file,
    }
    if config.source.repo_id is not None:
        source["repo_id"] = config.source.repo_id
    if config.source.revision is not None:
        source["revision"] = config.source.revision

    data: dict[str, Any] = {
        "name": config.name,
        "version": config.version,
        "splits": list(config.splits),
        "id_column": config.id_column,
        "stratum_column": config.stratum_column,
        "rank_column": config.rank_column,
        "columns": list(config.columns),
        "source": source,
        "allocation": {
            "default": dict(config.allocation.default),
            "overrides": {
                stratum: dict(counts)
                for stratum, counts in config.allocation.overrides.items()
            },
        },
    }
    if config.target_repo_id is not None:
        data["target"] = {"repo_id": config.target_repo_id}
    if config.exclude:
        data["exclude"] = list(config.exclude)
    return data


def load_config(path: Path) -> CorpusConfig:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        raise ConfigError(f"{path} is empty")
    return config_from_dict(_require_mapping(data, "config"))


def dump_config(config: CorpusConfig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(config_to_dict(config), sort_keys=False), encoding="utf-8"
    )


def find_lowered_counts(
    previous: CorpusConfig, current: CorpusConfig
) -> list[CountRegression]:
    """Return every per-stratum-split count the current config asks less of.

    The cheap half of the monotonicity check: it needs neither the archive nor the
    published manifest, so a config edit that would shrink a split fails before
    anything is downloaded. It is deliberately conservative — a stratum previously
    capped below its configured count is still rejected if that count falls, because
    the configured figure is the declared intent.
    """
    regressions: list[CountRegression] = []
    for split in previous.splits:
        previous_default = previous.allocation.default[split]
        current_default = current.allocation.default.get(split, 0)
        if current_default < previous_default:
            regressions.append(
                CountRegression(
                    split=split, previous=previous_default, current=current_default
                )
            )

    strata = set(previous.allocation.overrides) | set(current.allocation.overrides)
    for stratum in sorted(strata):
        previous_counts = previous.counts_for(stratum)
        current_counts = current.counts_for(stratum)
        for split in previous.splits:
            was = previous_counts[split]
            now = current_counts.get(split, 0)
            if now < was:
                regressions.append(
                    CountRegression(
                        stratum=stratum, split=split, previous=was, current=now
                    )
                )
    return regressions
