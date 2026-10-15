"""Tests for corpus.validate — checking removals before they are proposed."""

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_files import Removal, read_removals
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import Change, ChangeType, write_ledger
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.validate import check_removals
from tests.corpus._fake_hub import FakeHub, FakePullRequest, removals_change
from tests.corpus._repos import publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
ORE = CORPORA["ore"]

REASON = "editorial, not a research article"


def _removal_entry(id_value: str, local_change_id: int) -> str:
    return (
        f"  - id: {id_value}\n"
        f"    local_change_id: {local_change_id}\n"
        f"    change_reason: earlier\n"
        f"    changed_by: someone\n"
    )


def _correction_file(id_value: str, local_change_id: int) -> str:
    return (
        f"id: {id_value}\n"
        "corrections:\n"
        f"  - local_change_id: {local_change_id}\n"
        "    change_type: content-corrected\n"
        "    change_reason: earlier\n"
        "    changed_by: someone\n"
    )


def _record(
    repo: Repo, id_value: str, local_change_id: int, change_type: ChangeType
) -> None:
    write_ledger(
        root_of(repo) / layout.LEDGER_PATH,
        [
            Change(
                change_id=1,
                local_change_id=local_change_id,
                corpus=repo.corpus.name,
                id=id_value,
                split="train",
                file_type_pair="pdf-jats",
                parquet_file_name="train-00000.parquet",
                base_release="v1.0.0",
                change_type=change_type,
                change_reason="earlier",
                change_timestamp="2026-10-01T09:14:22Z",
                changed_by="someone",
            )
        ],
    )


@pytest.fixture
def biorxiv(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "b"], "test": ["c"]})


class TestCheckRemovals:
    def test_a_present_document_is_proposed_with_its_reason_and_submitter(
        self, biorxiv
    ):
        checks, proposals = check_removals([("a", REASON)], [biorxiv], "hazal")
        assert checks[0].ok
        assert checks[0].removal == Removal("a", 1, REASON, "hazal")
        [proposal] = proposals
        assert proposal.before is None
        assert read_removals(proposal.after) == [Removal("a", 1, REASON, "hazal")]

    def test_the_existing_file_is_kept_and_appended_to(self, biorxiv):
        existing = "removals:\n" + _removal_entry("z", 1)
        write_file(biorxiv, layout.REMOVALS_PATH, existing)
        _record(biorxiv, "z", 1, ChangeType.REMOVED)
        [proposal] = check_removals([("a", REASON)], [biorxiv], "hazal")[1]
        assert proposal.before == existing
        assert proposal.after.startswith(existing)

    def test_one_document_named_two_ways_is_refused_the_second_time(self, biorxiv):
        checks, proposals = check_removals(
            [("a", REASON), ("biorxiv__a", REASON)], [biorxiv], "hazal"
        )
        assert checks[0].ok
        assert checks[1].errors == ("listed more than once",)
        assert len(proposals[0].removals) == 1

    def test_a_missing_document_says_so(self, biorxiv):
        checks, proposals = check_removals([("z", REASON)], [biorxiv], "hazal")
        assert checks[0].errors[0].startswith("missing:")
        assert checks[0].location is None
        assert proposals == []

    def test_a_reason_is_required(self, biorxiv):
        checks, _ = check_removals([("a", "  ")], [biorxiv], "hazal")
        assert checks[0].errors == ("a reason is required",)

    def test_a_removal_already_pending_on_main_is_refused(self, biorxiv):
        """Merged but not yet applied: the ledger has no row for it."""
        write_file(
            biorxiv, layout.REMOVALS_PATH, "removals:\n" + _removal_entry("a", 1)
        )
        checks, _ = check_removals([("a", REASON)], [biorxiv], "hazal")
        assert "a removal is already pending" in checks[0].errors[0]

    def test_a_document_removed_and_added_back_can_be_removed_again(self, biorxiv):
        write_file(
            biorxiv, layout.REMOVALS_PATH, "removals:\n" + _removal_entry("a", 1)
        )
        _record(biorxiv, "a", 1, ChangeType.REMOVED)
        checks, _ = check_removals([("a", REASON)], [biorxiv], "hazal")
        assert checks[0].removal is not None
        assert checks[0].removal.local_change_id == 2

    def test_a_correction_pending_on_main_is_refused(self, biorxiv):
        write_file(
            biorxiv, layout.correction_paths("a").metadata, _correction_file("a", 1)
        )
        checks, _ = check_removals([("a", REASON)], [biorxiv], "hazal")
        assert "a correction is pending" in checks[0].errors[0]

    def test_an_applied_correction_continues_the_documents_sequence(self, biorxiv):
        write_file(
            biorxiv, layout.correction_paths("a").metadata, _correction_file("a", 1)
        )
        _record(biorxiv, "a", 1, ChangeType.CONTENT_CORRECTED)
        checks, _ = check_removals([("a", REASON)], [biorxiv], "hazal")
        assert checks[0].removal is not None
        assert checks[0].removal.local_change_id == 2

    def test_an_invalid_document_leaves_the_others_proposed(self, biorxiv):
        checks, proposals = check_removals(
            [("a", REASON), ("z", REASON), ("c", REASON)], [biorxiv], "hazal"
        )
        assert [c.ok for c in checks] == [True, False, True]
        assert [r.id for r in proposals[0].removals] == ["a", "c"]

    def test_each_repo_gets_its_own_proposal(self, tmp_path, biorxiv):
        ore = publish(tmp_path, ORE, Tier.OPEN, {"train": ["o"]})
        _, proposals = check_removals(
            [("a", REASON), ("o", REASON)], [biorxiv, ore], "hazal"
        )
        assert [(p.repo.repo_id, [r.id for r in p.removals]) for p in proposals] == [
            (biorxiv.repo_id, ["a"]),
            (ore.repo_id, ["o"]),
        ]

    def test_a_broken_removals_file_blocks_only_its_own_repo(self, tmp_path, biorxiv):
        ore = publish(tmp_path, ORE, Tier.OPEN, {"train": ["o"]})
        write_file(biorxiv, layout.REMOVALS_PATH, "removals: [\n")
        checks, proposals = check_removals(
            [("a", REASON), ("o", REASON)], [biorxiv, ore], "hazal"
        )
        assert "not valid YAML" in checks[0].errors[0]
        assert checks[1].ok
        assert [p.repo.repo_id for p in proposals] == [ore.repo_id]

    def test_a_file_that_cannot_be_appended_to_blocks_its_documents(self, biorxiv):
        write_file(biorxiv, layout.REMOVALS_PATH, "removals: []\n")
        checks, proposals = check_removals([("a", REASON)], [biorxiv], "hazal")
        assert "cannot append" in checks[0].errors[0]
        assert checks[0].removal is None
        assert proposals == []

    def test_a_blank_submitter_is_refused_outright(self, biorxiv):
        with pytest.raises(ValueError, match="changed_by"):
            check_removals([("a", REASON)], [biorxiv], " ")


class TestOpenPullRequests:
    """Read through the Hub, where a pull request can already be open."""

    @pytest.fixture
    def hub(self, biorxiv, monkeypatch):
        hub = FakeHub({biorxiv.repo_id: root_of(biorxiv)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def repo(self, biorxiv, hub):
        return Repo(BIORXIV, Tier.OPEN, HubRepo(biorxiv.repo_id))

    def test_a_document_already_proposed_says_where(self, hub, repo):
        hub.pull_requests = [FakePullRequest(7, "Remove a", removals_change("a"))]
        checks, proposals = check_removals([("a", REASON)], [repo], "hazal")
        assert (
            "already proposed for removal in open pull request #7"
            in (checks[0].errors[0])
        )
        assert proposals == []

    def test_another_open_removal_pull_request_blocks_the_repo(self, hub, repo):
        """Both would edit the one removals.yml, so the second would conflict."""
        hub.pull_requests = [FakePullRequest(7, "Remove z", removals_change("z"))]
        checks, _ = check_removals([("a", REASON)], [repo], "hazal")
        assert "has to be merged or closed first" in checks[0].errors[0]

    def test_an_open_correction_of_the_document_blocks_its_removal(self, hub, repo):
        path = layout.correction_paths("a").metadata
        hub.pull_requests = [FakePullRequest(8, "Correct a", {path: b"id: a\n"})]
        checks, _ = check_removals([("a", REASON), ("b", REASON)], [repo], "hazal")
        assert "open pull request #8" in checks[0].errors[0]
        assert "changes its correction" in checks[0].errors[0]
        assert checks[1].ok

    def test_an_unrelated_pull_request_changes_nothing(self, hub, repo):
        hub.pull_requests = [FakePullRequest(9, "Card", {"README.md": b"card"})]
        checks, _ = check_removals([("a", REASON)], [repo], "hazal")
        assert checks[0].ok

    def test_a_pull_request_that_cannot_be_read_fails_closed(self, hub, repo):
        hub.pull_requests = [
            FakePullRequest(9, "?", {layout.REMOVALS_PATH: b"removals: [\n"})
        ]
        checks, _ = check_removals([("a", REASON)], [repo], "hazal")
        assert "cannot be read" in checks[0].errors[0]
