#!/usr/bin/env python3
"""Exercise publication and retention through a simulated gh process."""

import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import release


REPO = "example/nishidb"
TAG = "v0.8.1"


def record(number, **changes):
    value = {
        "id": number, "tag_name": f"v1.0.{number}", "draft": False,
        "prerelease": False, "published_at": f"2026-08-{number:02}T00:00:00Z",
        "body": "handwritten notes", "assets": [],
    }
    value.update(changes)
    return value


class GitHub:
    def __init__(self, records):
        self.records = copy.deepcopy(records)
        self.calls = []
        self.fail = lambda args: False
        self.incomplete = False
        self.list_count = 0
        self.fail_list = None

    def run(self, args, **kwargs):
        self.calls.append(args)
        if self.fail(args):
            raise subprocess.CalledProcessError(1, args)
        self.assert_safe(args)
        if args[1] == "api":
            assert args[-2:] == ["--paginate", "--slurp"]
            if "/assets?" in args[2]:
                number = int(args[2].split("/")[-2])
                rows = next(r["assets"] for r in self.records if r["id"] == number)
            else:
                self.list_count += 1
                if self.list_count == self.fail_list:
                    raise subprocess.CalledProcessError(1, args)
                rows = self.records
            # Multiple pages and an empty page must all be consumed.
            return subprocess.CompletedProcess(args, 0, json.dumps([rows[:2], [], rows[2:]]))
        assert args[1] == "release" and args[-2:] == ["--repo", REPO]
        action, tag = args[2:4]
        current = next((r for r in self.records if r["tag_name"] == tag), None)
        if action == "create":
            assert current is None and "--draft" in args and "--verify-tag" in args
            assert "--generate-notes" in args
            self.records.append(record(20, tag_name=tag, draft=True, published_at=None))
        elif action == "upload":
            assert "--clobber" in args
            current["assets"] = [
                {"name": Path(p).name, "size": Path(p).stat().st_size, "state": "uploaded"}
                for p in args[4:args.index("--clobber")]
            ]
            if self.incomplete:
                current["assets"][-1]["state"] = "starter"
        elif action == "edit":
            assert current["draft"] and "--draft=false" in args
            current.update(draft=False, published_at="2026-09-15T00:00:00Z")
        elif action == "delete":
            assert "--yes" in args
            self.records.remove(current)
        else:
            raise AssertionError(f"Unexpected gh call: {args}")
        return subprocess.CompletedProcess(args, 0, "")

    @staticmethod
    def assert_safe(args):
        assert args[0] == "gh"
        assert "--cleanup-tag" not in args
        assert not any("/git/refs" in arg for arg in args)

    def actions(self):
        return [args[2] for args in self.calls if args[1] == "release"]


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        (self.directory / f"nishidb-{TAG}-linux-amd64").write_bytes(b"server-amd64")
        (self.directory / f"nishidb-{TAG}-linux-arm64").write_bytes(b"server-arm64")
        (self.directory / f"nishidb_{TAG[1:]}_amd64.deb").write_bytes(b"deb-amd64")
        (self.directory / f"nishidb_{TAG[1:]}_arm64.deb").write_bytes(b"deb-arm64")
        (self.directory / "nishidb-jdbc.jar").write_bytes(b"driver")

    def publish(self, github):
        with patch.object(release.subprocess, "run", side_effect=github.run):
            release.publish(REPO, TAG, self.directory)

    def test_exact_tags(self):
        for tag in ["v0.0.0", "v1.2.3", "v10.20.30"]:
            release.validate_tag(tag)
        for tag in ["1.2.3", "v1.2", "v1.2.3.4", "v01.2.3", "v1.02.3", "v1.2.03",
                    "v1.2.3-rc1", "v1.2.3+meta", "v1.2.3\n", "v١.2.3", "v1.*.3"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.validate_tag(tag)

    def test_no_deletions_with_up_to_five_publications(self):
        for count in range(6):
            github = GitHub([record(n) for n in range(1, count + 1)])
            with patch.object(release.subprocess, "run", side_effect=github.run):
                release.retain_five(REPO, release.releases(REPO))
            self.assertNotIn("delete", github.actions())

    def test_new_release_uploads_before_publishing_then_prunes(self):
        github = GitHub([record(n) for n in range(1, 8)])
        self.publish(github)
        self.assertEqual(["create", "upload", "edit", "delete", "delete", "delete"], github.actions())
        self.assertEqual({"v1.0.4", "v1.0.5", "v1.0.6", "v1.0.7", TAG},
                         {r["tag_name"] for r in github.records})

    def test_retention_uses_date_includes_prereleases_and_ignores_drafts(self):
        rows = [record(n) for n in range(1, 8)]
        rows[0]["tag_name"] = "v99.0.0"
        rows[-1].update(tag_name="preview", prerelease=True)
        rows.append(record(8, draft=True, published_at=None))
        github = GitHub(rows)
        self.publish(github)
        tags = {r["tag_name"] for r in github.records}
        self.assertNotIn("v99.0.0", tags)
        self.assertIn("preview", tags)
        self.assertIn("v1.0.8", tags)
        self.assertEqual(5, sum(not r["draft"] for r in github.records))

    def test_date_ties_use_id(self):
        github = GitHub([record(n, published_at="2026-08-01T00:00:00Z") for n in range(1, 8)])
        with patch.object(release.subprocess, "run", side_effect=github.run):
            release.retain_five(REPO, release.releases(REPO))
        self.assertEqual([3, 4, 5, 6, 7], [r["id"] for r in github.records])

    def test_retries_keep_notes_and_publication_date(self):
        for draft in [False, True]:
            with self.subTest(draft=draft):
                github = GitHub([record(1, tag_name=TAG, draft=draft,
                                        published_at=None if draft else "2026-08-01T00:00:00Z")])
                self.publish(github)
                self.assertNotIn("create", github.actions())
                self.assertEqual("handwritten notes", github.records[0]["body"])
                published = github.records[0]["published_at"]
                if not draft:
                    self.assertNotIn("edit", github.actions())
                    self.assertEqual("2026-08-01T00:00:00Z", published)
                self.publish(github)
                self.assertEqual(published, github.records[0]["published_at"])

    def test_missing_or_empty_local_asset_never_calls_github(self):
        # The five assets are the two platform binaries, the two .deb
        # packages and the driver jar: missing ANY one of them — arm64 and
        # each .deb included, not just the jar — must refuse before ever
        # touching the network.
        asset_names = [
            f"nishidb-{TAG}-linux-amd64",
            f"nishidb-{TAG}-linux-arm64",
            f"nishidb_{TAG[1:]}_amd64.deb",
            f"nishidb_{TAG[1:]}_arm64.deb",
            "nishidb-jdbc.jar",
        ]
        for name in asset_names:
            asset = self.directory / name
            original = asset.read_bytes()
            for missing in [False, True]:
                if missing:
                    asset.unlink()
                else:
                    asset.write_bytes(b"")
                github = GitHub([])
                with self.subTest(asset=name, missing=missing), self.assertRaises((ValueError, FileNotFoundError)):
                    self.publish(github)
                self.assertEqual([], github.calls)
            asset.write_bytes(original)

    def test_remote_failures_never_reach_retention(self):
        for action in ["create", "upload", "edit"]:
            github = GitHub([record(n) for n in range(1, 8)])
            github.fail = lambda args: args[1:3] == ["release", action]
            with self.subTest(action=action), self.assertRaises(subprocess.CalledProcessError):
                self.publish(github)
            self.assertNotIn("delete", github.actions())

    def test_each_release_listing_failure_prevents_deletion(self):
        for listing in [1, 2, 3]:
            github = GitHub([record(n) for n in range(1, 8)])
            github.fail_list = listing
            with self.subTest(listing=listing), self.assertRaises(subprocess.CalledProcessError):
                self.publish(github)
            self.assertNotIn("delete", github.actions())

    def test_unconfirmed_asset_prevents_publication_and_deletion(self):
        github = GitHub([record(n) for n in range(1, 8)])
        github.incomplete = True
        with self.assertRaises(RuntimeError):
            self.publish(github)
        self.assertNotIn("edit", github.actions())
        self.assertNotIn("delete", github.actions())
        self.assertTrue(next(r for r in github.records if r["tag_name"] == TAG)["draft"])
        github.incomplete = False
        self.publish(github)
        self.assertEqual(5, len(github.records))
        self.assertEqual(1, github.actions().count("create"))

    def test_wrong_asset_size_prevents_publication(self):
        github = GitHub([record(n) for n in range(1, 8)])
        original = github.run

        def wrong_size(args, **kwargs):
            result = original(args, **kwargs)
            if args[1] == "api" and "/assets?" in args[2]:
                pages = json.loads(result.stdout)
                pages[0][-1]["size"] += 1
                result.stdout = json.dumps(pages)
            return result

        with patch.object(release.subprocess, "run", side_effect=wrong_size):
            with self.assertRaises(RuntimeError):
                release.publish(REPO, TAG, self.directory)
        self.assertNotIn("edit", github.actions())
        self.assertNotIn("delete", github.actions())

    def test_unconfirmed_publication_prevents_retention(self):
        github = GitHub([record(n) for n in range(1, 8)])
        original = github.run

        def still_draft(args, **kwargs):
            result = original(args, **kwargs)
            if args[1:3] == ["release", "edit"]:
                next(r for r in github.records if r["tag_name"] == TAG)["draft"] = True
            return result

        with patch.object(release.subprocess, "run", side_effect=still_draft):
            with self.assertRaises(RuntimeError):
                release.publish(REPO, TAG, self.directory)
        self.assertNotIn("delete", github.actions())

    def test_asset_listing_failure_prevents_deletion(self):
        github = GitHub([record(n) for n in range(1, 8)])
        github.fail = lambda args: args[1] == "api" and "/assets?" in args[2]
        with self.assertRaises(subprocess.CalledProcessError):
            self.publish(github)
        self.assertNotIn("edit", github.actions())
        self.assertNotIn("delete", github.actions())

    def test_cleanup_failure_is_retryable_without_republishing(self):
        github = GitHub([record(n) for n in range(1, 8)])
        github.fail = lambda args: args[1:4] == ["release", "delete", "v1.0.2"]
        with self.assertRaises(subprocess.CalledProcessError):
            self.publish(github)
        self.assertEqual(7, len(github.records))  # one deletion succeeded
        published = next(r for r in github.records if r["tag_name"] == TAG)["published_at"]
        github.fail = lambda args: False
        self.publish(github)
        self.assertEqual(5, len(github.records))
        self.assertEqual(1, github.actions().count("edit"))
        self.assertEqual(published, next(r for r in github.records if r["tag_name"] == TAG)["published_at"])

    def test_malformed_or_incomplete_snapshot_fails_before_mutation(self):
        for value in ["not json", '{"message":"forbidden"}', '[[{"id":1}]]']:
            with patch.object(release.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, value)) as run:
                with self.assertRaises(ValueError):
                    release.publish(REPO, TAG, self.directory)
                self.assertEqual(1, run.call_count)


if __name__ == "__main__":
    unittest.main()
