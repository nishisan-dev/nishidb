#!/usr/bin/env python3
"""Publish both release assets, then retain five publications without deleting tags.

Requires gh, GH_TOKEN and GH_REPO. All remote operations use argument arrays;
release titles, tags and API responses are never evaluated as shell commands.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def validate_tag(tag):
    if not TAG.fullmatch(tag):
        raise ValueError(f"Invalid release tag: {tag!r}; expected vx.y.z")


def gh(*args):
    return subprocess.run(
        ["gh", *args], check=True, text=True, stdout=subprocess.PIPE
    ).stdout


def releases(repo):
    # gh fails if ANY page fails; decode and validate the complete snapshot
    # before returning it to the caller that can delete releases.
    pages = json.loads(gh("api", f"repos/{repo}/releases?per_page=100", "--paginate", "--slurp"))
    if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
        raise ValueError("Invalid paginated releases response")
    result = [release for page in pages for release in page]
    for release in result:
        if (
            not isinstance(release.get("id"), int)
            or not isinstance(release.get("tag_name"), str)
            or not release["tag_name"]
            or not isinstance(release.get("draft"), bool)
            or (not release["draft"] and not release.get("published_at"))
        ):
            raise ValueError("Incomplete release metadata; refusing retention")
    return result


def retain_five(repo, snapshot):
    published = sorted(
        (release for release in snapshot if not release["draft"]),
        key=lambda release: (release["published_at"], release["id"]),
        reverse=True,
    )
    for release in published[5:]:
        print(f"Deleting release {release['tag_name']} (preserving its tag)", flush=True)
        gh("release", "delete", release["tag_name"], "--yes", "--repo", repo)


def publish(repo, tag, directory):
    validate_tag(tag)
    assets = [directory / f"nishidb-{tag}-linux-amd64", directory / "nishidb-jdbc.jar"]
    sizes = {asset.name: asset.stat().st_size for asset in assets}
    if not all(sizes.values()):
        raise ValueError("Release artifacts must not be empty")

    existing = next((r for r in releases(repo) if r["tag_name"] == tag), None)
    if existing is None:
        gh("release", "create", tag, "--draft", "--verify-tag", "--generate-notes",
           "--title", tag, "--repo", repo)
    # A published release is never converted back into a draft on retry.
    gh("release", "upload", tag, *(str(asset) for asset in assets), "--clobber", "--repo", repo)
    current = next((r for r in releases(repo) if r["tag_name"] == tag), None)
    if current is None:
        raise RuntimeError("Release missing after upload")
    # Assets are independently paginated: the release response can truncate them.
    pages = json.loads(gh("api", f"repos/{repo}/releases/{current['id']}/assets?per_page=100",
                         "--paginate", "--slurp"))
    uploaded = {a["name"]: a for page in pages for a in page}
    for name, size in sizes.items():
        asset = uploaded.get(name, {})
        if asset.get("state") != "uploaded" or asset.get("size") != size:
            raise RuntimeError(f"Release asset not fully uploaded: {name}")
    if current["draft"]:
        gh("release", "edit", tag, "--draft=false", "--verify-tag", "--repo", repo)

    snapshot = releases(repo)
    if not any(r["tag_name"] == tag and not r["draft"] for r in snapshot):
        raise RuntimeError("Publication not confirmed; refusing retention")
    retain_five(repo, snapshot)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    validation = commands.add_parser("validate")
    validation.add_argument("tag")
    publication = commands.add_parser("publish")
    publication.add_argument("tag")
    publication.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "validate":
            validate_tag(args.tag)
        else:
            repo = os.environ.get("GH_REPO", "")
            if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
                raise ValueError("GH_REPO must name the owner/repository")
            publish(repo, args.tag, args.directory)
    except (ValueError, RuntimeError, OSError, subprocess.CalledProcessError) as error:
        print(f"Release failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
