#!/usr/bin/env python3
"""
Publish a SourceCraft release for the current git tag.

Called after a successful TeamCity build. The repository must already be
checked out at the release tag (HEAD points to the tag commit).

Environment variables:
  - SOURCECRAFT_TOKEN:      predefined SourceCraft token (injected automatically by the platform)
  - SOURCECRAFT_RELEASE_TAG:    Tag name for the release (e.g. 25.12.6.38-yc.1)
  - SOURCECRAFT_API_BASE (optional, default: https://public-api.o.cloud.yandex.net)
  - SOURCECRAFT_ORG_SLUG (optional, default: yc)
  - SOURCECRAFT_REPO_SLUG (optional, default: clickhouse)
"""

import logging
import os
import subprocess
import sys

from sourcecraft_api import SourceCraftClient

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Release notes helpers
# ---------------------------------------------------------------------------

def get_upstream_tag() -> str:
    """
    Return the nearest upstream tag (matching `v*`) that precedes `HEAD`.

    We use ``HEAD^`` (the parent of the current commit) so that ``git describe``
    walks backwards past `HEAD` itself when it is already a matching tag.
    Returns an empty string if not found.
    """
    try:
        return subprocess.check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match", "v*", "HEAD^"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        log.warning("Could not determine upstream tag for HEAD")
        return ""


def get_commit_subjects(from_ref: str) -> list[str]:
    """
    Return a list of commit subjects in the range *from_ref*...`HEAD`.
    """
    try:
        raw = subprocess.check_output(
            ["git", "log", f"{from_ref}..HEAD", "--format=%s"],
            text=True,
        ).strip()
    except subprocess.CalledProcessError as exc:
        log.warning("git log failed: %s", exc)
        return []
    return [line for line in raw.splitlines() if line]


def build_release_notes() -> str:
    """
    Build a markdown release notes string listing all YC commits since the
    upstream tag.

    Operates on `HEAD` — the CI runner checks out the release tag before
    calling this script.
    """
    upstream_tag = get_upstream_tag()
    if not upstream_tag:
        return ""

    log.info("Upstream tag: %s", upstream_tag)
    subjects = get_commit_subjects(upstream_tag)

    if not subjects:
        return f"No YC commits since `{upstream_tag}`."

    bullets = "\n".join(f"- {s}" for s in subjects)
    return f"## Changes since `{upstream_tag}`\n\n{bullets}\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    sc_token = os.environ["SOURCECRAFT_TOKEN"]
    release_tag = os.environ["SOURCECRAFT_RELEASE_TAG"]
    sc_api_base = os.environ.get("SOURCECRAFT_API_BASE", "https://public-api.o.cloud.yandex.net")
    sc_org = os.environ.get("SOURCECRAFT_ORG_SLUG", "yc")
    sc_repo = os.environ.get("SOURCECRAFT_REPO_SLUG", "clickhouse")

    client = SourceCraftClient(
        token=sc_token,
        base_url=sc_api_base,
        org_slug=sc_org,
        repo_slug=sc_repo,
    )

    # 1. Build release notes from git log
    release_notes = build_release_notes()
    log.info("Release notes:\n%s", release_notes)

    # 2. Create release
    log.info("Creating SourceCraft release for tag %s ...", release_tag)
    release = client.create_release(
        tag=release_tag,
        title=release_tag,
        release_notes=release_notes,
        publish=True,
    )
    release_id = release.get("id") or release.get("slug") or ""
    if release_id:
        log.info("Release created: id=%s", release_id)
    else:
        log.warning(
            "Release creation returned no id — release may already exist."
        )

    log.info("Done. Release %s published.", release_tag)


if __name__ == "__main__":
    main()
