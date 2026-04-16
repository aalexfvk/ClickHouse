#!/usr/bin/env python3
"""
CLI tool to add a comment to a pull request.

Usage:
    python3 post_pr_comment.py <pr_slug> <comment_text>

Expected environment variables:
  SOURCECRAFT_TOKEN  — API bearer token
  API_BASE           — API base URL (e.g. https://public-api.o.cloud.yandex.net)
  ORG_SLUG           — organisation slug (e.g. yc)
  REPO_SLUG          — repository slug  (e.g. clickhouse)
"""

import os
import sys

from sourcecraft_api import SourceCraftClient


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python3 post_pr_comment.py <pr_slug> <comment_text>", file=sys.stderr)
        sys.exit(1)

    pr_slug = sys.argv[1]
    comment_text = sys.argv[2]

    token     = os.environ["SOURCECRAFT_TOKEN"]
    api_base  = os.environ.get("API_BASE", "https://public-api.o.cloud.yandex.net")
    org_slug  = os.environ.get("ORG_SLUG", "yc")
    repo_slug = os.environ.get("REPO_SLUG", "clickhouse")

    client = SourceCraftClient(
        token=token,
        base_url=api_base,
        org_slug=org_slug,
        repo_slug=repo_slug,
    )

    print(f"📝 Adding comment to PR '{pr_slug}'...")
    comment_slug = client.create_pull_request_comment(pr_slug, comment_text)
    
    if comment_slug != "<unknown>":
        print(f"✅ Comment added successfully: {comment_slug}")
    else:
        print("❌ Failed to add comment", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
