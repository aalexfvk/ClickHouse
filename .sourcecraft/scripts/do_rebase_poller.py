#!/usr/bin/env python3
"""
Poll open PRs every time this script is invoked and trigger
apply_rebase_candidate for any PR that carries the 'do-rebase' label.

Expected environment variables:
  SOURCECRAFT_TOKEN  — API bearer token
  API_BASE           — API base URL (e.g. https://public-api.o.cloud.yandex.net)
  ORG_SLUG           — organisation slug (e.g. yc)
  REPO_SLUG          — repository slug  (e.g. clickhouse)
  REBASE_LABEL       — label slug to watch (default: do-rebase)
"""

import os
import sys

from sourcecraft_api import SourceCraftClient


def main() -> None:
    token        = os.environ["SOURCECRAFT_TOKEN"]
    api_base     = os.environ["API_BASE"]
    org_slug     = os.environ["ORG_SLUG"]
    repo_slug    = os.environ["REPO_SLUG"]
    rebase_label = os.environ.get("REBASE_LABEL", "do-rebase")

    client = SourceCraftClient(
        token=token,
        base_url=api_base,
        org_slug=org_slug,
        repo_slug=repo_slug,
    )

    print(f"🔍 Fetching open PRs with label '{rebase_label}'...")
    pulls = client.list_open_pull_requests()
    print(f"   Total open PRs fetched: {len(pulls)}")

    processed = 0
    for pr in pulls:
        if rebase_label not in pr.label_slugs:
            continue

        print("=========================================")
        print(f"🎯 PR '{pr.slug}': source='{pr.source_branch}' target='{pr.target_branch}'")

        # Remove label first to prevent double-triggering on the next poll cycle
        remaining = client.remove_pull_request_labels(pr.slug, [rebase_label])
        print(f"  🏷️  Label '{rebase_label}' removed. Remaining labels: {remaining}")

        run_slug = client.trigger_workflow(
            workflow_name="apply_rebase_candidate",
            inputs={
                "candidate_branch": pr.source_branch,
                "target_branch":    pr.target_branch,
                "pr_number":        pr.slug,
            },
        )
        print(f"  ✅ Workflow run started: {run_slug}")
        processed += 1

    print("=========================================")
    print(f"✅ Done. Processed {processed} PR(s) with label '{rebase_label}'.")


if __name__ == "__main__":
    main()
