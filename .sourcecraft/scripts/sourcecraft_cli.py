#!/usr/bin/env python3
"""
SourceCraft CLI dispatcher.

Usage:
    python3 sourcecraft_cli.py <subcommand> [args...]

Subcommands:
    create-pr      <source_branch> <target_branch> <title> [description]
    post-comment   <pr_slug> <comment_text>
    rebase-poller  (no positional args — reads env vars only)

Expected environment variables (all subcommands):
    SOURCECRAFT_TOKEN  — API bearer token
    API_BASE           — API base URL (default: https://public-api.o.cloud.yandex.net)
    ORG_SLUG           — organisation slug (default: yc)
    REPO_SLUG          — repository slug  (default: clickhouse)

Extra env vars for rebase-poller:
    REBASE_LABEL       — label slug to watch (default: do-rebase)
"""

import argparse
import os
import sys

from sourcecraft_api import SourceCraftClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_client() -> SourceCraftClient:
    return SourceCraftClient(
        token=os.environ["SOURCECRAFT_TOKEN"],
        base_url=os.environ.get("API_BASE", "https://public-api.o.cloud.yandex.net"),
        org_slug=os.environ.get("ORG_SLUG", "yc"),
        repo_slug=os.environ.get("REPO_SLUG", "clickhouse"),
    )


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_create_pr(args: argparse.Namespace) -> None:
    client = _build_client()

    print(f"📬 Creating PR: '{args.title}' ({args.source_branch} → {args.target_branch})...")
    pr_slug = client.create_pull_request(
        source_branch=args.source_branch,
        target_branch=args.target_branch,
        title=args.title,
        description=args.description,
        publish=True,
    )

    if pr_slug != "<unknown>":
        print(f"✅ PR created successfully: {pr_slug}")
    else:
        print("❌ Failed to create PR", file=sys.stderr)
        sys.exit(1)


def cmd_post_comment(args: argparse.Namespace) -> None:
    client = _build_client()

    print(f"📝 Adding comment to PR '{args.pr_slug}'...")
    comment_slug = client.create_pull_request_comment(args.pr_slug, args.comment_text)

    if comment_slug != "<unknown>":
        print(f"✅ Comment added successfully: {comment_slug}")
    else:
        print("❌ Failed to add comment", file=sys.stderr)
        sys.exit(1)


def cmd_rebase_poller(_args: argparse.Namespace) -> None:
    rebase_label = os.environ.get("REBASE_LABEL", "do-rebase")
    client = _build_client()

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


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sourcecraft_cli.py",
        description="SourceCraft CLI dispatcher",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # -- create-pr -----------------------------------------------------------
    p_create = subparsers.add_parser("create-pr", help="Create a pull request")
    p_create.add_argument("source_branch", help="Source branch name")
    p_create.add_argument("target_branch", help="Target branch name")
    p_create.add_argument("title",         help="PR title")
    p_create.add_argument("description",   nargs="?", default="", help="PR description (optional)")
    p_create.set_defaults(func=cmd_create_pr)

    # -- post-comment --------------------------------------------------------
    p_comment = subparsers.add_parser("post-comment", help="Add a comment to a pull request")
    p_comment.add_argument("pr_slug",      help="PR slug")
    p_comment.add_argument("comment_text", help="Comment text")
    p_comment.set_defaults(func=cmd_post_comment)

    # -- rebase-poller -------------------------------------------------------
    p_poller = subparsers.add_parser(
        "rebase-poller",
        help="Poll open PRs and trigger rebase workflow for labelled ones",
    )
    p_poller.set_defaults(func=cmd_rebase_poller)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
