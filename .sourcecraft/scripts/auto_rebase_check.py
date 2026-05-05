#!/usr/bin/env python3
import inspect
import os
import re
import subprocess
import sys

ALLOWED_SUFFIXES = ["stable", "lts"]


def run(cmd: list[str], check: bool = True, **kwargs) -> int:
    return subprocess.run(cmd, check=check, text=True, **kwargs).returncode


def run_output(cmd: list[str], **kwargs) -> str:
    return subprocess.run(cmd, check=True, text=True, capture_output=True, **kwargs).stdout.strip()


def get_latest_tag(release_line: str) -> str:
    patterns = []
    for suffix in ALLOWED_SUFFIXES:
        if release_line == "master":
            patterns.append(f"v*-{suffix}")
        else:
            patterns.append(f"v{release_line}.*-{suffix}")

    print(f"  [DEBUG] get_latest_tag: release_line={release_line} patterns={patterns}", file=sys.stderr)

    lines = run_output(["git", "tag", "-l", *patterns, "--sort=-v:refname"]).splitlines()
    first = lines[0] if lines else ""

    print(f"  [DEBUG] get_latest_tag: total_matching={len(lines)} result='{first}'", file=sys.stderr)
    print("  [DEBUG] get_latest_tag: first 5 tags:", file=sys.stderr)
    for line in lines[:5]:
        print(f"  {line}", file=sys.stderr)

    return first


def get_branches(input_branch: str) -> list[str]:
    if input_branch != "all":
        return [input_branch]

    raw = run_output(["git", "ls-remote", "--heads", "origin"])
    branches = []
    for line in raw.splitlines():
        if not line:
            continue
        branch = line.split("\t")[1].removeprefix("refs/heads/")
        if re.match(r"^yc/(master|\d+\.\d+)$", branch):
            branches.append(branch)
    return branches


def dry_run_rebase(branch: str, latest_tag: str, old_base: str) -> bool:
    """Attempt a rebase without pushing. Returns True if successful."""
    print("⏳ Starting dry-run rebase...")
    run(["git", "checkout", "--detach", f"origin/{branch}", "--quiet"])
    ok = run(["git", "rebase", "--onto", latest_tag, old_base, "HEAD"],
             check=False, capture_output=True) == 0
    if not ok:
        run(["git", "rebase", "--abort"])
    run(["git", "reset", "--hard", "HEAD", "--quiet"])
    return ok


def full_rebase_and_push(branch: str, latest_tag: str, old_base: str, candidate: str) -> bool:
    """Rebase onto latest_tag and push the candidate branch. Returns True if rebase succeeded."""
    print("⏳ Starting full rebase and PR creation...")
    run(["git", "checkout", "-B", candidate, f"origin/{branch}", "--quiet"])
    ok = run(["git", "rebase", "--onto", latest_tag, old_base, candidate],
             check=False, capture_output=True) == 0
    if not ok:
        run(["git", "rebase", "--abort"])
        run(["git", "checkout", "-B", candidate, f"origin/{branch}", "--quiet"])
    run(["git", "push", "origin", candidate, "--force", "--quiet"])
    return ok


def process_branch(branch: str, create_pr: bool) -> bool:
    """Process a single branch. Returns True if a conflict was detected."""
    print(f"\n=========================================")
    print(f"Processing branch: {branch}")
    print(f"=========================================")

    run(["git", "fetch", "origin", branch, "--quiet"])

    try:
        old_base = run_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match", "v*", f"origin/{branch}"]
        )
    except subprocess.CalledProcessError:
        old_base = ""

    if not old_base:
        print(f"::warning::No upstream tag found on {branch}, skipping.")
        return False

    print(f"Current base: {old_base}")

    release_line = branch.removeprefix("yc/")
    latest_tag = get_latest_tag(release_line)

    if not latest_tag:
        print(f"::warning::No upstream tag found for release line {release_line}, skipping.")
        return False

    print(f"Latest upstream tag: {latest_tag}")

    if old_base == latest_tag:
        print(f"✅ {branch} is already based on {latest_tag}, nothing to do.")
        return False

    print(f"⚠️ {branch} needs rebase: {old_base} → {latest_tag}")
    current_sha = run_output(["git", "rev-parse", "--short=10", f"origin/{branch}"])
    candidate = f"yc/rebase/{release_line}/{latest_tag}/from-{current_sha}"

    if run(["git", "ls-remote", "--exit-code", "--heads", "origin", candidate],
           check=False, capture_output=True) == 0:
        print(f"ℹ️ Candidate branch {candidate} already exists. Skipping.")
        return False

    if not create_pr:
        ok = dry_run_rebase(branch, latest_tag, old_base)
        if ok:
            print(f"✅ Dry-run rebase of {branch} onto {latest_tag} succeeded.")
        else:
            print(f"::error::Dry-run rebase of {branch} onto {latest_tag} has CONFLICTS.")
        return not ok

    rebase_ok = full_rebase_and_push(branch, latest_tag, old_base, candidate)

    if rebase_ok:
        pr_title = f"[Auto-rebase] {branch} → {latest_tag}"
        pr_desc = inspect.cleandoc(f"""
            ### 🤖 Automated Rebase PR

            **Type:** Auto-rebase
            **Branch:** {branch}
            **Previous Base:** {old_base}
            **Upstream Tag:** {latest_tag}

            ---
            ⚠️ **DO NOT MERGE MANUALLY**
            1. Wait for checks to pass.
            2. Use the **Apply Rebase** pipeline to finalize.
        """)
    else:
        pr_title = f"[Auto-rebase CONFLICT] {branch} → {latest_tag}"
        pr_desc = inspect.cleandoc(f"""
            ### ⚠️ Rebase Conflict — Manual Fix Required

            **Type:** Manual resolution
            **Branch:** {branch}
            **Previous Base:** {old_base}
            **Upstream Tag:** {latest_tag}
            **Base Commit:** {current_sha}

            Rebase failed with conflicts. Please:
            1. `git fetch origin && git checkout {candidate}`
            2. `git rebase --onto {latest_tag} {old_base}`
            3. Resolve conflicts, then `git push origin {candidate} --force`
            4. CI will re-run tests automatically.
        """)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    cli_path = os.path.join(script_dir, "sourcecraft_cli.py")
    print("Current directory: ", os.getcwd())
    run(["python3", cli_path, "create-pr", candidate, branch, pr_title, pr_desc])
    print(f"✅ PR created: {pr_title}")
    run(["git", "checkout", "--detach", "HEAD", "--quiet"])
    return not rebase_ok


def main() -> None:
    create_pr = os.environ.get("CREATE_PR", "false") == "true"
    input_branch = os.environ.get("INPUT_BRANCH", "all")

    print(f"Mode: {'full (create PR)' if create_pr else 'dry-run (check only)'}")

    branches = get_branches(input_branch)
    print(f"Branches to check: {' '.join(branches)}")

    failed_branches: list[str] = []
    for branch in branches:
        if process_branch(branch, create_pr):
            failed_branches.append(branch)

    if failed_branches:
        print("\n❌ The following branches have rebase conflicts:\n")
        for fb in failed_branches:
            print(f"  - {fb}")
        if create_pr:
            print("::error::PRs created for conflicting branches. Manual resolution required.")
        else:
            print("::error::Dry-run conflicts detected. Manual intervention required before creating PRs.")
        sys.exit(1)

    print("\n✅ All branches processed successfully.")


if __name__ == "__main__":
    main()
