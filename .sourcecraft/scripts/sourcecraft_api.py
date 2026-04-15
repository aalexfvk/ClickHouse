"""
SourceCraft REST API client.

Usage:
    from sourcecraft_api import SourceCraftClient

    client = SourceCraftClient(
        token="...",
        base_url="https://public-api.o.cloud.yandex.net",
        org_slug="yc",
        repo_slug="clickhouse",
    )
"""

import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PullRequest:
    slug: str
    source_branch: str
    target_branch: str
    label_slugs: list[str] = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> "PullRequest":
        source = (data.get("source") or {}).get("ref") or data.get("source_branch", "")
        target = (data.get("target") or {}).get("ref") or data.get("target_branch", "")
        labels = [lbl.get("slug", "") for lbl in data.get("labels", [])]
        return PullRequest(
            slug=data["slug"],
            source_branch=source,
            target_branch=target,
            label_slugs=labels,
        )


class SourceCraftClient:
    """Thin wrapper around the SourceCraft public REST API."""

    def __init__(
        self,
        token: str,
        base_url: str,
        org_slug: str,
        repo_slug: str,
    ) -> None:
        self._token = token
        self._base = base_url.rstrip("/")
        self._org = org_slug
        self._repo = repo_slug

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, headers=self._headers(), method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            text = exc.read().decode(errors="replace")
            print(f"  ⚠️  HTTP {exc.code} {method} {url}: {text}", file=sys.stderr)
            return {}

    def _repo_path(self, suffix: str = "") -> str:
        return f"/repos/{self._org}/{self._repo}{suffix}"

    # ------------------------------------------------------------------
    # Pull Requests
    # ------------------------------------------------------------------

    def list_open_pull_requests(self) -> list[PullRequest]:
        """Return all open PRs (handles pagination automatically)."""
        pulls: list[PullRequest] = []
        page_token = ""
        while True:
            qs = "filter=status%3Dopen&page_size=50"
            if page_token:
                qs += f"&page_token={page_token}"
            resp = self._request("GET", self._repo_path(f"/pulls?{qs}"))
            for item in resp.get("pull_requests", []):
                pulls.append(PullRequest.from_dict(item))
            page_token = resp.get("next_page_token", "")
            if not page_token:
                break
        return pulls

    # ------------------------------------------------------------------
    # Labels
    # ------------------------------------------------------------------

    def remove_pull_request_labels(self, pr_slug: str, label_slugs: list[str]) -> list[str]:
        """
        Remove *label_slugs* from the given PR.
        Returns the list of label slugs remaining on the PR.
        """
        resp = self._request(
            "DELETE",
            self._repo_path(f"/pulls/{pr_slug}/labels"),
            {"label_slugs": label_slugs},
        )
        return [lbl.get("slug", "") for lbl in resp.get("labels", [])]

    # ------------------------------------------------------------------
    # CI/CD
    # ------------------------------------------------------------------

    def trigger_workflow(
        self,
        workflow_name: str,
        inputs: dict[str, str],
        head_ref: str = "",
    ) -> str:
        """
        Start a CI run for *workflow_name* with the given *inputs*.
        Returns the run slug (or '<unknown>' on failure).
        """
        values = [{"name": k, "value": v} for k, v in inputs.items()]
        body: dict[str, Any] = {
            "workflows": [
                {
                    "name": workflow_name,
                    "values": values,
                }
            ]
        }
        if head_ref:
            body["head"] = {"ref": head_ref}

        resp = self._request("POST", self._repo_path("/cicd/runs"), body)
        return resp.get("slug", "<unknown>")
