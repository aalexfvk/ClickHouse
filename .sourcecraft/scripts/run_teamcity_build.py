#!/usr/bin/env python3
"""
Trigger a TeamCity build by tag from SourceCraft.

Port of the run_build/impl/__init__.py tasklet to plain requests.
Reads parameters from environment variables:
  - TEAMCITY_URL: base TeamCity URL (e.g. https://teamcity.aw.cloud.yandex.ru)
  - TEAMCITY_BUILD_TYPE_ID: build configuration id
  - TEAMCITY_TOKEN: Bearer token for the REST API
  - TEAMCITY_BUILD_BRANCH: tag name (without refs/tags/); if not set explicitly,
    taken from SOURCECRAFT_COMMIT_REF_NAME (set automatically by SourceCraft)

Logic:
  1. POST {TEAMCITY_URL}/app/rest/buildQueue with build parameters
  2. Print webUrl to stdout
  3. Poll build['href'] every 60 s until state=finished
  4. exit 0 on status=SUCCESS, otherwise exit 1

Release publishing is handled by a separate cube that runs publish_release.py
after this script exits successfully.
"""

import datetime
import logging
import os
import sys
import time
from dataclasses import dataclass

import requests

POLL_INTERVAL_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 10
RETRY_DELAY_SECONDS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    teamcity_url: str
    build_type_id: str
    token: str
    build_branch: str

    @classmethod
    def from_env(cls) -> "Config":
        build_branch = os.environ.get("TEAMCITY_BUILD_BRANCH") or os.environ[
            "SOURCECRAFT_COMMIT_REF_NAME"
        ]
        return cls(
            teamcity_url=os.environ["TEAMCITY_URL"],
            build_type_id=os.environ["TEAMCITY_BUILD_TYPE_ID"],
            token=os.environ["TEAMCITY_TOKEN"],
            build_branch=build_branch,
        )

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    @property
    def build_branch_ref(self) -> str:
        return f"refs/tags/{self.build_branch}"


# ---------------------------------------------------------------------------
# TeamCity helpers
# ---------------------------------------------------------------------------

def get_with_retry(url: str, headers: dict, max_retries: int = MAX_RETRIES) -> dict:
    """Perform a GET request, retrying on transient HTTP errors (5xx, connection issues)."""
    last_exc: Exception = RuntimeError("unreachable")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            last_exc = e
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                if e.response.status_code < 500:
                    raise
            if attempt == max_retries:
                break
            log.warning(
                "Request failed (attempt %d/%d): %s — retrying in %ds",
                attempt,
                max_retries,
                e,
                RETRY_DELAY_SECONDS,
            )
            time.sleep(RETRY_DELAY_SECONDS)
    raise last_exc


def trigger_build(cfg: Config) -> dict:
    """Enqueue a new build in TeamCity and return the build object."""
    data = {
        "buildType": {"id": cfg.build_type_id},
        "properties": {
            "property": [
                {"name": "teamcity.build.branch", "value": cfg.build_branch_ref}
            ]
        },
        "triggeringOptions": {"queueAtTop": True},
    }

    log.info(
        "Starting build: buildTypeId=%s, branch=%s",
        cfg.build_type_id,
        cfg.build_branch_ref,
    )

    response = requests.post(
        f"{cfg.teamcity_url}/app/rest/buildQueue",
        headers=cfg.headers,
        json=data,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def poll_until_finished(cfg: Config, build_href: str) -> dict:
    """Poll the build status every POLL_INTERVAL_SECONDS until state=finished."""
    elapsed = 0
    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

        build = get_with_retry(
            f"{cfg.teamcity_url}{build_href}",
            headers=cfg.headers,
        )

        state = build.get("state", "unknown")
        status = build.get("status", "unknown")
        percentage = build.get("percentageComplete", 0)
        status_text = build.get("statusText", "")

        elapsed_str = str(datetime.timedelta(seconds=elapsed))
        if state == "queued":
            log.info("[%s] queued: %d%%", elapsed_str, percentage)
        elif state == "running":
            log.info("[%s] running: %d%% - %s", elapsed_str, percentage, status_text)
        elif state == "finished":
            log.info("[%s] finished: status=%s", elapsed_str, status)
            return build
        else:
            log.info("[%s] state=%s, status=%s", elapsed_str, state, status)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    cfg = Config.from_env()

    build = trigger_build(cfg)

    build_url = build.get("webUrl", "<unknown>")
    build_href = build.get("href", "")
    build_id = build.get("id", "<unknown>")

    log.info("Build queued: id=%s, webUrl=%s", build_id, build_url)

    if not build_href:
        log.error("Build href not found in response")
        sys.exit(1)

    build = poll_until_finished(cfg, build_href)

    status = build.get("status", "unknown")
    status_text = build.get("statusText", "")

    if status == "SUCCESS":
        log.info("SUCCESS: %s", build_url)
        sys.exit(0)
    else:
        log.error("FAILURE: %s - %s", status, status_text)
        log.error("Build URL: %s", build_url)
        sys.exit(1)


if __name__ == "__main__":
    main()
