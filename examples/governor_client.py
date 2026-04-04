from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


AGENT_ID = "starter-agent"


def _get_json(base_url: str, path: str, token: str) -> Any:
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-VEI-Agent-Id": AGENT_ID,
            "X-VEI-Agent-Name": "starter-agent",
            "X-VEI-Agent-Role": "external-agent",
            "X-VEI-Agent-Team": "external",
            "User-Agent": "vei-governor-client/1.0",
        },
        method="GET",
    )
    with urlopen(request, timeout=10) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _post_json(base_url: str, path: str, token: str, payload: dict[str, Any]) -> Any:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-VEI-Agent-Id": AGENT_ID,
            "X-VEI-Agent-Name": "starter-agent",
            "X-VEI-Agent-Role": "external-agent",
            "X-VEI-Agent-Team": "external",
            "User-Agent": "vei-governor-client/1.0",
        },
        data=body,
        method="POST",
    )
    with urlopen(request, timeout=10) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _register_proxy_agent(base_url: str, token: str) -> None:
    _post_json(
        base_url,
        "/api/governor/agents",
        token,
        {
            "agent_id": AGENT_ID,
            "name": "starter-agent",
            "mode": "proxy",
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Quick governor client for the VEI compatibility gateway."
    )
    parser.add_argument("--base-url", required=True, help="Twin gateway base URL")
    parser.add_argument("--token", required=True, help="Twin bearer token")
    parser.add_argument(
        "--post-message",
        default="",
        help="Optional Slack message to post into the first available channel",
    )
    args = parser.parse_args()

    _register_proxy_agent(args.base_url, args.token)

    slack_channels = _get_json(
        args.base_url,
        "/slack/api/conversations.list",
        args.token,
    )
    jira_issues = _get_json(
        args.base_url,
        "/jira/rest/api/3/search",
        args.token,
    )
    graph_messages = _get_json(
        args.base_url,
        "/graph/v1.0/me/messages",
        args.token,
    )
    salesforce_records = _get_json(
        args.base_url,
        "/salesforce/services/data/v60.0/query?"
        + urlencode({"q": "SELECT Id, Name FROM Opportunity LIMIT 2"}),
        args.token,
    )

    summary = {
        "slack_channels": len(slack_channels.get("channels", [])),
        "jira_issues": len(jira_issues.get("issues", [])),
        "mail_messages": len(graph_messages.get("value", [])),
        "crm_records": len(salesforce_records.get("records", [])),
    }

    if args.post_message and slack_channels.get("channels"):
        channel_id = str(slack_channels["channels"][0]["id"])
        post_result = _post_json(
            args.base_url,
            "/slack/api/chat.postMessage",
            args.token,
            {"channel": channel_id, "text": args.post_message},
        )
        summary["posted_message"] = {
            "channel": channel_id,
            "ok": bool(post_result.get("ok")),
        }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
