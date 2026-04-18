from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from textwrap import wrap

from PIL import Image, ImageDraw, ImageFont

TIMELINE_IMAGE_PATH = Path("docs/assets/enron-whatif/enron-bankruptcy-arc-timeline.png")
TIMELINE_MARKDOWN_PATH = Path(
    "docs/examples/enron-master-agreement-public-context/timeline_arc.md"
)


@dataclass(frozen=True)
class TimelineEvent:
    when: str
    label: str
    detail: str
    color: str


TIMELINE_EVENTS = (
    TimelineEvent(
        when="1999-05-12",
        label="PG&E deal bundle",
        detail="Legal and counterparty pressure shows up early in the repo-owned PG&E power-deal example.",
        color="#2f6bff",
    ),
    TimelineEvent(
        when="2000-09-27",
        label="Master Agreement bundle",
        detail="The contract draft branch point now sits inside the wider market and trust arc.",
        color="#154f3b",
    ),
    TimelineEvent(
        when="2000-12-15",
        label="California strategy bundle",
        detail="The preservation-order branch ties the trading desk to later regulatory fallout.",
        color="#b44f1e",
    ),
    TimelineEvent(
        when="2001-04-06",
        label="PG&E Chapter 11",
        detail="The counterparty credit story becomes impossible to treat as a purely legal thread.",
        color="#7e3f98",
    ),
    TimelineEvent(
        when="2001-06-19",
        label="FERC Western refunds",
        detail="The California crisis picks up the public regulatory backdrop tracked in the fixture.",
        color="#7e3f98",
    ),
    TimelineEvent(
        when="2001-08-14",
        label="Skilling resignation",
        detail="The public trust collapse accelerates before the late-October accounting spiral.",
        color="#c2293d",
    ),
    TimelineEvent(
        when="2001-10-30",
        label="Watkins bundle",
        detail="The Watkins memo path lands inside the repo as a saved whistleblower branch point.",
        color="#c2293d",
    ),
    TimelineEvent(
        when="2002-03-14",
        label="Andersen indictment",
        detail="The bankruptcy arc continues past the bundle dates into the auditor collapse.",
        color="#222222",
    ),
)


def _parse_date(value: str) -> datetime:
    return datetime.fromisoformat(f"{value}T00:00:00+00:00").astimezone(UTC)


def _wrap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    *,
    font: ImageFont.ImageFont,
    max_width: int,
) -> str:
    words = text.split()
    if not words:
        return ""
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        left, _top, right, _bottom = draw.textbbox((0, 0), candidate, font=font)
        if right - left <= max_width:
            current = candidate
            continue
        lines.append(current)
        current = word
    lines.append(current)
    return "\n".join(lines)


def render_timeline_image(path: Path = TIMELINE_IMAGE_PATH) -> Path:
    width = 1800
    height = 1080
    image = Image.new("RGB", (width, height), "#f7f2e8")
    draw = ImageDraw.Draw(image)
    title_font = ImageFont.load_default()
    body_font = ImageFont.load_default()
    small_font = ImageFont.load_default()

    draw.text(
        (90, 70),
        "Enron bankruptcy arc and repo-owned branch examples",
        fill="#171717",
        font=title_font,
    )
    draw.text(
        (90, 120),
        "Public macro events and saved what-if bundles on one line",
        fill="#4a4a4a",
        font=body_font,
    )

    timeline_y = 520
    start_x = 120
    end_x = width - 120
    draw.line((start_x, timeline_y, end_x, timeline_y), fill="#222222", width=6)

    parsed_events = [_parse_date(item.when) for item in TIMELINE_EVENTS]
    start_date = min(parsed_events)
    end_date = max(parsed_events)
    total_days = max(1, (end_date - start_date).days)

    for index, event in enumerate(TIMELINE_EVENTS):
        offset_days = (_parse_date(event.when) - start_date).days
        x = start_x + int((offset_days / total_days) * (end_x - start_x))
        draw.ellipse((x - 11, timeline_y - 11, x + 11, timeline_y + 11), fill=event.color)
        draw.text((x - 35, timeline_y + 22), event.when, fill="#333333", font=small_font)

        box_width = 360
        detail_text = _wrap_text(
            draw,
            event.detail,
            font=small_font,
            max_width=box_width - 32,
        )
        line_count = len(detail_text.splitlines()) if detail_text else 1
        box_height = 74 + (line_count * 16)
        box_top = 240 if index % 2 == 0 else 610
        box_left = min(max(30, x - box_width // 2), width - box_width - 30)
        box_right = box_left + box_width
        box_bottom = box_top + box_height
        draw.rounded_rectangle(
            (box_left, box_top, box_right, box_bottom),
            radius=18,
            fill="#fffaf1",
            outline=event.color,
            width=3,
        )
        connector_y = box_bottom if index % 2 == 0 else box_top
        draw.line((x, timeline_y, x, connector_y), fill=event.color, width=3)
        draw.text((box_left + 16, box_top + 14), event.label, fill="#171717", font=body_font)
        draw.multiline_text(
            (box_left + 16, box_top + 48),
            detail_text,
            fill="#4a4a4a",
            font=small_font,
            spacing=4,
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path)
    return path


def render_timeline_markdown(path: Path = TIMELINE_MARKDOWN_PATH) -> Path:
    lines = [
        "# Enron timeline arc",
        "",
        "The saved Enron bundles now cover four different branch points inside the broader Enron collapse story.",
        "",
        "## Arc",
        "",
    ]
    for event in TIMELINE_EVENTS:
        lines.append(f"- {event.when} — **{event.label}**: {event.detail}")
    lines.extend(
        [
            "",
            "## Reading order",
            "",
            "- Start with the PG&E power-deal bundle for the early counterparty-credit thread.",
            "- Move to the Master Agreement bundle for the contract-control branch inside the operating company.",
            "- Open the California strategy bundle for the trading and regulatory path.",
            "- End with the Watkins memo bundle for the late trust and accounting warning path.",
            "",
            f"![Enron timeline](../../assets/enron-whatif/{TIMELINE_IMAGE_PATH.name})",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> None:
    image_path = render_timeline_image()
    markdown_path = render_timeline_markdown()
    print(f"wrote: {image_path}")
    print(f"wrote: {markdown_path}")


if __name__ == "__main__":
    main()
