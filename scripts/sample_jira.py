import os
import textwrap
from collections import Counter
from typing import List, Dict, Any

import requests
from PIL import Image, ImageDraw, ImageFont

JIRA_BASE_URL = "https://something.atlassian.net"
PROJECT_KEY = "PK"
JIRA_EMAIL = "user@test.com"
JIRA_API_TOKEN = "TOKEN GOES HERE"
PNG_OUTPUT_PATH = "/static/spaces/332446952d9c4dcd8c22a2c8a7dc0e5c.png"
TARGET_FIX_VERSION = "1.0"

SEARCH_URL = f"{JIRA_BASE_URL}/rest/api/3/search/jql"
FIELDS_URL = f"{JIRA_BASE_URL}/rest/api/3/field"
PAGE_SIZE = 100


class JiraError(RuntimeError):
    """Raised when Jira operations fail."""


def build_session() -> requests.Session:
    session = requests.Session()
    session.auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    session.headers.update({"Accept": "application/json"})
    return session


def _load_json(response: requests.Response, context: str) -> Dict[str, Any]:
    if response.status_code != 200:
        try:
            problem = response.json()
        except ValueError:
            raise JiraError(f"{context} failed (HTTP {response.status_code}): {response.text}")
        message = "; ".join(problem.get("errorMessages") or []) or problem.get("message") or str(problem)
        raise JiraError(f"{context} failed (HTTP {response.status_code}): {message}")
    try:
        return response.json()
    except ValueError as exc:
        raise JiraError(f"{context} returned non-JSON data") from exc


def fetch_field_metadata(session: requests.Session) -> List[Dict[str, Any]]:
    response = session.get(FIELDS_URL, timeout=30)
    data = _load_json(response, "Fetching Jira fields")
    if not isinstance(data, list):
        raise JiraError("Unexpected field metadata format returned by Jira")
    return data


def detect_team_field_keys(field_metadata: List[Dict[str, Any]]) -> List[str]:
    candidates = []
    for field in field_metadata:
        field_id = field.get("id") or field.get("key")
        if not field_id:
            continue
        name = (field.get("name") or "").strip()
        lower = name.lower()
        if "team" in lower or "squad" in lower:
            if lower == "team":
                priority = 0
            elif "owning" in lower or "assigned" in lower:
                priority = 1
            else:
                priority = 2
            candidates.append((priority, field_id))
    ordered_keys: List[str] = []
    for _, field_id in sorted(candidates, key=lambda pair: pair[0]):
        if field_id not in ordered_keys:
            ordered_keys.append(field_id)
    return ordered_keys


def fetch_issues(session: requests.Session, jql: str, fields: List[str]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    next_token = None
    params = {"jql": jql, "maxResults": PAGE_SIZE}
    if fields:
        params["fields"] = ",".join(fields)
    while True:
        page_params = dict(params)
        if next_token:
            page_params["nextPageToken"] = next_token
        response = session.get(SEARCH_URL, params=page_params, timeout=60)
        data = _load_json(response, "Querying Jira issues")
        page_issues = data.get("issues")
        if page_issues is None:
            raise JiraError("Jira response missing 'issues' data")
        issues.extend(page_issues)
        if data.get("isLast", True):
            break
        next_token = data.get("nextPageToken")
        if not next_token:
            break
    return issues


def extract_label(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("value", "name", "displayName", "label"):
            label = value.get(key)
            if label:
                return str(label)
        return ""
    if isinstance(value, list):
        parts = [extract_label(item) if isinstance(item, dict) else str(item) for item in value]
        parts = [part for part in (p.strip() for p in parts) if part]
        return ", ".join(parts)
    return str(value).strip()


def extract_team(fields: Dict[str, Any], team_field_keys: List[str]) -> str:
    for key in team_field_keys:
        label = extract_label(fields.get(key))
        if label:
            return label
    return "Unassigned"


def count_issues_by_team(issues: List[Dict[str, Any]], team_field_keys: List[str]) -> Counter:
    counts: Counter = Counter()
    for issue in issues:
        fields = issue.get("fields") or {}
        team_name = extract_team(fields, team_field_keys)
        counts[team_name] += 1
    return counts


def load_font(size: int) -> ImageFont.FreeTypeFont:
    for font_path in ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "DejaVuSans.ttf"):
        if os.path.exists(font_path):
            try:
                return ImageFont.truetype(font_path, size)
            except OSError:
                continue
    return ImageFont.load_default()


def measure_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> (int, int):
    if hasattr(draw, "textbbox"):
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]
    return draw.textsize(text, font=font)


def wrap_label(label: str, max_line_length: int = 12, max_lines: int = 3) -> str:
    if not label:
        return ""
    wrapped = textwrap.wrap(label, width=max_line_length) or [label]
    if len(wrapped) > max_lines:
        wrapped = wrapped[:max_lines]
        wrapped[-1] = wrapped[-1][: max(1, max_line_length - 1)] + "…"
    return "\n".join(wrapped)


def draw_centered_multiline(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    x_center: float,
    y_top: float,
    line_spacing: int = 4,
    fill: str = "black",
) -> None:
    lines = text.split("\n") if text else [""]
    y = y_top
    for line in lines:
        line_w, line_h = measure_text(draw, line, font)
        draw.text((x_center - line_w / 2, y), line, fill=fill, font=font)
        y += line_h + line_spacing


def build_chart(team_counts: Counter) -> Image.Image:
    sorted_items = sorted(team_counts.items(), key=lambda item: (-item[1], item[0].lower()))
    if not sorted_items:
        raise JiraError("No matching Jira issues found for visualization")

    bar_width = 80
    bar_gap = 40
    left_margin = 140
    right_margin = 120
    top_margin = 100
    bottom_margin = 160
    chart_height = 420
    num_bars = len(sorted_items)
    width = max(640, left_margin + right_margin + num_bars * bar_width + max(0, num_bars - 1) * bar_gap)
    height = top_margin + chart_height + bottom_margin

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    title_font = load_font(30)
    label_font = load_font(16)
    tick_font = load_font(14)

    title = f"Remaining ZT Fix {TARGET_FIX_VERSION} Tasks by Team"
    title_width, title_height = measure_text(draw, title, title_font)
    draw.text(((width - title_width) / 2, 30), title, fill="black", font=title_font)

    chart_left = left_margin
    chart_bottom = top_margin + chart_height
    chart_top = top_margin
    chart_right = width - right_margin

    draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill="black", width=2)
    draw.line([(chart_left, chart_bottom), (chart_left, chart_top)], fill="black", width=2)

    max_count = max(count for _, count in sorted_items)
    scale = chart_height / max_count if max_count else 1

    # Y-axis ticks
    tick_step = max(1, max_count // 5)
    tick_values = list(range(0, max_count, tick_step))
    if not tick_values or tick_values[-1] != max_count:
        tick_values.append(max_count)
    for value in tick_values:
        y = chart_bottom - value * scale
        draw.line([(chart_left - 5, y), (chart_left, y)], fill="black", width=1)
        tick_label = str(value)
        label_w, label_h = measure_text(draw, tick_label, tick_font)
        draw.text((chart_left - 10 - label_w, y - label_h / 2), tick_label, fill="black", font=tick_font)

    for index, (team, count) in enumerate(sorted_items):
        x0 = chart_left + index * (bar_width + bar_gap)
        x1 = x0 + bar_width
        bar_height = count * scale
        y0 = chart_bottom - bar_height
        draw.rectangle([x0, y0, x1, chart_bottom], fill="#4C72B0")

        count_text = str(count)
        text_w, text_h = measure_text(draw, count_text, label_font)
        draw.text((x0 + (bar_width - text_w) / 2, y0 - text_h - 6), count_text, fill="black", font=label_font)

        wrapped_label = wrap_label(team)
        draw_centered_multiline(draw, wrapped_label, tick_font, x0 + bar_width / 2, chart_bottom + 10)

    return image


def ensure_output_directory(path: str) -> None:
    directory = os.path.dirname(path)
    if not directory:
        return
    os.makedirs(directory, exist_ok=True)


def render() -> None:
    session = build_session()
    fields = fetch_field_metadata(session)
    team_field_keys = detect_team_field_keys(fields)
    if not team_field_keys:
        raise JiraError("Could not find any Jira field containing 'team' or 'squad'")

    jql = (
        f'project = "{PROJECT_KEY}" AND statusCategory != Done AND issuetype != Epic '
        f'AND ("Fix for" = "{TARGET_FIX_VERSION}" OR fixVersion = "{TARGET_FIX_VERSION}")'
    )

    fetch_fields = sorted(set(["summary"] + team_field_keys))
    issues = fetch_issues(session, jql, fetch_fields)
    counts = count_issues_by_team(issues, team_field_keys)

    if not counts:
        raise JiraError("No Jira issues matched the provided filters")

    image = build_chart(counts)
    ensure_output_directory(PNG_OUTPUT_PATH)
    image.save(PNG_OUTPUT_PATH, format="PNG")


if __name__ == "__main__":
    render()
