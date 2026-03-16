from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import uuid
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from requests.auth import HTTPBasicAuth

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WORKSPACE_FILE = BASE_DIR / "workspace.json"
WORKSPACE_BACKUP_FILE = WORKSPACE_FILE.with_suffix(".bak")
SCRIPTS_DIR = BASE_DIR / "scripts"
STATIC_DIR = BASE_DIR / "static"
OUTPUT_DIR = STATIC_DIR / "spaces"
LEGACY_STATIC_DIR = BASE_DIR.parent.parent / "GIT_REPO_SQUIRREL_VIZIT" / "static"
CONNECTOR_CONFIG_FILE = BASE_DIR / "config.json"

MIN_SPACE_HEIGHT = 200
DEFAULT_SPACE_HEIGHT = 360
MIN_SPACE_WIDTH = 260
MAX_SPACE_WIDTH = 1400
DEFAULT_SPACE_WIDTH = 420
SPACE_CANVAS_PADDING = 24
SPACE_VERTICAL_GAP = 32

ALLOWED_COPILOT_MODELS = {
    "claude-sonnet-4.5",
    "claude-haiku-4.5",
    "claude-opus-4.5",
    "claude-sonnet-4",
    "gpt-5",
    "gpt-5.1",
    "gpt-5.1-codex-mini",
    "gpt-5.1-codex",
    "gpt-5-mini",
    "gpt-4.1",
    "gemini-3-pro-preview",
}
DEFAULT_COPILOT_MODEL = "gpt-5.1-codex"
MAX_COPILOT_ITERATIONS = 10
DEFAULT_COPILOT_ITERATIONS = MAX_COPILOT_ITERATIONS
DEFAULT_CONNECTOR_CONFIG = {
    "siteUrl": "",
    "projectKey": "",
    "accountEmail": "",
    "apiKey": "",
    "model": DEFAULT_COPILOT_MODEL,
    "copilotMaxIterations": DEFAULT_COPILOT_ITERATIONS,
}
MAX_EXISTING_SCRIPT_CONTEXT_CHARS = 6000
VERSION_LABEL_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

WORKSPACE_LOCK = threading.RLock()
CONFIG_LOCK = threading.RLock()

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["JSON_SORT_KEYS"] = False


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9\s_-]", "", value).strip().lower()
    cleaned = re.sub(r"\s+", "-", cleaned)
    return cleaned or f"node-{uuid.uuid4().hex[:6]}"


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text or "")


def coerce_iteration_limit(value: Any) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return DEFAULT_COPILOT_ITERATIONS
    return max(1, min(MAX_COPILOT_ITERATIONS, limit))


def resolve_iteration_limit(connector: Optional[Dict[str, Any]], override: Optional[Any] = None) -> int:
    if override is not None:
        return coerce_iteration_limit(override)
    if isinstance(connector, dict):
        return coerce_iteration_limit(connector.get("copilotMaxIterations"))
    return DEFAULT_COPILOT_ITERATIONS


def space_image_signature(space: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    migrate_legacy_space_image(space)
    path = space_image_path(space)
    if not path.exists():
        return None
    stat = path.stat()
    mtime_ns = getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))
    return (stat.st_size, mtime_ns)


def png_rendered_since(space: Dict[str, Any], previous_signature: Optional[Tuple[int, int]]) -> bool:
    migrate_legacy_space_image(space)
    path = space_image_path(space)
    if not path.exists():
        return False
    stat = path.stat()
    if stat.st_size <= 0:
        return False
    current_signature = (
        stat.st_size,
        getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)),
    )
    if previous_signature is None:
        return True
    return current_signature != previous_signature


def png_available(space: Dict[str, Any]) -> bool:
    migrate_legacy_space_image(space)
    path = space_image_path(space)
    return path.exists() and path.stat().st_size > 0


def file_signature(path: Path) -> Optional[Tuple[int, int]]:
    if not path.exists():
        return None
    stat = path.stat()
    size = stat.st_size
    timestamp = getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))
    return (size, timestamp)


def ndjson(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False) + "\n"


def normalize_site_url(site_url: str) -> str:
    if not site_url:
        return ""
    cleaned = site_url.strip()
    if not cleaned.lower().startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"
    return cleaned.rstrip("/")


def normalize_project_key(project_key: Optional[str]) -> str:
    if not project_key:
        return ""
    trimmed = project_key.strip()
    if not trimmed:
        return ""
    if trimmed.lstrip("-").isdigit():
        return str(abs(int(trimmed)))
    return trimmed.upper()


def describe_jira_error(response: Optional[requests.Response]) -> str:
    if response is None:
        return "Unknown Jira error"
    try:
        payload = response.json()
    except ValueError:
        payload = None

    messages: List[str] = []
    if isinstance(payload, dict):
        error_messages = payload.get("errorMessages")
        if isinstance(error_messages, list):
            messages.extend(str(item) for item in error_messages if item)
        field_errors = payload.get("errors")
        if isinstance(field_errors, dict):
            messages.extend(str(item) for item in field_errors.values() if item)
    if messages:
        return "; ".join(messages)

    text = (response.text or "").strip()
    if text:
        return text[:400]
    return f"HTTP {response.status_code}"


def build_jira_session(account_email: str, api_token: str) -> requests.Session:
    session = requests.Session()
    session.auth = HTTPBasicAuth(account_email, api_token)
    session.headers.update({"Accept": "application/json"})
    return session


def normalize_connector_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = dict(DEFAULT_CONNECTOR_CONFIG)
    if not isinstance(payload, dict):
        payload = {}
    site_url = normalize_site_url(payload.get("siteUrl", ""))
    project_key = normalize_project_key(payload.get("projectKey"))
    account_email = (payload.get("accountEmail") or "").strip()
    api_key = (payload.get("apiKey") or "").strip()
    model = payload.get("model")
    if isinstance(model, str) and model in ALLOWED_COPILOT_MODELS:
        base["model"] = model
    base.update(
        {
            "siteUrl": site_url,
            "projectKey": project_key,
            "accountEmail": account_email,
            "apiKey": api_key,
            "copilotMaxIterations": coerce_iteration_limit(payload.get("copilotMaxIterations")),
        }
    )
    if payload.get("updatedAt"):
        base["updatedAt"] = str(payload["updatedAt"])
    return base


def load_connector_config() -> Dict[str, Any]:
    with CONFIG_LOCK:
        if not CONNECTOR_CONFIG_FILE.exists():
            return dict(DEFAULT_CONNECTOR_CONFIG)
        try:
            data = json.loads(CONNECTOR_CONFIG_FILE.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return dict(DEFAULT_CONNECTOR_CONFIG)
    connector = normalize_connector_payload(data)
    return connector


def save_connector_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    connector = normalize_connector_payload(payload)
    connector["updatedAt"] = datetime.utcnow().isoformat()
    with CONFIG_LOCK:
        CONNECTOR_CONFIG_FILE.write_text(json.dumps(connector, indent=2), encoding="utf-8")
    return connector


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def ensure_workspace_file() -> None:
    if not WORKSPACE_FILE.exists():
        WORKSPACE_FILE.write_text(json.dumps({"nodes": []}, indent=2), encoding="utf-8")


def ensure_project_dirs() -> None:
    ensure_data_dir()
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


ensure_data_dir()
ensure_workspace_file()
ensure_project_dirs()


def ensure_folder_path(fs_path: str) -> None:
    ensure_data_dir()
    folder = DATA_DIR / fs_path
    folder.mkdir(parents=True, exist_ok=True)


def remove_folder_path(fs_path: str) -> None:
    if not fs_path:
        return
    target = (DATA_DIR / fs_path).resolve()
    base = DATA_DIR.resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)


def remove_tab_script_dir(slug: Optional[str]) -> None:
    if not slug:
        return
    target = (SCRIPTS_DIR / slug).resolve()
    base = SCRIPTS_DIR.resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)


def _merge_directories(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.mkdir(parents=True, exist_ok=True)
    for child in src.iterdir():
        target = dst / child.name
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            else:
                target.unlink()
        child.rename(target)
    shutil.rmtree(src, ignore_errors=True)


def rename_scripts_subdir(old_slug: Optional[str], new_slug: Optional[str]) -> None:
    if not old_slug or not new_slug or old_slug == new_slug:
        return
    old_dir = (SCRIPTS_DIR / old_slug).resolve()
    new_dir = (SCRIPTS_DIR / new_slug).resolve()
    if not old_dir.exists():
        return
    if new_dir.exists():
        _merge_directories(old_dir, new_dir)
    else:
        new_dir.parent.mkdir(parents=True, exist_ok=True)
        old_dir.rename(new_dir)


def rename_data_subdir(old_path: Optional[str], new_path: Optional[str]) -> None:
    if not old_path or not new_path or old_path == new_path:
        return
    old_dir = (DATA_DIR / old_path).resolve()
    new_dir = (DATA_DIR / new_path).resolve()
    if not old_dir.exists():
        return
    if new_dir.exists():
        _merge_directories(old_dir, new_dir)
    else:
        new_dir.parent.mkdir(parents=True, exist_ok=True)
        old_dir.rename(new_dir)


def rebase_space_python_path(path_value: Optional[str], old_slug: str, new_slug: str) -> Optional[str]:
    if not path_value or not old_slug or not new_slug or old_slug == new_slug:
        return path_value
    path_obj = Path(path_value)
    if not path_obj.is_absolute():
        parts = list(path_obj.parts)
        if len(parts) >= 2 and parts[0] == "scripts" and parts[1] == old_slug:
            parts[1] = new_slug
            return str(Path(*parts))
        return path_value
    try:
        rel = path_obj.relative_to(SCRIPTS_DIR)
    except ValueError:
        return path_value
    rel_parts = list(rel.parts)
    if not rel_parts or rel_parts[0] != old_slug:
        return path_value
    rel_parts[0] = new_slug
    new_abs = (SCRIPTS_DIR / Path(*rel_parts)).resolve()
    return str(new_abs)


def update_space_script_paths(spaces: Optional[List[Dict[str, Any]]], old_slug: str, new_slug: str) -> None:
    if not spaces:
        return
    for space in spaces:
        updated = rebase_space_python_path(space.get("python_path"), old_slug, new_slug)
        if updated:
            space["python_path"] = updated
        if space.get("tab_slug") != new_slug:
            space["tab_slug"] = new_slug
        for entry in space.get("versions", []) or []:
            rebased = rebase_space_python_path(entry.get("python_path"), old_slug, new_slug)
            if rebased:
                entry["python_path"] = rebased


def ensure_space_defaults(space: Dict[str, Any]) -> bool:
    modified = False
    if "id" not in space:
        space["id"] = uuid.uuid4().hex
        modified = True
    if "title" not in space:
        space["title"] = "Insight Space"
        modified = True
    if "python_path" not in space:
        space["python_path"] = ""
        modified = True
    if "image_path" not in space:
        space["image_path"] = f"spaces/{space['id']}.png"
        modified = True
    if "last_prompt" not in space:
        space["last_prompt"] = None
        modified = True
    if "height" not in space:
        space["height"] = DEFAULT_SPACE_HEIGHT
        modified = True
    if "width" not in space:
        space["width"] = DEFAULT_SPACE_WIDTH
        modified = True
    if "created_at" not in space:
        space["created_at"] = datetime.utcnow().isoformat()
        modified = True
    if "updated_at" not in space:
        space["updated_at"] = datetime.utcnow().isoformat()
        modified = True
    if "last_log" not in space:
        space["last_log"] = ""
        modified = True
    if "last_run_output" not in space:
        space["last_run_output"] = ""
        modified = True
    if "x" not in space:
        space["x"] = SPACE_CANVAS_PADDING
        modified = True
    if "y" not in space:
        space["y"] = SPACE_CANVAS_PADDING
        modified = True
    if "versions" not in space:
        space["versions"] = []
        modified = True
    if "active_version_id" not in space:
        space["active_version_id"] = None
        modified = True
    if "tab_slug" not in space:
        space["tab_slug"] = ""
        modified = True

    try:
        height_val = int(space.get("height", DEFAULT_SPACE_HEIGHT))
    except (TypeError, ValueError):
        height_val = DEFAULT_SPACE_HEIGHT
    height_val = max(MIN_SPACE_HEIGHT, height_val)
    if height_val != space["height"]:
        space["height"] = height_val
        modified = True

    try:
        width_val = int(space.get("width", DEFAULT_SPACE_WIDTH))
    except (TypeError, ValueError):
        width_val = DEFAULT_SPACE_WIDTH
    width_val = max(MIN_SPACE_WIDTH, min(MAX_SPACE_WIDTH, width_val))
    if width_val != space["width"]:
        space["width"] = width_val
        modified = True

    try:
        x_val = int(space.get("x", SPACE_CANVAS_PADDING))
    except (TypeError, ValueError):
        x_val = SPACE_CANVAS_PADDING
    x_val = max(0, x_val)
    if x_val != space["x"]:
        space["x"] = x_val
        modified = True

    try:
        y_val = int(space.get("y", SPACE_CANVAS_PADDING))
    except (TypeError, ValueError):
        y_val = SPACE_CANVAS_PADDING
    y_val = max(0, y_val)
    if y_val != space["y"]:
        space["y"] = y_val
        modified = True

    if "break_before" in space:
        space.pop("break_before", None)
        modified = True

    return modified


def next_space_position(tab_node: Dict[str, Any]) -> Tuple[int, int]:
    spaces = tab_node.get("spaces", []) or []
    if not spaces:
        return SPACE_CANVAS_PADDING, SPACE_CANVAS_PADDING

    max_bottom = SPACE_CANVAS_PADDING
    for existing in spaces:
        ensure_space_defaults(existing)
        try:
            y_val = int(existing.get("y", SPACE_CANVAS_PADDING))
        except (TypeError, ValueError):
            y_val = SPACE_CANVAS_PADDING
        try:
            height_val = int(existing.get("height", DEFAULT_SPACE_HEIGHT))
        except (TypeError, ValueError):
            height_val = DEFAULT_SPACE_HEIGHT
        current_bottom = max(0, y_val) + max(MIN_SPACE_HEIGHT, height_val) + SPACE_VERTICAL_GAP
        if current_bottom > max_bottom:
            max_bottom = current_bottom

    return SPACE_CANVAS_PADDING, max_bottom


def space_script_path(space: Dict[str, Any]) -> Path:
    path_str = space.get("python_path", "").strip()
    if not path_str:
        tab_slug = space_tab_slug(space)
        fallback = Path("scripts") / tab_slug / space["id"] / "working.py"
        return (BASE_DIR / fallback).resolve()
    path_obj = Path(path_str)
    if not path_obj.is_absolute():
        path_obj = (BASE_DIR / path_obj).resolve()
    return path_obj


def space_image_path(space: Dict[str, Any]) -> Path:
    rel = Path(space.get("image_path", f"spaces/{space['id']}.png"))
    if rel.is_absolute():
        return rel
    return (STATIC_DIR / rel).resolve()


def legacy_space_image_path(space: Dict[str, Any]) -> Optional[Path]:
    rel = Path(space.get("image_path", f"spaces/{space['id']}.png"))
    if rel.is_absolute():
        return rel
    return (LEGACY_STATIC_DIR / rel).resolve()


def _cleanup_empty_legacy_dirs(path: Path) -> None:
    try:
        legacy_root = LEGACY_STATIC_DIR.resolve()
        current = path.resolve()
    except OSError:
        return
    while current != legacy_root and current != current.parent:
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def migrate_legacy_space_image(space: Dict[str, Any]) -> bool:
    try:
        legacy_root = LEGACY_STATIC_DIR.resolve(strict=False)
    except OSError:
        return False
    if not legacy_root.exists():
        return False
    legacy_path = legacy_space_image_path(space)
    if not legacy_path or not legacy_path.exists():
        return False
    try:
        legacy_path.resolve().relative_to(legacy_root)
    except ValueError:
        return False
    target_path = space_image_path(space)
    try:
        if legacy_path.resolve() == target_path.resolve():
            return False
    except OSError:
        pass
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(legacy_path), str(target_path))
    except OSError:
        try:
            shutil.copy2(str(legacy_path), str(target_path))
            legacy_path.unlink(missing_ok=True)
        except OSError:
            return False
    _cleanup_empty_legacy_dirs(legacy_path.parent)
    return True


def delete_space_artifacts(space: Dict[str, Any]) -> None:
    for entry in space.get("versions", []) or []:
        delete_version_assets(entry)
    script_path = space_script_path(space)
    try:
        if script_path.exists():
            script_path.unlink()
    except OSError:
        pass
    scripts_root = (SCRIPTS_DIR / space_tab_slug(space) / space["id"]).resolve()
    if scripts_root.exists():
        shutil.rmtree(scripts_root, ignore_errors=True)
    image_path = space_image_path(space)
    try:
        if image_path.exists():
            image_path.unlink()
    except OSError:
        pass
    images_root = (OUTPUT_DIR / space["id"]).resolve()
    if images_root.exists():
        shutil.rmtree(images_root, ignore_errors=True)


def cleanup_node(node: Dict[str, Any]) -> None:
    if node["type"] == "tab":
        for space in node.get("spaces", []):
            delete_space_artifacts(space)
    elif node["type"] == "folder":
        for child in node.get("children", []):
            cleanup_node(child)


def hydrate_nodes(nodes: List[Dict[str, Any]], parent_id: Optional[str] = None, parent_path: Optional[str] = None) -> bool:
    changed = False
    for node in nodes:
        if "id" not in node:
            node["id"] = uuid.uuid4().hex
            changed = True
        if "children" not in node:
            node["children"] = []
            changed = True
        if "slug" not in node:
            node["slug"] = slugify(node["name"])
            changed = True
        fs_path = f"{parent_path}/{node['slug']}" if parent_path else node["slug"]
        node["fs_path"] = fs_path
        node["parent_id"] = parent_id

        if node["type"] == "folder":
            ensure_folder_path(fs_path)
            if hydrate_nodes(node["children"], node["id"], fs_path):
                changed = True
        elif node["type"] == "tab":
            if "content" not in node:
                node["content"] = f"Fresh canvas for {node['name']}"
                changed = True
            if "spaces" not in node:
                node["spaces"] = []
                changed = True
            spaces = node["spaces"]
            for space in spaces:
                if ensure_space_defaults(space):
                    changed = True
                if migrate_space_artifacts(space, node["slug"]):
                    changed = True
                if ensure_space_version_tracking(space, node["slug"]):
                    changed = True
        else:
            if "content" not in node:
                node["content"] = node.get("content", "Workspace item")
                changed = True
    return changed


def _default_script_rel(tab_slug: str, space_id: str) -> Path:
    return Path("scripts") / tab_slug / f"{space_id}.py"


def _default_image_rel(space_id: str) -> Path:
    return Path("spaces") / f"{space_id}.png"


def allocate_space_files(space: Dict[str, Any], tab_slug: str) -> Tuple[Path, Path]:
    space["tab_slug"] = tab_slug
    script_dir = (SCRIPTS_DIR / tab_slug / space["id"]).resolve()
    script_dir.mkdir(parents=True, exist_ok=True)
    image_dir = (OUTPUT_DIR / space["id"]).resolve()
    image_dir.mkdir(parents=True, exist_ok=True)
    return script_dir, image_dir


def _default_script_rel(tab_slug: str, space_id: str) -> Path:
    return Path("scripts") / tab_slug / f"{space_id}.py"


def _default_image_rel(space_id: str) -> Path:
    return Path("spaces") / f"{space_id}.png"


def _relativize_script_path(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


def _relativize_image_path(path: Path) -> str:
    try:
        rel = path.relative_to(STATIC_DIR)
        return str(rel)
    except ValueError:
        return str(path)


def _discover_script_candidates(space_id: str) -> List[Path]:
    candidates: List[Path] = []
    try:
        for match in SCRIPTS_DIR.rglob(f"{space_id}.py"):
            if match.is_file():
                candidates.append(match.resolve())
    except OSError:
        pass
    try:
        for folder in SCRIPTS_DIR.rglob(space_id):
            if folder.is_dir():
                for match in folder.glob("*.py"):
                    if match.is_file():
                        candidates.append(match.resolve())
    except OSError:
        pass
    unique: List[Path] = []
    seen: Set[Path] = set()
    for path in candidates:
        if not path.exists():
            continue
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(resolved)
    return unique


def _discover_image_candidate(space_id: str) -> Optional[Path]:
    try:
        for match in STATIC_DIR.rglob(f"{space_id}.png"):
            if match.is_file():
                return match.resolve()
    except OSError:
        return None
    return None


def _score_script_candidate(path: Path, tab_slug: str) -> Tuple[int, int]:
    try:
        rel = path.relative_to(SCRIPTS_DIR)
        parts = rel.parts
    except ValueError:
        parts = path.parts
    preference = 0
    if parts and parts[0] == tab_slug:
        preference -= 10
    if tab_slug in parts:
        preference -= 5
    return preference, len(parts)


def _select_script_candidate(candidates: List[Path], tab_slug: str) -> Optional[Path]:
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: _score_script_candidate(candidate, tab_slug))


def _is_new_layout_script_path(path: Path, space_id: str) -> bool:
    try:
        rel = path.relative_to(SCRIPTS_DIR)
    except ValueError:
        return False
    return space_id in rel.parts[:-1]


def migrate_space_artifacts(space: Dict[str, Any], tab_slug: str) -> bool:
    if not space or not space.get("id"):
        return False
    current_script_path = space_script_path(space)
    needs_migration = not current_script_path.exists() or not _is_new_layout_script_path(current_script_path, space["id"])
    if not needs_migration:
        return False

    candidates = []
    if current_script_path.exists():
        candidates.append(current_script_path)
    candidates.extend(_discover_script_candidates(space["id"]))
    candidate = _select_script_candidate(candidates, tab_slug)
    if not candidate:
        return False

    versions = space.setdefault("versions", [])
    version_id = space.get("active_version_id")
    if not version_id:
        if versions:
            version_id = versions[-1].get("id") or uuid.uuid4().hex
        else:
            version_id = uuid.uuid4().hex
        space["active_version_id"] = version_id

    target_dir = (SCRIPTS_DIR / tab_slug / space["id"]).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{version_id}.py"

    if not target_path.exists() or candidate.resolve() != target_path.resolve():
        try:
            shutil.move(str(candidate), str(target_path))
        except (OSError, shutil.Error):
            shutil.copy2(candidate, target_path)

    script_rel = _relativize_script_path(target_path)
    space["python_path"] = script_rel

    entry = next((item for item in versions if item.get("id") == version_id), None)
    if entry is None:
        timestamp = datetime.utcnow()
        entry = {
            "id": version_id,
            "label": format_version_label(timestamp),
            "created_at": timestamp.isoformat(),
            "prompt": space.get("last_prompt"),
            "log": space.get("last_log", ""),
            "run_output": space.get("last_run_output", ""),
        }
        versions.append(entry)
    entry["python_path"] = script_rel
    if not entry.get("image_path") and space.get("image_path"):
        entry["image_path"] = space.get("image_path")

    image_path = space_image_path(space)
    if not image_path.exists():
        image_candidate = _discover_image_candidate(space["id"])
        if image_candidate and image_candidate.exists():
            image_rel = _relativize_image_path(image_candidate)
            space["image_path"] = image_rel
            entry.setdefault("image_path", image_rel)

    space["updated_at"] = datetime.utcnow().isoformat()
    return True


def format_version_label(timestamp: datetime) -> str:
    return timestamp.strftime(VERSION_LABEL_FORMAT)


def space_tab_slug(space: Dict[str, Any], fallback: Optional[str] = None) -> str:
    slug = (space.get("tab_slug") or "").strip()
    if slug:
        return slug
    path_value = (space.get("python_path") or "").strip()
    if path_value:
        path_obj = Path(path_value)
        if not path_obj.is_absolute():
            parts = path_obj.parts
            if len(parts) >= 2 and parts[0] == "scripts":
                slug = parts[1]
                space["tab_slug"] = slug
                return slug
    if fallback:
        space["tab_slug"] = fallback
        return fallback
    return "workspace"


def delete_version_assets(entry: Dict[str, Any]) -> None:
    script_ref = (entry or {}).get("python_path")
    if script_ref:
        script_path = Path(script_ref)
        if not script_path.is_absolute():
            script_path = (BASE_DIR / script_path).resolve()
        try:
            if script_path.exists():
                script_path.unlink()
        except OSError:
            pass
    image_ref = (entry or {}).get("image_path")
    if image_ref:
        image_path = Path(image_ref)
        if not image_path.is_absolute():
            image_path = (STATIC_DIR / image_path).resolve()
        try:
            if image_path.exists():
                image_path.unlink()
        except OSError:
            pass


def ensure_space_version_tracking(space: Dict[str, Any], tab_slug: str) -> bool:
    allocate_space_files(space, tab_slug)
    versions = space.setdefault("versions", [])
    modified = False
    if space.get("tab_slug") != tab_slug:
        space["tab_slug"] = tab_slug
        modified = True
    if versions:
        if not space.get("active_version_id"):
            space["active_version_id"] = versions[-1]["id"]
            modified = True
        return modified
    timestamp = datetime.utcnow()
    version_id = space.get("active_version_id") or uuid.uuid4().hex
    script_ref = space.get("python_path") or str(Path("scripts") / tab_slug / f"{space['id']}.py")
    image_ref = space.get("image_path") or str(Path("spaces") / f"{space['id']}.png")
    entry = {
        "id": version_id,
        "label": format_version_label(timestamp),
        "created_at": timestamp.isoformat(),
        "python_path": script_ref,
        "image_path": image_ref,
        "prompt": space.get("last_prompt"),
        "log": space.get("last_log", ""),
        "run_output": space.get("last_run_output", ""),
    }
    versions.append(entry)
    space["active_version_id"] = version_id
    if space.get("python_path") != script_ref:
        space["python_path"] = script_ref
    if space.get("image_path") != image_ref:
        space["image_path"] = image_ref
    return True


def prune_future_versions(space: Dict[str, Any], active_index: int) -> bool:
    versions = space.setdefault("versions", [])
    if active_index < 0 or active_index >= len(versions) - 1:
        return False
    stale_entries = versions[active_index + 1 :]
    for entry in stale_entries:
        delete_version_assets(entry)
    del versions[active_index + 1 :]
    return bool(stale_entries)


def prune_versions_after_active(space: Dict[str, Any]) -> bool:
    versions = space.setdefault("versions", [])
    if not versions:
        return False
    active_id = space.get("active_version_id")
    if not active_id:
        return False
    try:
        active_index = next(index for index, entry in enumerate(versions) if entry.get("id") == active_id)
    except StopIteration:
        return False
    return prune_future_versions(space, active_index)


def begin_version_draft(space: Dict[str, Any], prompt_text: Optional[str] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    tab_slug = space_tab_slug(space)
    ensure_space_version_tracking(space, tab_slug)
    versions = space.setdefault("versions", [])
    active_id = space.get("active_version_id") or (versions[-1]["id"] if versions else None)
    space["active_version_id"] = active_id
    active_index = 0 if not versions else next((i for i, entry in enumerate(versions) if entry["id"] == active_id), len(versions) - 1)
    prune_future_versions(space, active_index)
    timestamp = datetime.utcnow()
    version_id = uuid.uuid4().hex
    script_rel = Path("scripts") / tab_slug / space["id"] / f"{version_id}.py"
    image_rel = Path("spaces") / space["id"] / f"{version_id}.png"
    script_path = (BASE_DIR / script_rel).resolve()
    script_path.parent.mkdir(parents=True, exist_ok=True)
    image_path = (STATIC_DIR / image_rel).resolve()
    image_path.parent.mkdir(parents=True, exist_ok=True)
    previous_state = {
        "python_path": space.get("python_path"),
        "image_path": space.get("image_path"),
        "active_version_id": space.get("active_version_id"),
    }
    space["python_path"] = str(script_rel)
    space["image_path"] = str(image_rel)
    space["active_version_id"] = version_id
    draft = {
        "id": version_id,
        "label": format_version_label(timestamp),
        "created_at": timestamp.isoformat(),
        "python_path": str(script_rel),
        "image_path": str(image_rel),
        "prompt": prompt_text or space.get("last_prompt"),
    }
    return draft, previous_state


def rollback_version_draft(space: Dict[str, Any], previous_state: Optional[Dict[str, Any]], draft: Optional[Dict[str, Any]]) -> None:
    if draft:
        delete_version_assets(draft)
    prior = previous_state or {}
    space["python_path"] = prior.get("python_path", space.get("python_path", ""))
    space["image_path"] = prior.get("image_path", space.get("image_path", ""))
    space["active_version_id"] = prior.get("active_version_id")


def commit_version_entry(space: Dict[str, Any], draft: Optional[Dict[str, Any]]) -> None:
    if not draft:
        return
    versions = space.setdefault("versions", [])
    versions[:] = [entry for entry in versions if entry.get("id") != draft["id"]]
    entry = {
        "id": draft["id"],
        "label": draft["label"],
        "created_at": draft["created_at"],
        "python_path": draft["python_path"],
        "image_path": draft["image_path"],
        "prompt": space.get("last_prompt"),
        "log": space.get("last_log", ""),
        "run_output": space.get("last_run_output", ""),
    }
    versions.append(entry)
    space["active_version_id"] = draft["id"]
    space["updated_at"] = draft["created_at"]


def get_active_version_entry(space: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    active_id = space.get("active_version_id")
    if not active_id:
        return None
    for entry in space.get("versions", []) or []:
        if entry.get("id") == active_id:
            return entry
    return None


def build_script_content(space: Dict[str, Any], prompt_text: Optional[str] = None) -> str:
    prompt = prompt_text or "Placeholder insight"
    prompt_repr = repr(prompt)
    output_path = space_image_path(space)
    return f"""from __future__ import annotations

from PIL import Image, ImageDraw, ImageFont
from datetime import datetime

PROMPT = {prompt_repr}
OUTPUT_PATH = r"{output_path}"


def render():
    width, height = 1200, 720
    base = Image.new("RGB", (width, height), "#040d1e")
    draw = ImageDraw.Draw(base)
    gradient_colors = ["#162b4a", "#0f1d33", "#091226"]
    for index, color in enumerate(gradient_colors):
        draw.rectangle([(0, height * index / len(gradient_colors)), (width, height)], fill=color)

    title = "Generated Insight"
    subtitle = f"Prompt captured @ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    body = PROMPT

    font = ImageFont.load_default()
    draw.text((40, 40), title, font=font, fill="#f4f7ff")
    draw.text((40, 70), subtitle, font=font, fill="#94a3c4")

    text_y = 120
    for line in body.splitlines():
        draw.text((40, text_y), line, font=font, fill="#d1d9ff")
        text_y += 18

    draw.text((40, height - 40), "Customize this script in code view for richer charts.", font=font, fill="#94a3c4")
    base.save(OUTPUT_PATH, format="PNG")


if __name__ == "__main__":
    render()
"""


def write_space_script(space: Dict[str, Any], prompt_text: Optional[str] = None) -> Path:
    script_path = space_script_path(space)
    script_path.parent.mkdir(parents=True, exist_ok=True)
    content = build_script_content(space, prompt_text)
    script_path.write_text(content, encoding="utf-8")
    return script_path


def run_space_script(space: Dict[str, Any]) -> Tuple[bool, str]:
    script_path = space_script_path(space)
    if not script_path.exists():
        space["last_run_output"] = "Script file missing"
        return False, space["last_run_output"]
 
    def _normalize_output(*chunks: Optional[str]) -> str:
        text = "\n".join(part.strip() for part in chunks if part and part.strip())
        cleaned = strip_ansi(text)
        return cleaned.strip() or "Script completed with no console output."
 
    try:
        result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True, check=True)
        message = _normalize_output(result.stdout, result.stderr)
        outcome = True
    except subprocess.CalledProcessError as exc:
        message = _normalize_output(exc.stdout, exc.stderr, str(exc))
        outcome = False
 
    space["updated_at"] = datetime.utcnow().isoformat()
    space["last_run_output"] = message
    migrate_legacy_space_image(space)
    return outcome, message


def capture_script_content(path: Optional[Path]) -> Tuple[Optional[str], bool]:
    if not path:
        return None, False
    target = path
    if not target.is_absolute():
        target = (BASE_DIR / target).resolve()
    if not target.exists():
        return None, False
    try:
        content = target.read_text(encoding="utf-8")
    except OSError:
        return None, False
    cleaned = content.strip()
    if not cleaned:
        return None, False
    if len(cleaned) <= MAX_EXISTING_SCRIPT_CONTEXT_CHARS:
        return cleaned, False
    return cleaned[:MAX_EXISTING_SCRIPT_CONTEXT_CHARS], True


def build_copilot_prompt(space: Dict[str, Any], prompt_text: str, connector: Dict[str, Any], baseline_path: Optional[Path] = None) -> str:
    image_path = space_image_path(space)
    script_path = space_script_path(space)
    baseline_path = baseline_path or script_path
    baseline_display = str(baseline_path)
    existing_script, script_truncated = capture_script_content(baseline_path)
    site = connector.get("siteUrl") or "<jira-site-url>"
    project_key = connector.get("projectKey") or "<project-key>"
    email = connector.get("accountEmail") or "<account-email>"
    api_key_note = connector.get("apiKey") or "<jira-api-token>"
    reference_script = SCRIPTS_DIR / "sample_jira.py"
    reference_line = ""
    if reference_script.exists():
        try:
            reference_display = reference_script.relative_to(BASE_DIR)
        except ValueError:
            reference_display = reference_script
        reference_line = (
            f"Reference script: {reference_display} (under scripts/) shows the expected Jira + Pillow pipeline. "
            "Review it before updating this space."
        )
    recent_runtime = (space.get("last_run_output") or space.get("last_log") or "").strip()
    if recent_runtime:
        max_chars = 2000
        if len(recent_runtime) > max_chars:
            recent_runtime = recent_runtime[-max_chars:]

    metadata = textwrap.dedent(
        f"""
        You are assisting with an internal data-visualization pipeline. Generate a single Python 3 script that, when executed, fetches data from the Jira Cloud REST API using the provided credentials and produces a PNG visualization saved to the exact path {image_path}. The script itself lives at {script_path} and will be executed with `python {script_path.name}`.

        Jira connection details:
        - Site URL: {site}
        - Project key or board identifier: {project_key}
        - Account email/username: {email}
        - API token : {api_key_note}

        Quality bar:
        - Use the `requests` library to query Jira (authenticate with email + API token using HTTP basic auth).
        - Use Pillow (PIL) to build the PNG visualization. The PNG must be written to {image_path} via a callable `render()` that runs under `if __name__ == "__main__":`.
        - The previous renderer currently lives at {baseline_display}. Review it fully, reuse its working helpers, and then write your updated code to {script_path} (do not discard the file unless explicitly instructed).
        - Only write the PNG after a successful data fetch + render; on any error or missing data, raise an informative exception and **do not** write or keep a placeholder PNG. This lets the orchestration layer retry with your console output.
        - After drafting the script, actually execute `python {script_path.name}` end-to-end in the same workspace environment and verify that it completes without errors and that {image_path} now contains a valid PNG.
        - If the run fails, raises an exception, or the PNG is missing or empty, diagnose the issue, revise the script, and repeat execution until the PNG is produced successfully before returning your answer.
        - Write the updated renderer directly to {script_path} and keep your response empty or at most a terse confirmation (never echo the script content).
        """
    ).strip()

    if reference_line:
        metadata += f"\n\n{reference_line}"

    if recent_runtime:
        metadata += f"\n\nLatest renderer console output (most recent run):\n{recent_runtime}"

    if existing_script:
        snippet_label = f"Renderer at {baseline_display}"
        if script_truncated:
            snippet_label += f" (truncated to first {MAX_EXISTING_SCRIPT_CONTEXT_CHARS} characters)"
        metadata += f"\n\n{snippet_label}:\n```python\n{existing_script}\n```"
    else:
        metadata += f"\n\nExisting renderer: <none found at {baseline_display} — build this renderer from scratch.>"

    user_request = textwrap.dedent(
        f"""
        User prompt describing the desired visualization:
        {prompt_text.strip()}

        Tasks:
        1. Update the renderer at {script_path} according to the prompt (edit the file in place and reuse any useful helpers).
        2. Execute `python {script_path.name}` to ensure {image_path} is produced successfully.
        3. Prefer to stay silent after completion; if output is unavoidable, respond with a single terse confirmation and never print the script code.
        """
    ).strip()

    return f"{metadata}\n\n{user_request}\n"


def extract_code_from_output(text: str) -> str:
    fence = re.search(r"```(?:python)?\s*([\s\S]+?)```", text)
    if fence:
        return fence.group(1).strip()
    return ""


def generate_space_with_copilot(
    space: Dict[str, Any],
    prompt_text: str,
    connector: Dict[str, Any],
    model: str,
    baseline_path: Optional[Path] = None,
) -> Tuple[bool, str]:
    if not prompt_text:
        return False, "Prompt text is required"
    prompt_body = build_copilot_prompt(space, prompt_text, connector, baseline_path=baseline_path)
    script_path = space_script_path(space)
    script_path.parent.mkdir(parents=True, exist_ok=True)
    before_signature = file_signature(script_path)
    cmd = ["copilot", "--model", model, "--no-color", "-p", prompt_body, "--allow-all-tools"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        stdout = strip_ansi((result.stdout or "").strip())
        stderr = strip_ansi((result.stderr or "").strip())
        log = "\n".join(filter(None, [stdout, stderr])).strip()
    except FileNotFoundError:
        return False, "Copilot CLI is not installed on the server."
    except subprocess.CalledProcessError as exc:
        stdout = strip_ansi((exc.stdout or "").strip())
        stderr = strip_ansi((exc.stderr or "").strip())
        log = "\n".join(filter(None, [stdout, stderr, str(exc)])).strip()
        return False, strip_ansi(log) or "Copilot command failed."

    log = strip_ansi(log)
    code = extract_code_from_output(stdout or log)
    if code:
        script_path.write_text(code, encoding="utf-8")
        return True, log or "Script generated via Copilot."
 
    after_signature = file_signature(script_path)
    if after_signature != before_signature:
        return True, log or "Copilot updated the script in place."
 
    return False, log or "Copilot did not return code or update the script file."



def find_space(nodes: List[Dict[str, Any]], space_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    for node in nodes:
        if node["type"] == "tab":
            for space in node.get("spaces", []):
                if space.get("id") == space_id:
                    return space, node
        if node["type"] == "folder":
            found_space, parent_tab = find_space(node.get("children", []), space_id)
            if found_space:
                return found_space, parent_tab
    return None, None


def sync_space_artifacts(
    space: Dict[str, Any],
    prompt_text: Optional[str] = None,
    connector: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
) -> Tuple[bool, str]:
    log_messages: List[str] = []
    used_model = model if model in ALLOWED_COPILOT_MODELS else DEFAULT_COPILOT_MODEL
    wants_new_version = bool(prompt_text and connector is not None)
    draft: Optional[Dict[str, Any]] = None
    previous_state: Optional[Dict[str, Any]] = None

    baseline_script_path: Optional[Path] = None

    try:
        if wants_new_version:
            baseline_script_path = space_script_path(space)
            prune_versions_after_active(space)
            draft, previous_state = begin_version_draft(space, prompt_text)
            success, copilot_log = generate_space_with_copilot(
                space,
                prompt_text,
                connector,
                used_model,
                baseline_path=baseline_script_path,
            )
            log_messages.append(copilot_log)
            if not success:
                rollback_version_draft(space, previous_state, draft)
                space["last_log"] = "\n".join(filter(None, log_messages)).strip()
                return False, space["last_log"]
        else:
            ensure_space_version_tracking(space, space_tab_slug(space))
            write_space_script(space, prompt_text)
            log_messages.append("Generated default renderer.")

        previous_signature = space_image_signature(space)
        success, runtime_log = run_space_script(space)
        log_messages.append(runtime_log)
        png_ready = success and png_rendered_since(space, previous_signature)
        combined_log = "\n".join(filter(None, log_messages)).strip()
        space["last_log"] = combined_log

        if wants_new_version:
            if success and png_ready:
                commit_version_entry(space, draft)
            else:
                rollback_version_draft(space, previous_state, draft)
                failure_message = runtime_log or "Renderer failed to produce PNG output."
                return False, combined_log or failure_message

        final_success = success if not wants_new_version else success and png_ready
        return final_success, combined_log
    except Exception:
        if wants_new_version:
            rollback_version_draft(space, previous_state, draft)
        raise


def create_space_record(tab_node: Dict[str, Any], title: Optional[str] = None) -> Dict[str, Any]:
    next_x, next_y = next_space_position(tab_node)
    space = {
        "id": uuid.uuid4().hex,
        "title": (title or f"Space {len(tab_node.get('spaces', [])) + 1}").strip() or "Insight Space",
        "python_path": "",
        "image_path": "",
        "last_prompt": None,
        "height": DEFAULT_SPACE_HEIGHT,
        "width": DEFAULT_SPACE_WIDTH,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "x": next_x,
        "y": next_y,
    }
    ensure_space_defaults(space)
    allocate_space_files(space, tab_node["slug"])
    return space


def build_copy_title(tab_node: Dict[str, Any], source_title: Optional[str]) -> str:
    base_title = (source_title or "Insight Space").strip() or "Insight Space"
    existing_titles = {((space.get("title") or "").strip()) for space in tab_node.get("spaces", [])}
    candidate = f"{base_title} (Copy)"
    suffix = 2
    while candidate in existing_titles:
        candidate = f"{base_title} (Copy {suffix})"
        suffix += 1
    return candidate


def build_copy_tab_name(source_name: Optional[str], siblings: List[Dict[str, Any]]) -> str:
    base_name = (source_name or "Workspace Tab").strip() or "Workspace Tab"
    existing_names = {
        (sibling.get("name") or "").strip() for sibling in siblings if sibling.get("type") == "tab"
    }
    candidate = f"{base_name} (Copy)"
    suffix = 2
    while candidate in existing_names:
        candidate = f"{base_name} (Copy {suffix})"
        suffix += 1
    return candidate


def duplicate_space_record(space: Dict[str, Any], tab_node: Dict[str, Any], preserve_layout: bool = False) -> Dict[str, Any]:
    new_space = deepcopy(space)
    new_space.pop("image_url", None)
    new_space["id"] = uuid.uuid4().hex
    new_space["title"] = build_copy_title(tab_node, space.get("title"))
    new_space["python_path"] = ""
    new_space["image_path"] = ""
    new_space["created_at"] = datetime.utcnow().isoformat()
    new_space["updated_at"] = datetime.utcnow().isoformat()

    if preserve_layout:
        try:
            new_space["x"] = max(0, int(space.get("x", SPACE_CANVAS_PADDING)))
        except (TypeError, ValueError):
            new_space["x"] = SPACE_CANVAS_PADDING
        try:
            new_space["y"] = max(0, int(space.get("y", SPACE_CANVAS_PADDING)))
        except (TypeError, ValueError):
            new_space["y"] = SPACE_CANVAS_PADDING
    else:
        offset = SPACE_CANVAS_PADDING
        try:
            new_space["x"] = max(0, int(space.get("x", SPACE_CANVAS_PADDING)) + offset)
        except (TypeError, ValueError):
            new_space["x"] = SPACE_CANVAS_PADDING
        try:
            new_space["y"] = max(0, int(space.get("y", SPACE_CANVAS_PADDING)) + offset)
        except (TypeError, ValueError):
            new_space["y"] = SPACE_CANVAS_PADDING

    ensure_space_defaults(new_space)
    new_space["versions"] = []
    new_space["active_version_id"] = None
    new_space["last_prompt"] = space.get("last_prompt")
    new_space["last_log"] = space.get("last_log", "")
    new_space["last_run_output"] = space.get("last_run_output", "")
    if "aspect_ratio" in space:
        new_space["aspect_ratio"] = space.get("aspect_ratio")

    allocate_space_files(new_space, tab_node["slug"])
    draft, previous_state = begin_version_draft(new_space, new_space.get("last_prompt"))
    try:
        original_script = space_script_path(space)
        new_script = space_script_path(new_space)
        if original_script.exists():
            new_script.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original_script, new_script)
        else:
            fallback_prompt = new_space.get("last_prompt") or "Copied space"
            new_script.write_text(build_script_content(new_space, fallback_prompt), encoding="utf-8")

        original_image = space_image_path(space)
        new_image = space_image_path(new_space)
        if original_image.exists():
            new_image.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original_image, new_image)
        elif new_image.exists():
            new_image.unlink()
    except Exception:
        rollback_version_draft(new_space, previous_state, draft)
        raise

    commit_version_entry(new_space, draft)
    return new_space


def serialize_space(space: Dict[str, Any]) -> Dict[str, Any]:
    migrate_legacy_space_image(space)
    payload = deepcopy(space)
    payload.setdefault("x", SPACE_CANVAS_PADDING)
    payload.setdefault("y", SPACE_CANVAS_PADDING)
    payload["image_url"] = f"/static/{space['image_path']}"
    payload["last_log"] = space.get("last_log", "")
    payload["last_prompt"] = space.get("last_prompt")
    payload["last_run_output"] = space.get("last_run_output", "")
    versions_payload: List[Dict[str, Any]] = []
    for entry in space.get("versions", []) or []:
        versions_payload.append(
            {
                "id": entry.get("id"),
                "label": entry.get("label"),
                "createdAt": entry.get("created_at"),
                "prompt": entry.get("prompt"),
                "isActive": entry.get("id") == space.get("active_version_id"),
            }
        )
    payload["versions"] = versions_payload
    payload["active_version_id"] = space.get("active_version_id")
    return payload


def _load_workspace_payload() -> Dict[str, Any]:
    ensure_workspace_file()
    try:
        raw_text = WORKSPACE_FILE.read_text(encoding="utf-8")
    except OSError:
        raw_text = ""
    if not raw_text.strip():
        default_payload = {"nodes": []}
        WORKSPACE_FILE.write_text(json.dumps(default_payload, indent=2), encoding="utf-8")
        return default_payload
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive path
        app.logger.warning("Workspace file is corrupt: %s", exc)
        backup_payload = _load_workspace_backup()
        if backup_payload is not None:
            app.logger.warning("Workspace restored from backup copy")
            WORKSPACE_FILE.write_text(json.dumps(backup_payload, indent=2), encoding="utf-8")
            return backup_payload
        corrupt_copy = WORKSPACE_FILE.with_suffix(
            f".corrupt-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        )
        try:
            shutil.copy2(WORKSPACE_FILE, corrupt_copy)
        except OSError:
            pass
        app.logger.error("Workspace backup missing or invalid; resetting workspace")
        default_payload = {"nodes": []}
        WORKSPACE_FILE.write_text(json.dumps(default_payload, indent=2), encoding="utf-8")
        return default_payload


def _load_workspace_backup() -> Optional[Dict[str, Any]]:
    if not WORKSPACE_BACKUP_FILE.exists():
        return None
    try:
        raw_text = WORKSPACE_BACKUP_FILE.read_text(encoding="utf-8")
    except OSError:
        return None
    if not raw_text.strip():
        return {"nodes": []}
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        return None


def load_workspace() -> Dict[str, Any]:
    data = _load_workspace_payload()
    if hydrate_nodes(data.get("nodes", [])):
        save_workspace(data)
    return data


def strip_runtime_fields(nodes: List[Dict[str, Any]]) -> None:
    for node in nodes:
        node.pop("fs_path", None)
        node.pop("parent_id", None)
        if node["type"] == "folder":
            strip_runtime_fields(node.get("children", []))


def save_workspace(workspace: Dict[str, Any]) -> None:
    to_store = deepcopy(workspace)
    strip_runtime_fields(to_store.get("nodes", []))
    serialized = json.dumps(to_store, indent=2)
    workspace_dir = WORKSPACE_FILE.parent
    workspace_dir.mkdir(parents=True, exist_ok=True)
    if WORKSPACE_FILE.exists():
        try:
            shutil.copy2(WORKSPACE_FILE, WORKSPACE_BACKUP_FILE)
        except OSError as exc:  # pragma: no cover - best effort logging
            app.logger.warning("Unable to update workspace backup: %s", exc)
    tmp_fd = None
    tmp_path: Optional[Path] = None
    try:
        tmp_fd, tmp_name = tempfile.mkstemp(
            dir=str(workspace_dir),
            prefix=f"{WORKSPACE_FILE.stem}-",
            suffix=".tmp",
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmp_file:
            tmp_file.write(serialized)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        tmp_fd = None
        os.replace(tmp_path, WORKSPACE_FILE)
        tmp_path = None
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except OSError:
                pass
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def generate_copilot_stream(
    workspace: Dict[str, Any],
    space: Dict[str, Any],
    prompt_text: str,
    connector: Dict[str, Any],
    model: str,
    max_iterations: Optional[int] = None,
):
    space["last_prompt"] = prompt_text
    iteration_limit = resolve_iteration_limit(connector, max_iterations)
    attempt_logs: List[str] = []
    baseline_script_path = space_script_path(space)
    try:
        prune_versions_after_active(space)
        draft, previous_state = begin_version_draft(space, prompt_text)
    except Exception as exc:  # noqa: BLE001
        yield {"type": "error", "message": str(exc)}
        return

    script_path = space_script_path(space)
    version_committed = False

    try:
        for attempt in range(1, iteration_limit + 1):
            try:
                if script_path.exists():
                    script_path.unlink()
            except OSError:
                pass

            prompt_body = build_copilot_prompt(space, prompt_text, connector, baseline_path=baseline_script_path)
            cmd = ["copilot", "--model", model, "--no-color", "-p", prompt_body, "--allow-all-tools"]
            yield {
                "type": "status",
                "message": f"Launching Copilot (attempt {attempt}/{iteration_limit})",
            }
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
            except FileNotFoundError:
                message = "Copilot CLI is not installed on the server."
                attempt_logs.append(message)
                space["last_log"] = "\n\n".join(filter(None, attempt_logs)).strip() or message
                rollback_version_draft(space, previous_state, draft)
                draft = None
                save_workspace(workspace)
                yield {"type": "error", "message": message}
                return

            combined_lines: List[str] = []
            try:
                assert process.stdout is not None
                for raw_line in iter(process.stdout.readline, ""):
                    clean_line = strip_ansi(raw_line.rstrip("\n"))
                    if clean_line:
                        combined_lines.append(clean_line)
                        yield {"type": "log", "message": clean_line}
                process.wait()
            finally:
                if process.stdout:
                    process.stdout.close()

            if process.returncode != 0:
                message = f"Copilot exited with status {process.returncode}"
                attempt_logs.append(message)
                space["last_log"] = "\n\n".join(filter(None, attempt_logs)).strip() or message
                rollback_version_draft(space, previous_state, draft)
                draft = None
                save_workspace(workspace)
                yield {"type": "error", "message": message}
                return

            log_text = "\n".join(combined_lines).strip()
            sanitized_log = strip_ansi(log_text)
            attempt_logs.append(
                f"[Attempt {attempt}] Copilot log:\n{sanitized_log}"
                if sanitized_log
                else f"[Attempt {attempt}] Copilot log: <empty>"
            )

            code_captured = False
            if script_path.exists():
                code_captured = True
            else:
                code = extract_code_from_output(sanitized_log)
                if code:
                    script_path.parent.mkdir(parents=True, exist_ok=True)
                    script_path.write_text(code, encoding="utf-8")
                    code_captured = True

            if not code_captured:
                message = "Copilot did not return Python code."
                attempt_logs.append(message)
                if attempt < iteration_limit:
                    remaining = iteration_limit - attempt
                    yield {
                        "type": "status",
                        "message": f"{message} Retrying ({remaining} attempts left).",
                    }
                    continue
                space["last_log"] = "\n\n".join(filter(None, attempt_logs)).strip() or message
                rollback_version_draft(space, previous_state, draft)
                draft = None
                save_workspace(workspace)
                yield {"type": "error", "message": message}
                return

            yield {"type": "status", "message": "Executing renderer"}
            previous_signature = space_image_signature(space)
            runtime_success, runtime_log = run_space_script(space)
            attempt_logs.append(f"[Attempt {attempt}] Runtime:\n{runtime_log}")
            if runtime_log:
                yield {"type": "log", "message": runtime_log}
            png_ready = runtime_success and png_rendered_since(space, previous_signature)

            if png_ready:
                final_log = "\n\n".join(filter(None, attempt_logs)).strip()
                space["last_log"] = final_log or runtime_log or sanitized_log or "Renderer completed."
                commit_version_entry(space, draft)
                version_committed = True
                draft = None
                save_workspace(workspace)
                yield {"type": "status", "message": f"PNG detected after attempt {attempt}."}
                yield {"type": "complete", "log": space["last_log"], "space": serialize_space(space)}
                return

            attempt_logs.append(f"[Attempt {attempt}] PNG not detected.")
            if attempt < iteration_limit:
                yield {
                    "type": "status",
                    "message": f"No PNG detected after attempt {attempt}; retrying "
                    f"({iteration_limit - attempt} remaining).",
                }
                continue

            failure_message = "Copilot attempts exhausted without PNG output."
            final_log = "\n\n".join(filter(None, attempt_logs)).strip()
            space["last_log"] = final_log or failure_message
            rollback_version_draft(space, previous_state, draft)
            draft = None
            save_workspace(workspace)
            yield {"type": "error", "message": failure_message}
            return
    finally:
        if draft is not None and not version_committed:
            rollback_version_draft(space, previous_state, draft)


def find_node(nodes: List[Dict[str, Any]], node_id: str, parent: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    for node in nodes:
        if node["id"] == node_id:
            return node, parent
        if node["type"] == "folder":
            found, owner = find_node(node.get("children", []), node_id, node)
            if found:
                return found, owner
    return None, None


def remove_node(nodes: List[Dict[str, Any]], node_id: str) -> Optional[Dict[str, Any]]:
    for index, node in enumerate(nodes):
        if node["id"] == node_id:
            return nodes.pop(index)
        if node["type"] == "folder":
            removed = remove_node(node.get("children", []), node_id)
            if removed:
                return removed
    return None


def ensure_unique_slug(siblings: List[Dict[str, Any]], base_slug: str) -> str:
    slug = base_slug
    existing = {item.get("slug") for item in siblings}
    counter = 2
    while slug in existing:
        slug = f"{base_slug}-{counter}"
        counter += 1
    return slug


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/tree")
def get_tree():
    workspace = load_workspace()
    return jsonify(workspace)


@app.get("/api/nodes/<node_id>")
def get_node(node_id: str):
    workspace = load_workspace()
    node, _ = find_node(workspace.get("nodes", []), node_id)
    if not node:
        return jsonify({"error": "Node not found"}), 404
    return jsonify(node)


@app.post("/api/nodes")
def create_node():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    node_type = payload.get("type")
    parent_id = payload.get("parentId")

    if not name:
        return jsonify({"error": "Name is required"}), 400
    if node_type not in {"folder", "tab"}:
        return jsonify({"error": "Invalid node type"}), 400

    workspace = load_workspace()
    parent_children: List[Dict[str, Any]]
    parent_path: Optional[str]

    if parent_id:
        parent_node, _ = find_node(workspace.get("nodes", []), parent_id)
        if not parent_node:
            return jsonify({"error": "Parent not found"}), 404
        if parent_node["type"] != "folder":
            return jsonify({"error": "Items can only be added inside folders"}), 400
        parent_children = parent_node.setdefault("children", [])
        parent_path = parent_node["fs_path"]
    else:
        parent_children = workspace.setdefault("nodes", [])
        parent_path = None

    slug = ensure_unique_slug(parent_children, slugify(name))
    fs_path = f"{parent_path}/{slug}" if parent_path else slug
    node_id = uuid.uuid4().hex

    if node_type == "folder":
        new_node = {
            "id": node_id,
            "name": name,
            "type": "folder",
            "slug": slug,
            "children": [],
            "parent_id": parent_id,
            "fs_path": fs_path,
        }
        ensure_folder_path(fs_path)
    else:
        new_node = {
            "id": node_id,
            "name": name,
            "type": "tab",
            "slug": slug,
            "content": f"Working notes for {name}",
            "parent_id": parent_id,
            "fs_path": fs_path,
        }

    parent_children.append(new_node)
    save_workspace(workspace)
    return jsonify({"node": new_node}), 201


@app.patch("/api/nodes/<node_id>")
def patch_node(node_id: str):
    payload = request.get_json(silent=True) or {}
    new_name = (payload.get("name") or "").strip()

    if not new_name:
        return jsonify({"error": "Name is required"}), 400

    with WORKSPACE_LOCK:
        workspace = load_workspace()
        node, parent_node = find_node(workspace.get("nodes", []), node_id)
        if not node:
            return jsonify({"error": "Node not found"}), 404

        if node["type"] != "tab":
            return jsonify({"error": "Only tabs can be renamed"}), 400

        siblings = parent_node["children"] if parent_node else workspace.get("nodes", [])
        comparison_siblings = [item for item in siblings if item.get("id") != node_id]
        base_slug = slugify(new_name)
        new_slug = ensure_unique_slug(comparison_siblings, base_slug)
        old_slug = node.get("slug") or slugify(node.get("name", ""))
        parent_path = parent_node.get("fs_path") if parent_node else None
        old_fs_path = node.get("fs_path")
        new_fs_path = f"{parent_path}/{new_slug}" if parent_path else new_slug

        if new_slug != old_slug:
            rename_scripts_subdir(old_slug, new_slug)
            rename_data_subdir(old_fs_path, new_fs_path)
            update_space_script_paths(node.get("spaces"), old_slug, new_slug)

        node["name"] = new_name
        node["slug"] = new_slug
        node["fs_path"] = new_fs_path
        save_workspace(workspace)
        return jsonify({"node": node})


@app.post("/api/nodes/<tab_id>/copy")
def copy_tab(tab_id: str):
    with WORKSPACE_LOCK:
        workspace = load_workspace()
        tab_node, parent_node = find_node(workspace.get("nodes", []), tab_id)
        if not tab_node or tab_node.get("type") != "tab":
            return jsonify({"error": "Tab not found"}), 404

        siblings = parent_node["children"] if parent_node else workspace.setdefault("nodes", [])
        new_name = build_copy_tab_name(tab_node.get("name"), siblings)
        new_slug = ensure_unique_slug(siblings, slugify(new_name))
        parent_path = parent_node.get("fs_path") if parent_node else None
        fs_path = f"{parent_path}/{new_slug}" if parent_path else new_slug

        new_tab = {
            "id": uuid.uuid4().hex,
            "name": new_name,
            "type": "tab",
            "slug": new_slug,
            "content": tab_node.get("content") or f"Working notes for {new_name}",
            "parent_id": parent_node["id"] if parent_node else None,
            "fs_path": fs_path,
            "spaces": [],
        }

        for space in tab_node.get("spaces", []):
            ensure_space_defaults(space)
            cloned_space = duplicate_space_record(space, new_tab, preserve_layout=True)
            new_tab["spaces"].append(cloned_space)

        try:
            source_index = next(index for index, node in enumerate(siblings) if node.get("id") == tab_id)
        except StopIteration:
            source_index = len(siblings) - 1
        insert_index = max(source_index + 1, 0)
        siblings.insert(insert_index, new_tab)

        save_workspace(workspace)
        return jsonify({"node": new_tab}), 201


@app.delete("/api/nodes/<node_id>")
def delete_node(node_id: str):
    workspace = load_workspace()
    removed = remove_node(workspace.get("nodes", []), node_id)
    if not removed:
        return jsonify({"error": "Node not found"}), 404

    cleanup_node(removed)
    if removed["type"] == "folder":
        remove_folder_path(removed.get("fs_path", ""))
    elif removed["type"] == "tab":
        remove_tab_script_dir(removed.get("slug"))

    save_workspace(workspace)
    return jsonify({"status": "removed", "node": removed})


@app.get("/api/config")
def read_connector_config():
    connector = load_connector_config()
    return jsonify({"connector": connector})


@app.post("/api/config")
def update_connector_config():
    payload = request.get_json(silent=True) or {}
    connector = save_connector_config(payload)
    return jsonify({"connector": connector})


@app.post("/api/connectors/test")
def test_connector():
    payload = request.get_json(silent=True) or {}
    site_url = normalize_site_url((payload.get("siteUrl") or ""))
    project_key = (payload.get("projectKey") or "").strip()
    account_email = (payload.get("accountEmail") or "").strip()
    api_key = (payload.get("apiKey") or "").strip()

    if not site_url or not account_email or not api_key:
        return jsonify({"error": "Site URL, account email, and API token are required"}), 400

    session = build_jira_session(account_email, api_key)

    try:
        me_resp = session.get(f"{site_url}/rest/api/3/myself", timeout=20)
    except requests.RequestException as exc:
        return jsonify({"error": f"Unable to contact Jira: {exc}"}), 502

    if me_resp.status_code >= 400:
        return jsonify({"error": f"Authentication failed: {describe_jira_error(me_resp)}"}), me_resp.status_code

    profile = me_resp.json() if me_resp.content else {}
    display_name = profile.get("displayName") or profile.get("emailAddress") or account_email

    project_name = None
    issue_metadata: Dict[str, Any] = {}
    if project_key:
        try:
            project_resp = session.get(f"{site_url}/rest/api/3/project/{project_key}", timeout=20)
        except requests.RequestException as exc:
            return jsonify({"error": f"Unable to reach project {project_key}: {exc}"}), 502

        if project_resp.status_code >= 400:
            return jsonify({"error": f"Unable to access project {project_key}: {describe_jira_error(project_resp)}"}), project_resp.status_code

        project_payload = project_resp.json() if project_resp.content else {}
        project_name = project_payload.get("name") or project_key

        try:
            search_resp = session.get(
                f"{site_url}/rest/api/3/search/jql",
                params={
                    "jql": f'project = "{project_key}" ORDER BY updated DESC',
                    "maxResults": 1,
                    "fields": "summary,status",
                },
                timeout=20,
            )
            if search_resp.ok:
                issues = search_resp.json().get("issues", [])
                if issues:
                    issue = issues[0]
                    fields = issue.get("fields", {})
                    issue_metadata = {
                        "key": issue.get("key"),
                        "summary": fields.get("summary"),
                        "status": (fields.get("status") or {}).get("name"),
                    }
        except requests.RequestException:
            pass

    message_parts = [f"Authenticated as {display_name}."]
    if project_name:
        project_line = f"Verified access to project {project_name}."
        if issue_metadata.get("key"):
            project_line += f" Latest issue: {issue_metadata['key']}"
            if issue_metadata.get("summary"):
                project_line += f" — {issue_metadata['summary']}"
            project_line += "."
        message_parts.append(project_line)

    details = {
        "account": display_name,
        "site": site_url,
        "project": project_name,
        "issue": issue_metadata or None,
    }

    return jsonify({"status": "ok", "message": " ".join(message_parts), "details": details})


@app.post("/api/spaces")
def create_space():
    payload = request.get_json(silent=True) or {}
    tab_id = payload.get("tabId")
    title = (payload.get("title") or "").strip()
    prompt_seed = (payload.get("prompt") or "Initial placeholder").strip()

    if not tab_id:
        return jsonify({"error": "tabId is required"}), 400

    workspace = load_workspace()
    tab_node, _ = find_node(workspace.get("nodes", []), tab_id)
    if not tab_node or tab_node.get("type") != "tab":
        return jsonify({"error": "Tab not found"}), 404

    space = create_space_record(tab_node, title)
    tab_node.setdefault("spaces", []).append(space)

    space["last_prompt"] = prompt_seed
    success, message = sync_space_artifacts(space, prompt_seed)
    if not success:
        return jsonify({"error": f"Unable to render space: {message}"}), 500

    save_workspace(workspace)
    return jsonify({"space": serialize_space(space)}), 201


@app.post("/api/spaces/<space_id>/copy")
def copy_space(space_id: str):
    with WORKSPACE_LOCK:
        workspace = load_workspace()
        source_space, tab_node = find_space(workspace.get("nodes", []), space_id)
        if not source_space or not tab_node:
            return jsonify({"error": "Space not found"}), 404

        new_space = duplicate_space_record(source_space, tab_node)
        spaces = tab_node.setdefault("spaces", [])
        try:
            source_index = next(index for index, item in enumerate(spaces) if item.get("id") == space_id)
        except StopIteration:
            source_index = len(spaces) - 1
        insert_index = max(source_index + 1, 0)
        spaces.insert(insert_index, new_space)
        save_workspace(workspace)
        return jsonify({"space": serialize_space(new_space), "tabId": tab_node["id"]}), 201


@app.post("/api/spaces/<space_id>/copy-to/<target_tab_id>")
def copy_space_to_tab(space_id: str, target_tab_id: str):
    with WORKSPACE_LOCK:
        workspace = load_workspace()
        source_space, _ = find_space(workspace.get("nodes", []), space_id)
        if not source_space:
            return jsonify({"error": "Space not found"}), 404

        target_tab, _ = find_node(workspace.get("nodes", []), target_tab_id)
        if not target_tab:
            return jsonify({"error": "Target tab not found"}), 404
        if target_tab.get("type") != "tab":
            return jsonify({"error": "Target must be a tab"}), 400

        new_space = duplicate_space_record(source_space, target_tab)
        target_spaces = target_tab.setdefault("spaces", [])
        target_spaces.append(new_space)
        save_workspace(workspace)
        return jsonify({"space": serialize_space(new_space), "tabId": target_tab["id"]}), 201


@app.post("/api/tabs/<tab_id>/spaces/reorder")
def reorder_spaces(tab_id: str):
    payload = request.get_json(silent=True) or {}
    space_id = payload.get("spaceId")
    before_space_id = payload.get("beforeSpaceId") or None

    if not space_id:
        return jsonify({"error": "spaceId is required"}), 400

    workspace = load_workspace()
    tab_node, _ = find_node(workspace.get("nodes", []), tab_id)
    if not tab_node or tab_node.get("type") != "tab":
        return jsonify({"error": "Tab not found"}), 404

    spaces = tab_node.setdefault("spaces", [])
    source_index = next((index for index, item in enumerate(spaces) if item.get("id") == space_id), -1)
    if source_index < 0:
        return jsonify({"error": "Space not found in tab"}), 404

    if before_space_id == space_id:
        return jsonify({"tab": tab_node})

    space = spaces.pop(source_index)

    if before_space_id:
        try:
            target_index = next(index for index, item in enumerate(spaces) if item.get("id") == before_space_id)
        except StopIteration:
            spaces.insert(source_index, space)
            return jsonify({"error": "Target space not found"}), 404
        spaces.insert(target_index, space)
    else:
        spaces.append(space)

    save_workspace(workspace)
    return jsonify({"tab": tab_node})


@app.get("/api/spaces/<space_id>")
def get_space(space_id: str):
    workspace = load_workspace()
    space, tab_node = find_space(workspace.get("nodes", []), space_id)
    if not space:
        return jsonify({"error": "Space not found"}), 404

    script_path = space_script_path(space)
    script_content = script_path.read_text(encoding="utf-8") if script_path.exists() else ""
    return jsonify({"space": serialize_space(space), "script": script_content, "tabId": tab_node["id"] if tab_node else None})


@app.post("/api/spaces/<space_id>/prompt-stream")
def stream_space_prompt(space_id: str):
    payload = request.get_json(silent=True) or {}
    prompt_text = (payload.get("prompt") or "").strip()
    if not prompt_text:
        return jsonify({"error": "Prompt cannot be empty"}), 400

    connector = payload.get("connector") or {}
    if not isinstance(connector, dict):
        connector = {}
    model = payload.get("model") or DEFAULT_COPILOT_MODEL
    if model not in ALLOWED_COPILOT_MODELS:
        model = DEFAULT_COPILOT_MODEL
    iteration_limit = resolve_iteration_limit(connector, payload.get("maxIterations"))

    workspace = load_workspace()
    space, _ = find_space(workspace.get("nodes", []), space_id)
    if not space:
        return jsonify({"error": "Space not found"}), 404

    @stream_with_context
    def event_stream():
        try:
            for event in generate_copilot_stream(
                workspace,
                space,
                prompt_text,
                connector,
                model,
                max_iterations=iteration_limit,
            ):
                yield ndjson(event)
        except Exception as exc:  # noqa: BLE001
            yield ndjson({"type": "error", "message": str(exc)})

    response = Response(event_stream(), mimetype="application/x-ndjson")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@app.post("/api/spaces/<space_id>/prompt")
def update_space_prompt(space_id: str):
    payload = request.get_json(silent=True) or {}
    prompt_text = (payload.get("prompt") or "").strip()
    if not prompt_text:
        return jsonify({"error": "Prompt cannot be empty"}), 400

    connector = payload.get("connector") or {}
    if not isinstance(connector, dict):
        connector = {}
    model = payload.get("model") or DEFAULT_COPILOT_MODEL
    if model not in ALLOWED_COPILOT_MODELS:
        model = DEFAULT_COPILOT_MODEL
    iteration_limit = resolve_iteration_limit(connector, payload.get("maxIterations"))

    workspace = load_workspace()
    space, _ = find_space(workspace.get("nodes", []), space_id)
    if not space:
        return jsonify({"error": "Space not found"}), 404

    final_log = []
    for event in generate_copilot_stream(
        workspace,
        space,
        prompt_text,
        connector,
        model,
        max_iterations=iteration_limit,
    ):
        if event["type"] == "error":
            return jsonify({"error": event.get("message", "Copilot failed")}), 500
        if event["type"] in {"log", "status"}:
            final_log.append(event.get("message", ""))
        if event["type"] == "complete":
            log_text = event.get("log") or "\n".join(filter(None, final_log))
            return jsonify({"space": serialize_space(space), "log": log_text})

    return jsonify({"error": "Copilot did not produce a result"}), 500


@app.post("/api/spaces/<space_id>/update")
def rerun_space(space_id: str):
    workspace = load_workspace()
    space, _ = find_space(workspace.get("nodes", []), space_id)
    if not space:
        return jsonify({"error": "Space not found"}), 404

    previous_signature = space_image_signature(space)
    success, message = run_space_script(space)
    png_generated = success and png_rendered_since(space, previous_signature)
    png_ready = png_available(space)
    space["last_log"] = message

    save_workspace(workspace)
    response_payload = {
        "space": serialize_space(space),
        "log": message,
        "pngReady": png_ready,
        "pngGenerated": png_generated,
    }

    if not success:
        return jsonify({**response_payload, "error": message}), 500

    return jsonify(response_payload)


@app.post("/api/spaces/<space_id>/versions/<version_id>/activate")
def activate_space_version(space_id: str, version_id: str):
    workspace = load_workspace()
    space, _ = find_space(workspace.get("nodes", []), space_id)
    if not space:
        return jsonify({"error": "Space not found"}), 404

    versions = space.get("versions", []) or []
    entry = next((item for item in versions if item.get("id") == version_id), None)
    if not entry:
        return jsonify({"error": "Version not found"}), 404

    if entry.get("python_path"):
        space["python_path"] = entry["python_path"]
    if entry.get("image_path"):
        space["image_path"] = entry["image_path"]
    space["active_version_id"] = entry["id"]
    if entry.get("prompt") is not None:
        space["last_prompt"] = entry.get("prompt")
    space["last_log"] = entry.get("log", space.get("last_log", ""))
    space["last_run_output"] = entry.get("run_output", space.get("last_run_output", ""))
    space["updated_at"] = datetime.utcnow().isoformat()

    save_workspace(workspace)
    return jsonify({"space": serialize_space(space)})


@app.put("/api/spaces/<space_id>/code")
def overwrite_space_code(space_id: str):
    payload = request.get_json(silent=True) or {}
    code = payload.get("code")
    if code is None:
        return jsonify({"error": "Code is required"}), 400

    workspace = load_workspace()
    space, _ = find_space(workspace.get("nodes", []), space_id)
    if not space:
        return jsonify({"error": "Space not found"}), 404

    script_path = space_script_path(space)
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(code, encoding="utf-8")

    success, message = run_space_script(space)
    space["last_log"] = message
    if not success:
        return jsonify({"error": message}), 500

    save_workspace(workspace)
    return jsonify({"space": serialize_space(space), "log": message})


@app.patch("/api/spaces/<space_id>")
def patch_space(space_id: str):
    payload = request.get_json(silent=True) or {}
    with WORKSPACE_LOCK:
        workspace = load_workspace()
        space, _ = find_space(workspace.get("nodes", []), space_id)
        if not space:
            return jsonify({"error": "Space not found"}), 404

        updated = False
        if "height" in payload:
            try:
                height_val = max(MIN_SPACE_HEIGHT, int(payload["height"]))
                space["height"] = height_val
                updated = True
            except (TypeError, ValueError):
                pass

        if "width" in payload:
            try:
                width_val = int(payload["width"])
                width_val = max(MIN_SPACE_WIDTH, min(MAX_SPACE_WIDTH, width_val))
                space["width"] = width_val
                updated = True
            except (TypeError, ValueError):
                pass

        if "x" in payload:
            try:
                x_val = max(0, int(payload["x"]))
                space["x"] = x_val
                updated = True
            except (TypeError, ValueError):
                pass

        if "y" in payload:
            try:
                y_val = max(0, int(payload["y"]))
                space["y"] = y_val
                updated = True
            except (TypeError, ValueError):
                pass

        if not updated:
            return jsonify({"error": "No valid fields supplied"}), 400

        space["updated_at"] = datetime.utcnow().isoformat()
        save_workspace(workspace)
        return jsonify({"space": serialize_space(space)})


@app.delete("/api/spaces/<space_id>")
def delete_space(space_id: str):
    workspace = load_workspace()
    space, tab_node = find_space(workspace.get("nodes", []), space_id)
    if not space or not tab_node:
        return jsonify({"error": "Space not found"}), 404

    delete_space_artifacts(space)
    tab_node["spaces"] = [item for item in tab_node.get("spaces", []) if item.get("id") != space_id]
    save_workspace(workspace)
    return jsonify({"status": "removed", "spaceId": space_id})


if __name__ == "__main__":
    ensure_project_dirs()
    app.run(debug=True, use_reloader=False)
