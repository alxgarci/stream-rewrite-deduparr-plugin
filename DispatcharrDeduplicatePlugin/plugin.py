import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import pytz
from django.db import transaction

from apps.channels.models import ChannelStream, Stream
from apps.m3u.models import M3UAccount

try:
    from core.utils import send_websocket_update
except Exception:  # pragma: no cover
    send_websocket_update = None

LOGGER = logging.getLogger("plugins.stream_rewrite_deduparr")

_bg_thread = None
_stop_event = threading.Event()


class PluginConfig:
    VERSION = "0.1.1"
    DATA_DIR = "/data"
    EXPORTS_DIR = "/data/exports"
    SETTINGS_FILE = "/data/stream_rewrite_deduparr_settings.json"
    LOCK_FILE = "/data/stream_rewrite_deduparr.lock"
    SCHEDULER_CHECK_INTERVAL = 20
    SCHEDULER_TRIGGER_GRACE_SECONDS = 50
    LOCK_TIMEOUT_MINUTES = 240
    DEFAULT_TIMEZONE = "Europe/Madrid"


@dataclass
class StreamRow:
    id: int
    name: str
    m3u_account_id: Optional[int]
    m3u_account_name: str
    url: str


class CronField:
    def __init__(self, expr: str, minimum: int, maximum: int):
        self.expr = (expr or "*").strip()
        self.minimum = minimum
        self.maximum = maximum
        self.values = self._parse(self.expr)

    def _expand_range(self, part: str) -> Iterable[int]:
        step = 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            step = int(step_text)
        else:
            base = part

        if base == "*":
            start, end = self.minimum, self.maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            value = int(base)
            if value < self.minimum or value > self.maximum:
                raise ValueError(f"Cron value {value} out of range {self.minimum}-{self.maximum}")
            return [value]

        if start < self.minimum or end > self.maximum or start > end:
            raise ValueError(f"Cron range {start}-{end} out of range {self.minimum}-{self.maximum}")
        return list(range(start, end + 1, step))

    def _parse(self, expr: str) -> set:
        values = set()
        for part in expr.split(","):
            part = part.strip()
            if not part:
                continue
            for value in self._expand_range(part):
                values.add(value)
        if not values:
            raise ValueError("Empty cron field")
        return values

    def matches(self, value: int) -> bool:
        return value in self.values


class CronSpec:
    def __init__(self, expr: str):
        self.expr = expr.strip()
        parts = [p for p in self.expr.split() if p]
        if len(parts) != 5:
            raise ValueError("Cron must have 5 fields: minute hour day month weekday")
        self.minute = CronField(parts[0], 0, 59)
        self.hour = CronField(parts[1], 0, 23)
        self.day = CronField(parts[2], 1, 31)
        self.month = CronField(parts[3], 1, 12)
        self.weekday = CronField(parts[4], 0, 6)

    def matches(self, dt: datetime) -> bool:
        weekday = (dt.weekday() + 1) % 7  # Python Mon=0 -> Cron Sun=0
        return (
            self.minute.matches(dt.minute)
            and self.hour.matches(dt.hour)
            and self.day.matches(dt.day)
            and self.month.matches(dt.month)
            and self.weekday.matches(weekday)
        )


class Plugin:
    name = "Stream Rewrite & Deduparr"
    version = PluginConfig.VERSION
    description = "Bulk URL replacement for all stored streams plus duplicate cleanup using M3U source priority."
    author = "alxgarci"
    help_url = "https://github.com/alxgarci/stream-rewrite-deduparr-plugin"

    fields = [
        {"id": "mode_info", "type": "info", "label": "Run one or both operations: URL replacement and duplicate cleanup."},
        {"id": "replace_enabled", "label": "Replace text in stream URLs", "type": "boolean", "default": True},
        {"id": "find_text", "label": "Text to find", "type": "string", "default": "", "placeholder": "old.domain.tld"},
        {"id": "replace_text", "label": "Replacement text", "type": "string", "default": "", "placeholder": "new.domain.tld"},
        {"id": "match_mode", "label": "Replacement mode", "type": "select", "default": "literal", "options": [
            {"value": "literal", "label": "Literal text"},
            {"value": "regex", "label": "Regex"},
        ]},
        {"id": "dedupe_enabled", "label": "Remove duplicate stream links", "type": "boolean", "default": True},
        {"id": "priority_m3us", "label": "M3U priority order", "type": "text", "default": "", "placeholder": "Provider A\nProvider B\nProvider C", "help_text": "One M3U source name per line. Earlier entries have higher priority when duplicates are removed."},
        {"id": "restrict_to_m3us", "label": "Only process these M3Us", "type": "text", "default": "", "placeholder": "Provider A\nProvider B", "help_text": "Optional allowlist. Leave empty to process all M3U sources."},
        {"id": "normalize_trailing_slash", "label": "Ignore trailing slash when deduplicating", "type": "boolean", "default": True},
        {"id": "normalize_scheme_case", "label": "Normalize URL scheme/host case", "type": "boolean", "default": True},
        {"id": "dry_run_mode", "label": "Dry run mode", "type": "boolean", "default": True, "help_text": "Preview only. No DB writes are performed."},
        {"id": "export_csv", "label": "Write CSV report", "type": "boolean", "default": True},
        {"id": "timezone", "label": "Timezone", "type": "string", "default": PluginConfig.DEFAULT_TIMEZONE, "placeholder": "Europe/Madrid"},
        {"id": "schedule_cron", "label": "Cron schedules", "type": "text", "default": "", "placeholder": "0 4 * * *\n30 16 * * 0", "help_text": "One 5-field cron expression per line."},
    ]

    actions = [
        {"id": "validate_settings", "label": "Validate settings", "description": "Check model compatibility, M3U names and cron expressions."},
        {"id": "run_selected", "label": "Run selected operations", "description": "Executes replacement and/or duplicate cleanup.", "confirm": {"required": True, "title": "Run plugin?", "message": "This may rewrite URLs and merge/delete duplicate streams."}},
        {"id": "update_schedule", "label": "Update schedule", "description": "Save settings and restart scheduler."},
        {"id": "clear_operation_lock", "label": "Clear operation lock", "description": "Remove stale lock file.", "confirm": {"required": True, "title": "Clear lock?", "message": "Only do this if nothing is currently running."}},
    ]

    def __init__(self):
        self._url_field_name = None
        self.saved_settings = {}

    def stop(self, context=None):
        self._stop_background_scheduler()

    def run(self, action: str, params: dict, context: dict):
        settings = (context or {}).get("settings", {}) or {}
        logger = (context or {}).get("logger") or LOGGER

        if action == "validate_settings":
            return self.validate_settings_action(settings, logger)
        if action == "update_schedule":
            return self.update_schedule_action(settings, logger)
        if action == "clear_operation_lock":
            return self.clear_operation_lock_action(settings, logger)
        if action == "run_selected":
            return self.run_selected_action(settings, logger, context)
        return {"status": "error", "message": f"Unknown action: {action}"}

    # ---------------------- settings / validation ----------------------

    def _split_lines(self, value: str) -> List[str]:
        if not value:
            return []
        return [line.strip() for line in value.replace(",", "\n").splitlines() if line.strip()]

    def _get_url_field_name(self) -> str:
        if self._url_field_name:
            return self._url_field_name
        candidates = ["url", "stream_url", "source_url", "input_url"]
        field_names = {f.name for f in Stream._meta.get_fields() if hasattr(f, "name")}
        for name in candidates:
            if name in field_names:
                self._url_field_name = name
                return name
        raise ValueError(f"No compatible URL field found on Stream model. Available fields: {sorted(field_names)}")

    def _validate_cron_lines(self, value: str) -> List[CronSpec]:
        lines = self._split_lines(value)
        return [CronSpec(line) for line in lines]

    def _validate_m3u_names(self, names: List[str]) -> Tuple[List[str], List[str]]:
        existing = list(M3UAccount.objects.all().values_list("name", flat=True))
        lower_map = {name.lower(): name for name in existing}
        found, missing = [], []
        for name in names:
            if name.lower() in lower_map:
                found.append(lower_map[name.lower()])
            else:
                missing.append(name)
        return found, missing

    def validate_settings_action(self, settings, logger):
        try:
            problems = []
            info = []

            url_field = self._get_url_field_name()
            info.append(f"URL field detected: {url_field}")

            replace_enabled = self._to_bool(settings.get("replace_enabled", True))
            dedupe_enabled = self._to_bool(settings.get("dedupe_enabled", True))
            if not replace_enabled and not dedupe_enabled:
                problems.append("Enable at least one operation: replacement and/or deduplication.")

            if replace_enabled:
                find_text = (settings.get("find_text") or "")
                match_mode = settings.get("match_mode") or "literal"
                if not find_text:
                    problems.append("'Text to find' is required when replacement is enabled.")
                if match_mode == "regex":
                    try:
                        re.compile(find_text)
                    except re.error as exc:
                        problems.append(f"Invalid regex: {exc}")

            priority_names = self._split_lines(settings.get("priority_m3us") or "")
            found_priority, missing_priority = self._validate_m3u_names(priority_names)
            if missing_priority:
                problems.append("Unknown M3U sources in priority list: " + ", ".join(missing_priority))
            else:
                info.append(f"Priority M3Us recognised: {len(found_priority)}")

            restrict_names = self._split_lines(settings.get("restrict_to_m3us") or "")
            found_restrict, missing_restrict = self._validate_m3u_names(restrict_names)
            if missing_restrict:
                problems.append("Unknown M3U sources in restriction list: " + ", ".join(missing_restrict))
            else:
                info.append(f"Restricted M3Us recognised: {len(found_restrict) if found_restrict else 0}")

            tz_name = (settings.get("timezone") or PluginConfig.DEFAULT_TIMEZONE).strip()
            try:
                pytz.timezone(tz_name)
                info.append(f"Timezone OK: {tz_name}")
            except Exception:
                problems.append(f"Unknown timezone: {tz_name}")

            cron_value = settings.get("schedule_cron") or ""
            specs = self._validate_cron_lines(cron_value)
            info.append(f"Cron entries: {len(specs)}")

            if problems:
                return {"status": "error", "message": "Validation failed:\n- " + "\n- ".join(problems)}
            return {"status": "success", "message": "Validation OK:\n- " + "\n- ".join(info)}
        except Exception as exc:
            logger.exception("Validation failed")
            return {"status": "error", "message": f"Validation failed: {exc}"}

    def update_schedule_action(self, settings, logger):
        validation = self.validate_settings_action(settings, logger)
        if validation.get("status") != "success":
            return validation
        self._save_settings(settings)
        self._start_background_scheduler(settings)
        cron_lines = self._split_lines(settings.get("schedule_cron") or "")
        if cron_lines:
            return {"status": "success", "message": "Schedule updated. Active cron entries:\n- " + "\n- ".join(cron_lines)}
        return {"status": "success", "message": "Schedule cleared and scheduler stopped."}

    def _save_settings(self, settings):
        os.makedirs(PluginConfig.DATA_DIR, exist_ok=True)
        with open(PluginConfig.SETTINGS_FILE, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2)
        self.saved_settings = settings

    # ---------------------- scheduler ----------------------

    def _start_background_scheduler(self, settings):
        global _bg_thread
        self._stop_background_scheduler()
        cron_lines = self._split_lines(settings.get("schedule_cron") or "")
        if not cron_lines:
            return
        specs = [CronSpec(line) for line in cron_lines]
        tz_name = (settings.get("timezone") or PluginConfig.DEFAULT_TIMEZONE).strip()
        tz = pytz.timezone(tz_name)
        last_keys = set()

        def loop():
            LOGGER.info("[Stream Rewrite & Deduparr] Scheduler started with %s cron entries in %s", len(specs), tz_name)
            while not _stop_event.is_set():
                now = datetime.now(tz)
                minute_key_base = now.strftime("%Y-%m-%d %H:%M")
                try:
                    for index, spec in enumerate(specs):
                        key = f"{minute_key_base}|{index}"
                        if spec.matches(now) and key not in last_keys:
                            last_keys.add(key)
                            LOGGER.info("[Stream Rewrite & Deduparr] Scheduled run triggered by '%s'", spec.expr)
                            self._execute(settings, LOGGER, scheduled=True, context=None)
                    last_keys_copy = {k for k in last_keys if k.startswith(now.strftime("%Y-%m-%d %H:")) or k.startswith(now.strftime("%Y-%m-%d %H:%M"))}
                    last_keys.clear()
                    last_keys.update(last_keys_copy)
                except Exception:
                    LOGGER.exception("Scheduler loop error")
                _stop_event.wait(PluginConfig.SCHEDULER_CHECK_INTERVAL)

        _bg_thread = threading.Thread(target=loop, name="stream-rewrite-deduparr-scheduler", daemon=True)
        _bg_thread.start()

    def _stop_background_scheduler(self):
        global _bg_thread
        if _bg_thread and _bg_thread.is_alive():
            _stop_event.set()
            _bg_thread.join(timeout=5)
            _stop_event.clear()
        _bg_thread = None

    # ---------------------- lock / progress ----------------------

    def _lock_exists(self) -> Tuple[bool, str]:
        if not os.path.exists(PluginConfig.LOCK_FILE):
            return False, ""
        try:
            with open(PluginConfig.LOCK_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            started = datetime.fromisoformat(data["started_at"])
            age_minutes = (datetime.now() - started).total_seconds() / 60.0
            if age_minutes > PluginConfig.LOCK_TIMEOUT_MINUTES:
                os.remove(PluginConfig.LOCK_FILE)
                return False, ""
            return True, f"{data.get('action', 'operation')} started {age_minutes:.1f} minutes ago"
        except Exception:
            try:
                os.remove(PluginConfig.LOCK_FILE)
            except Exception:
                pass
            return False, ""

    def _acquire_lock(self, action: str):
        os.makedirs(PluginConfig.DATA_DIR, exist_ok=True)
        with open(PluginConfig.LOCK_FILE, "w", encoding="utf-8") as fh:
            json.dump({"action": action, "started_at": datetime.now().isoformat(), "pid": os.getpid()}, fh)

    def _release_lock(self):
        try:
            if os.path.exists(PluginConfig.LOCK_FILE):
                os.remove(PluginConfig.LOCK_FILE)
        except Exception:
            pass

    def clear_operation_lock_action(self, settings, logger):
        if os.path.exists(PluginConfig.LOCK_FILE):
            os.remove(PluginConfig.LOCK_FILE)
            return {"status": "success", "message": "Lock cleared."}
        return {"status": "success", "message": "No lock file found."}

    def _notify(self, level: str, title: str, message: str):
        LOGGER.info("%s: %s", title, message)
        if send_websocket_update:
            try:
                send_websocket_update("updates", "update", {
                    "type": "notification",
                    "level": level,
                    "title": title,
                    "message": message,
                    "plugin": "stream_rewrite_deduparr",
                })
            except Exception:
                LOGGER.debug("Websocket update failed", exc_info=True)

    # ---------------------- action dispatch ----------------------

    def run_selected_action(self, settings, logger, context=None):
        is_locked, detail = self._lock_exists()
        if is_locked:
            return {"status": "error", "message": f"Another operation is already running: {detail}"}

        def background():
            self._acquire_lock("run_selected")
            try:
                result = self._execute(settings, logger, scheduled=False, context=context)
                level = "success" if result.get("status") == "success" else "error"
                self._notify(level, "Stream Rewrite & Deduparr", result.get("message", "Completed"))
            finally:
                self._release_lock()

        threading.Thread(target=background, name="stream-rewrite-deduparr-run", daemon=True).start()
        return {"status": "success", "background": True, "message": "Operation started in background. Check notifications or logs for completion."}

    # ---------------------- core helpers ----------------------

    def _to_bool(self, value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _load_streams(self, settings) -> List[StreamRow]:
        url_field = self._get_url_field_name()
        restrict_names = self._split_lines(settings.get("restrict_to_m3us") or "")
        restrict_found, _ = self._validate_m3u_names(restrict_names)
        qs = Stream.objects.all().select_related("m3u_account")
        if restrict_found:
            qs = qs.filter(m3u_account__name__in=restrict_found)

        rows = []
        for stream in qs:
            m3u = getattr(stream, "m3u_account", None)
            rows.append(StreamRow(
                id=stream.id,
                name=getattr(stream, "name", f"Stream {stream.id}"),
                m3u_account_id=getattr(stream, "m3u_account_id", None),
                m3u_account_name=getattr(m3u, "name", "") if m3u else "",
                url=getattr(stream, url_field, "") or "",
            ))
        return rows

    def _apply_replacement(self, old: str, settings) -> str:
        find_text = settings.get("find_text") or ""
        replace_text = settings.get("replace_text") or ""
        match_mode = settings.get("match_mode") or "literal"
        if not find_text:
            return old
        if match_mode == "regex":
            return re.sub(find_text, replace_text, old)
        return old.replace(find_text, replace_text)

    def _normalized_url(self, url: str, settings) -> str:
        value = (url or "").strip()
        if self._to_bool(settings.get("normalize_trailing_slash", True)):
            value = value.rstrip("/")
        if self._to_bool(settings.get("normalize_scheme_case", True)):
            m = re.match(r"^(https?://)([^/]+)(.*)$", value, flags=re.IGNORECASE)
            if m:
                value = f"{m.group(1).lower()}{m.group(2).lower()}{m.group(3)}"
        return value

    def _priority_map(self, settings) -> Dict[str, int]:
        names = self._split_lines(settings.get("priority_m3us") or "")
        found, _ = self._validate_m3u_names(names)
        return {name.lower(): idx for idx, name in enumerate(found)}

    def _pick_winner(self, candidates: List[StreamRow], priority_map: Dict[str, int]) -> StreamRow:
        def sort_key(row: StreamRow):
            p = priority_map.get((row.m3u_account_name or "").lower(), 10_000)
            return (p, row.id)
        return sorted(candidates, key=sort_key)[0]

    def _csv_path(self, prefix: str) -> str:
        os.makedirs(PluginConfig.EXPORTS_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(PluginConfig.EXPORTS_DIR, f"{prefix}_{ts}.csv")

    def _write_csv(self, path: str, rows: List[Dict[str, object]]):
        import csv
        if not rows:
            rows = [{"status": "empty"}]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def _repoint_channel_streams_safely(self, kept_id: int, loser_ids: List[int], logger) -> Dict[str, int]:
        loser_ids = [int(x) for x in loser_ids if int(x) != int(kept_id)]
        if not loser_ids:
            return {"updated": 0, "deleted": 0}

        loser_links = list(
            ChannelStream.objects
            .filter(stream_id__in=loser_ids)
            .order_by("channel_id", "id")
            .values("id", "channel_id", "stream_id")
        )

        if not loser_links:
            return {"updated": 0, "deleted": 0}

        affected_channel_ids = list({row["channel_id"] for row in loser_links})

        channels_already_with_kept = set(
            ChannelStream.objects
            .filter(channel_id__in=affected_channel_ids, stream_id=kept_id)
            .values_list("channel_id", flat=True)
        )

        ids_to_update = []
        ids_to_delete = []
        updated_channels = set()

        for row in loser_links:
            link_id = row["id"]
            channel_id = row["channel_id"]

            # Si el canal ya tiene el kept, cualquier loser sobra
            if channel_id in channels_already_with_kept:
                ids_to_delete.append(link_id)
                continue

            # Solo una relación por canal puede convertirse al kept
            if channel_id not in updated_channels:
                ids_to_update.append(link_id)
                updated_channels.add(channel_id)
            else:
                ids_to_delete.append(link_id)

        deleted_count = 0
        updated_count = 0

        if ids_to_delete:
            deleted_count, _ = ChannelStream.objects.filter(id__in=ids_to_delete).delete()

        if ids_to_update:
            updated_count = ChannelStream.objects.filter(id__in=ids_to_update).update(stream_id=kept_id)

        logger.info(
            "[Stream Rewrite & Deduparr] kept=%s loser_ids=%s -> updated_links=%s deleted_links=%s",
            kept_id,
            loser_ids,
            updated_count,
            deleted_count,
        )

        return {"updated": updated_count, "deleted": deleted_count}

    def _simulate_channelstream_merge(self, kept_id: int, loser_ids: List[int]) -> Dict[str, int]:
        loser_ids = [int(x) for x in loser_ids if int(x) != int(kept_id)]
        if not loser_ids:
            return {"updated": 0, "deleted": 0}

        loser_links = list(
            ChannelStream.objects
            .filter(stream_id__in=loser_ids)
            .order_by("channel_id", "id")
            .values("id", "channel_id", "stream_id")
        )

        if not loser_links:
            return {"updated": 0, "deleted": 0}

        affected_channel_ids = list({row["channel_id"] for row in loser_links})

        channels_already_with_kept = set(
            ChannelStream.objects
            .filter(channel_id__in=affected_channel_ids, stream_id=kept_id)
            .values_list("channel_id", flat=True)
        )

        updated_count = 0
        deleted_count = 0
        updated_channels = set()

        for row in loser_links:
            channel_id = row["channel_id"]

            if channel_id in channels_already_with_kept:
                deleted_count += 1
                continue

            if channel_id not in updated_channels:
                updated_count += 1
                updated_channels.add(channel_id)
            else:
                deleted_count += 1

        return {"updated": updated_count, "deleted": deleted_count}

    def _execute(self, settings, logger, scheduled=False, context=None):
        validation = self.validate_settings_action(settings, logger)
        if validation.get("status") != "success":
            return validation

        dry_run = self._to_bool(settings.get("dry_run_mode", True))
        replace_enabled = self._to_bool(settings.get("replace_enabled", True))
        dedupe_enabled = self._to_bool(settings.get("dedupe_enabled", True))
        export_csv = self._to_bool(settings.get("export_csv", True))
        url_field = self._get_url_field_name()
        priority_map = self._priority_map(settings)
        streams = self._load_streams(settings)

        replacement_plan: List[Dict[str, object]] = []
        replacement_updates: List[Tuple[int, str, str]] = []
        post_replace_by_id: Dict[int, str] = {}

        if replace_enabled:
            for row in streams:
                new_url = self._apply_replacement(row.url, settings)
                post_replace_by_id[row.id] = new_url
                if new_url != row.url:
                    replacement_updates.append((row.id, row.url, new_url))
                    replacement_plan.append({
                        "stream_id": row.id,
                        "stream_name": row.name,
                        "m3u": row.m3u_account_name,
                        "old_url": row.url,
                        "new_url": new_url,
                    })
                else:
                    post_replace_by_id[row.id] = row.url
        else:
            for row in streams:
                post_replace_by_id[row.id] = row.url

        dedupe_rows: List[Dict[str, object]] = []
        duplicate_groups: Dict[str, List[StreamRow]] = {}

        if dedupe_enabled:
            transformed_rows = []
            for row in streams:
                transformed_rows.append(StreamRow(
                    id=row.id,
                    name=row.name,
                    m3u_account_id=row.m3u_account_id,
                    m3u_account_name=row.m3u_account_name,
                    url=post_replace_by_id[row.id],
                ))

            for row in transformed_rows:
                key = self._normalized_url(row.url, settings)
                if not key:
                    continue
                duplicate_groups.setdefault(key, []).append(row)

            duplicate_groups = {k: v for k, v in duplicate_groups.items() if len(v) > 1}

            for normalized_url, rows in duplicate_groups.items():
                winner = self._pick_winner(rows, priority_map)
                losers = [r for r in rows if r.id != winner.id]
                for loser in losers:
                    dedupe_rows.append({
                        "normalized_url": normalized_url,
                        "kept_stream_id": winner.id,
                        "kept_stream_name": winner.name,
                        "kept_m3u": winner.m3u_account_name,
                        "removed_stream_id": loser.id,
                        "removed_stream_name": loser.name,
                        "removed_m3u": loser.m3u_account_name,
                    })

        report_paths = []
        if export_csv:
            if replacement_plan:
                path = self._csv_path("stream_rewrite_report")
                self._write_csv(path, replacement_plan)
                report_paths.append(path)
            if dedupe_rows:
                path = self._csv_path("stream_dedupe_report")
                self._write_csv(path, dedupe_rows)
                report_paths.append(path)

        dedupe_channelstream_updates = 0
        dedupe_channelstream_deletes = 0

        if dedupe_rows:
            by_kept_preview: Dict[int, List[int]] = {}
            for row in dedupe_rows:
                kept_id = int(row["kept_stream_id"])
                loser_id = int(row["removed_stream_id"])
                by_kept_preview.setdefault(kept_id, []).append(loser_id)

            for kept_id, loser_ids in by_kept_preview.items():
                sim = self._simulate_channelstream_merge(kept_id, loser_ids)
                dedupe_channelstream_updates += sim["updated"]
                dedupe_channelstream_deletes += sim["deleted"]

        if dry_run:
            return {
                "status": "success",
                "message": self._summary_message(
                    dry_run=True,
                    replacement_count=len(replacement_updates),
                    duplicate_count=len(dedupe_rows),
                    report_paths=report_paths,
                    channelstream_updates=dedupe_channelstream_updates,
                    channelstream_deletes=dedupe_channelstream_deletes,
                ),
            }

        with transaction.atomic():
            if replacement_updates:
                for stream_id, _old_url, new_url in replacement_updates:
                    Stream.objects.filter(id=stream_id).update(**{url_field: new_url})

            if dedupe_rows:
                by_kept: Dict[int, List[int]] = {}
                for row in dedupe_rows:
                    kept_id = int(row["kept_stream_id"])
                    loser_id = int(row["removed_stream_id"])
                    by_kept.setdefault(kept_id, []).append(loser_id)

                all_loser_ids = set()

                for kept_id, loser_ids in by_kept.items():
                    unique_loser_ids = sorted({int(x) for x in loser_ids if int(x) != int(kept_id)})
                    all_loser_ids.update(unique_loser_ids)

                    move_result = self._repoint_channel_streams_safely(kept_id, unique_loser_ids, logger)
                    dedupe_channelstream_updates += move_result["updated"]
                    dedupe_channelstream_deletes += move_result["deleted"]

                if all_loser_ids:
                    Stream.objects.filter(id__in=list(all_loser_ids)).delete()

        return {
            "status": "success",
            "message": self._summary_message(
                dry_run=False,
                replacement_count=len(replacement_updates),
                duplicate_count=len(dedupe_rows),
                report_paths=report_paths,
                channelstream_updates=dedupe_channelstream_updates,
                channelstream_deletes=dedupe_channelstream_deletes,
            ),
        }

    def _summary_message(
        self,
        dry_run: bool,
        replacement_count: int,
        duplicate_count: int,
        report_paths: List[str],
        channelstream_updates: int = 0,
        channelstream_deletes: int = 0,
    ) -> str:
        mode = "Dry run complete" if dry_run else "Execution complete"
        parts = [
            mode,
            f"URL replacements: {replacement_count}",
            f"Duplicate removals planned/applied: {duplicate_count}",
            f"ChannelStream reassignments planned/applied: {channelstream_updates}",
            f"ChannelStream duplicates removed planned/applied: {channelstream_deletes}",
        ]
        if report_paths:
            parts.append("Reports:\n- " + "\n- ".join(report_paths))
        return "\n\n".join(parts)