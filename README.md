# Stream Rewrite & Deduparr

A Dispatcharr plugin that can:
- **bulk rewrite** text inside all stored stream URLs;
- **remove duplicate streams** based on normalized URLs;
- **automatically keep the best stream** according to your configured M3U provider priority;
- run in **dry-run mode** to preview changes without touching the database;
- run manually or through **configurable cron schedules**.

---
(AI and [PiratesIRC projects](https://github.com/PiratesIRC) helped to develop this)

## What this plugin does

This plugin is designed for two common IPTV maintenance tasks inside Dispatcharr:

### 1. Bulk URL rewriting
It lets you search for text inside stream URLs and replace it with something else.

The rewrite applies to **all stored streams**, whether they are assigned to channels or not.

---

### 2. Stream deduplication
It detects streams with the same effective URL and removes duplicates.

When multiple duplicate streams are found:
- one stream is chosen as the **winner**;
- that stream is kept;
- `ChannelStream` references are safely moved from duplicate streams to the winning stream;
- the losing duplicate streams are deleted.

The winner is chosen based on:
1. your configured **M3U provider priority**;
2. if there is no configured priority match, the **lowest stream ID**.

---

## Main features

- Literal or regex URL replacement.
- Duplicate detection using normalized URLs.
- M3U provider priority handling.
- Optional M3U allowlist restriction.
- `dry_run_mode` preview support.
- CSV export of planned changes.
- Cron-based scheduling.
- Background execution.
- Locking to prevent concurrent runs.
- Manual lock clearing if a previous run got stuck.

---

## Overall execution flow

When the plugin runs, it:

1. validates the configuration;
2. automatically detects which field on the `Stream` model contains the URL;
3. loads the streams;
4. if replacement is enabled, computes the new URL for each stream;
5. if deduplication is enabled, groups streams by normalized URL;
6. chooses a winner inside each duplicate group;
7. generates CSV reports if enabled;
8. if `dry_run_mode` is enabled, stops there without changing the database;
9. if `dry_run_mode` is disabled:
   - updates URLs;
   - safely reassigns `ChannelStream` relations;
   - deletes duplicate loser streams.

---

## Automatic URL field detection

The plugin tries to detect the correct URL field on the `Stream` model automatically.

Candidate fields:
- `url`
- `stream_url`
- `source_url`
- `input_url`

The first field that exists will be used.

---

## Configuration options

## `replace_enabled`
**Type:** boolean  
**Default:** `True`

Enables or disables the URL text replacement operation.

If disabled:
- no stream URLs are modified;
- only deduplication will run, if enabled.

---

## `find_text`
**Type:** string  
**Default:** `""`

Text or pattern to search for inside stream URLs.

Typical uses:
- old domain;
- part of a path;
- IP address;
- port;
- regex pattern.

If `replace_enabled` is enabled, this field is required.

Examples:
- `old.domain.tld`
- `:8080`
- `/live/`
- `http://192\.168\.1\.10:\d+`

---

## `replace_text`
**Type:** string  
**Default:** `""`

Text that will replace `find_text`.

Examples:
- `new.domain.tld`
- `:80`
- `/stream/`

If regex mode is enabled, this value is passed as the replacement argument to `re.sub()`.

---

## `match_mode`
**Type:** select  
**Default:** `literal`

Options:
- `literal`
- `regex`

Defines how `find_text` is interpreted.

### `literal`
Uses direct replacement via `str.replace()`.

Example:
- search for `old.domain.tld`
- replace with `new.domain.tld`

### `regex`
Uses `re.sub()`.

Example:
- search for `http://192\.168\.1\.\d+:8080`
- replace with `https://stream.example.com`

---

## `dedupe_enabled`
**Type:** boolean  
**Default:** `True`

Enables or disables stream deduplication.

If disabled:
- no duplicate stream detection is performed;
- only URL replacement will run, if enabled.

---

## `priority_m3us`
**Type:** text  
**Default:** `""`

A list of M3U provider names, one per line, in priority order.

The first provider has the highest priority.

Example:
```text
Premium Provider
Backup Provider
Free Provider