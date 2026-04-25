from __future__ import annotations

from typing import Any

EXPORT_PROFILES = ("raw", "reviewed", "scrubbed", "metadata-only", "exclude")


def resolve_export_profile(
    record_profile: str | None,
    config_default: str | None,
) -> str:
    default = config_default or "raw"
    profile = record_profile or default
    if profile not in EXPORT_PROFILES:
        return default
    return profile


def is_exportable(profile: str) -> bool:
    return profile != "exclude"


def needs_scrub(profile: str) -> bool:
    return profile in ("scrubbed", "reviewed")


def metadata_only(profile: str) -> bool:
    return profile == "metadata-only"
