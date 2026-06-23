"""Map ITSM Generic Application asset custom fields to AAP workflow extra vars."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from bot.itsm_mcp import itsm_call

log = logging.getLogger("itsm-agent-bot")

_APACHE_APP_ASSET_TYPE = "Generic Application"

# ITSM asset custom field key -> AAP extra var name
_ASSET_FIELD_TO_EXTRA_VAR = {
    "rpm_packages": "apache_app_rpm_packages",
    "enabled_services": "apache_app_enabled_services",
    "app_clone_path": "apache_app_docroot",
    "exposed_port": "apache_exposure_service_port",
}


def _split_csv(val: str) -> list[str]:
    return [part.strip() for part in val.split(",") if part.strip()]


def _normalize_repo_name(raw: str) -> str:
    s = raw.strip()
    if "/" in s:
        s = s.rsplit("/", 1)[-1]
    if s.endswith(".git"):
        s = s[:-4]
    return s.strip()


def _apply_legacy_apache_vars(collected: dict[str, str]) -> None:
    """Derive apache_app_package / apache_app_git_package / apache_app_service from list extra vars.

    When apache_app_rpm_packages or apache_app_enabled_services are set (e.g. from an ITSM
    Generic Application asset), they override playbook defaults; legacy single-value vars are
    derived from the comma-separated lists for older playbooks and surveys.
    """
    rpm = str(collected.get("apache_app_rpm_packages") or "").strip()
    if rpm:
        packages = _split_csv(rpm)
        if packages:
            collected["apache_app_package"] = packages[0]
        if "git" in packages:
            collected["apache_app_git_package"] = "git"
        else:
            collected.pop("apache_app_git_package", None)
    services = str(collected.get("apache_app_enabled_services") or "").strip()
    if services:
        enabled = _split_csv(services)
        if enabled:
            collected["apache_app_service"] = enabled[0]


def _asset_matches(collected: dict[str, str], asset: dict[str, Any]) -> bool:
    if str(asset.get("asset_type_name") or "") != _APACHE_APP_ASSET_TYPE:
        return False
    custom_fields = asset.get("custom_fields") or {}
    if not isinstance(custom_fields, dict):
        return False

    vm = (collected.get("vm_name") or collected.get("target_host") or "").strip()
    if vm:
        asset_vm = str(custom_fields.get("vm_hostname") or custom_fields.get("hostname") or "").strip()
        if asset_vm and asset_vm != vm:
            return False

    app_repo = str(collected.get("app_repo") or "").strip()
    if app_repo:
        asset_repo = str(custom_fields.get("app_repo") or "").strip()
        if asset_repo:
            want = _normalize_repo_name(app_repo)
            have = _normalize_repo_name(asset_repo)
            if want != have and want not in have and have not in want:
                return False
    return True


def _pick_asset(collected: dict[str, str], assets: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [row for row in assets if isinstance(row, dict) and _asset_matches(collected, row)]
    if not candidates:
        return None

    vm = (collected.get("vm_name") or collected.get("target_host") or "").strip()
    app_repo = str(collected.get("app_repo") or "").strip()
    if vm and app_repo:
        exact_name = f"{vm}-{_normalize_repo_name(app_repo)}"
        for row in candidates:
            if str(row.get("name") or "") == exact_name:
                return row
    return candidates[0]


def _merge_asset_fields(collected: dict[str, str], asset: dict[str, Any]) -> dict[str, str]:
    out = dict(collected)
    custom_fields = asset.get("custom_fields") or {}
    if not isinstance(custom_fields, dict):
        return out

    for itsm_key, extra_key in _ASSET_FIELD_TO_EXTRA_VAR.items():
        value = custom_fields.get(itsm_key)
        if value is not None and str(value).strip():
            out[extra_key] = str(value).strip()

    _apply_legacy_apache_vars(out)
    return out


def _apply_itsm_field_aliases(collected: dict[str, str]) -> None:
    """Map ITSM catalog/asset field keys to AAP apache_app_* extra vars when not already set."""
    for itsm_key, extra_key in _ASSET_FIELD_TO_EXTRA_VAR.items():
        if str(collected.get(extra_key) or "").strip():
            continue
        value = collected.get(itsm_key)
        if value is not None and str(value).strip():
            collected[extra_key] = str(value).strip()
    _apply_legacy_apache_vars(collected)


async def enrich_collected_from_apache_asset(
    client: httpx.AsyncClient,
    mcp_url_str: str,
    mcp_token: str | None,
    collected: dict[str, str],
) -> dict[str, str]:
    """Add apache_app_* extra vars from ITSM request fields and matching Generic Application assets."""
    _apply_itsm_field_aliases(collected)

    vm = (collected.get("vm_name") or collected.get("target_host") or "").strip()
    app_repo = str(collected.get("app_repo") or "").strip()
    if not vm and not app_repo:
        return collected

    query = vm or _normalize_repo_name(app_repo)
    data = await itsm_call(
        client,
        mcp_url_str,
        mcp_token,
        "list_assets",
        {"query": query, "external_only": True},
    )
    if not isinstance(data, list):
        return collected

    asset = _pick_asset(collected, data)
    if asset is None:
        log.info(
            "No Generic Application asset match for vm=%s app_repo=%s",
            vm or "?",
            app_repo or "?",
        )
        return collected

    merged = _merge_asset_fields(collected, asset)
    log.info("Enriched AAP extra vars from Generic Application asset %s", asset.get("name"))
    return merged
