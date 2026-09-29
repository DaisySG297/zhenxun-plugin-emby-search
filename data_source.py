"""Emby API 数据源"""
import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from zhenxun.services.log import logger

from .config import config


def _make_client(proxy: bool = False) -> httpx.AsyncClient:
    """构造 httpx 异步客户端。proxy=False 时不走代理（国内可直连 Emby 本地）。

    2026-09-30 修复: httpx 0.28+ 已移除 proxies 参数（本机 0.28.1），
    原写法 proxies=False 在 AsyncClient 构造时直接 TypeError，
    导致所有 Emby API 请求静默失败（表现为"没有新增"）。
    不传 proxy 即默认不走代理，行为等价。
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=10.0, read=30.0),
        verify=False,
    )


async def _api_get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    """通用 GET 请求，失败返回 None 并打印日志。"""
    url = f"{config.base_url}{path}"
    headers = {"X-Emby-Token": config.api_token}
    try:
        async with _make_client() as client:
            resp = await client.get(url, headers=headers, params=params or {})
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning(f"[emby_search] API 请求失败 {path}: {e}")
        return None


# ─────────────────────────────────────────────
# 1. 搜库
# ─────────────────────────────────────────────

# 常见番剧缩写别名（小写归一后匹配）→ 实际搜索词
# Emby SearchTerm 是子串匹配，"RE0" 之类缩写无法命中正式译名，靠这张表转换
_ANIME_ALIASES = {
    "re0": "Re：从零",
    "re:0": "Re：从零",
    "rezero": "Re：从零",
    "re:zero": "Re：从零",
    "re：zero": "Re：从零",
    "rezero S2": "Re：从零",
}


def _resolve_alias(keyword: str) -> str:
    """把常见缩写归一后映射为 Emby 里能命中的搜索词。"""
    normalized = re.sub(r"[\s:：]+", "", keyword).lower()
    for alias, target in _ANIME_ALIASES.items():
        if re.sub(r"[\s:：]+", "", alias).lower() == normalized:
            return target
    return keyword


async def search_series(keyword: str, lib_id: Optional[str] = None, limit: int = 8) -> list[dict]:
    """
    在 Emby 媒体库中搜索番剧。

    Args:
        keyword: 搜索关键词
        lib_id:  指定库 ID（None = 全库搜索）
        limit:   最多返回条数

    Returns:
        list[dict]，每条包含 id / name / name_cn / overview / image_url / year / rating / episodes_count
    """
    keyword = _resolve_alias(keyword)
    # Emby 的 SearchTerm 对中文支持较好，直接用关键词搜索
    # 注意：必须 Recursive=true 且走 /Users/{user_id}/Items，
    # 否则 Emby 会忽略 IncludeItemTypes 返回媒体库目录（CollectionFolder）
    params = {
        "SearchTerm": keyword,
        "Limit": limit * 2,  # 多拉一些再过滤
        "Recursive": "true",
        "IncludeItemTypes": "Series",
        "UserId": config.user_id,
        "Fields": (
            "Overview,PremiereDate,CommunityRating,ParentId,ProductionYear,"
            "DateCreated,ChildCount,RecursiveItemCount"
        ),
    }
    if lib_id:
        params["ParentId"] = lib_id

    data = await _api_get(f"/Users/{config.user_id}/Items", params)
    if not data:
        return []

    results = []
    for item in data.get("Items", [])[:limit]:
        # 过滤掉 R18 库（除非用户主动指定）
        pid = item.get("ParentId", "")
        if lib_id is None and pid == config.r18_lib_id:
            continue  # 全局搜索默认跳过里番库

        img_tag = item.get("ImageTags", {}).get("Primary", "")
        img_url = ""
        if img_tag:
            img_url = f"{config.base_url}/Items/{item['Id']}/Images/Primary?tag={img_tag}&api_key={config.api_token}"

        created_dt = _parse_dt(item.get("DateCreated"))
        results.append(
            {
                "id": item["Id"],
                "name": item.get("Name", ""),
                "name_cn": item.get("Name", ""),
                "overview": item.get("Overview", ""),
                "year": item.get("ProductionYear"),
                "rating": item.get("CommunityRating"),
                "image_url": img_url,
                "parent_id": pid,
                "season_count": item.get("ChildCount") or 0,
                "episode_count": item.get("RecursiveItemCount") or 0,
                "added_time": created_dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                if created_dt
                else "",
            }
        )
    return results


async def get_series_detail(series_id: str) -> Optional[dict]:
    """获取单个 Series 的详情（基础信息 + 最新剧集列表）。"""
    # 获取 Series 基本信息
    params = {
        "UserId": config.user_id,
        "Fields": "Overview,Genres,PremiereDate,CommunityRating,Status",
    }
    show_data = await _api_get(f"/Shows/{series_id}", params)
    if not show_data:
        # Shows 接口 404 时尝试 Items 接口
        params["IncludeItemTypes"] = "Series"
        items_data = await _api_get("/Items", {"UserId": config.user_id, "Ids": series_id, **params})
        if items_data and items_data.get("Items"):
            show_data = items_data["Items"][0]

    # 获取剧集列表（最新 10 集）
    eps_data = await _api_get(
        f"/Shows/{series_id}/Episodes",
        {"UserId": config.user_id, "Limit": 10, "Fields": "Overview,PremiereDate"},
    )

    if not show_data:
        return None

    episodes = []
    if eps_data:
        for ep in eps_data.get("Items", []):
            episodes.append(
                {
                    "season": ep.get("ParentIndexNumber", 0),
                    "episode": ep.get("IndexNumber", 0),
                    "name": ep.get("Name", ""),
                    "premiere_date": ep.get("PremiereDate", "")[:10],
                    "overview": ep.get("Overview", ""),
                }
            )

    img_tag = show_data.get("ImageTags", {}).get("Primary", "")
    img_url = ""
    if img_tag:
        img_url = f"{config.base_url}/Items/{series_id}/Images/Primary?tag={img_tag}&api_key={config.api_token}"

    return {
        "id": series_id,
        "name": show_data.get("Name", ""),
        "overview": show_data.get("Overview", ""),
        "year": show_data.get("ProductionYear"),
        "rating": show_data.get("CommunityRating"),
        "genres": show_data.get("Genres", []),
        "status": show_data.get("Status", ""),
        "image_url": img_url,
        "episodes_total": eps_data.get("TotalRecordCount") if eps_data else 0,
        "episodes": episodes,
    }


# ─────────────────────────────────────────────
# 2. 最近新增
# ─────────────────────────────────────────────

def _parse_dt(iso: Optional[str]) -> Optional[datetime]:
    """解析 Emby 的 ISO 时间（UTC，带 Z 后缀），失败返回 None。"""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _fmt_local(iso: Optional[str]) -> str:
    """把 Emby 的 UTC 时间转成本地时间文本，精确到秒，如 '09-30 01:45:33'。"""
    dt = _parse_dt(iso)
    return dt.astimezone().strftime("%m-%d %H:%M:%S") if dt else ""


def _parse_group(path: str) -> str:
    """从媒体文件名解析字幕组/发布组，如 '... - S02E11 - orion origin.mp4' -> 'orion origin'。"""
    if not path:
        return ""
    stem = path.replace("\\", "/").rsplit("/", 1)[-1]
    stem = stem.rsplit(".", 1)[0]
    m = re.search(r"[Ss]\d{1,2}[Ee]\d{1,3}\s*-\s*(.+)$", stem)
    if m:
        return m.group(1).strip("[] ")
    return ""


def _series_poster(series_id: str, tag: str) -> str:
    if not tag:
        return ""
    return f"{config.base_url}/Items/{series_id}/Images/Primary?tag={tag}&api_key={config.api_token}"


async def get_recent_series(limit: int = 10, days: int = 7, lib_id: Optional[str] = None) -> list[dict]:
    """
    获取最近新增/更新的番剧，包含"整部入库"与"话更新"两类。

    Args:
        limit:  返回条数
        days:   时间范围（天）
        lib_id: 指定库 ID（None = 番剧库）

    Returns:
        list[dict]，每条包含 id / name_cn / image_url / year / added_date /
        last_time（最后更新时间，本地） / kind("new"|"update") / ep_count /
        episodes（每集 season/index/title/groups/regroup）
    """
    target_lib = lib_id or config.anime_lib_id
    min_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")

    base_params = {
        "UserId": config.user_id,
        "ParentId": target_lib,
        "SortBy": "DateCreated",
        "SortOrder": "Descending",
        "Recursive": "true",
        "MinDateCreated": min_date,
    }
    # 注意：ProductionYear/DateCreated 必须显式加入 Fields，接口默认不返回
    series_params = {
        **base_params,
        "Limit": limit * 3,
        "IncludeItemTypes": "Series",
        "Fields": "PremiereDate,ProductionYear,DateCreated",
    }
    ep_params = {
        **base_params,
        "Limit": 300,
        "IncludeItemTypes": "Episode",
        "Fields": "DateCreated,ParentIndexNumber,MediaSources",
    }

    series_data = await _api_get(f"/Users/{config.user_id}/Items", series_params)
    ep_data = await _api_get(f"/Users/{config.user_id}/Items", ep_params)
    if not series_data and not ep_data:
        return []

    entries: dict[str, dict] = {}
    # 1) 整部入库的剧集
    for item in (series_data or {}).get("Items", []):
        entries[item["Id"]] = {
            "id": item["Id"],
            "name": item.get("Name", ""),
            "name_cn": item.get("Name", ""),
            "year": item.get("ProductionYear"),
            "image_url": _series_poster(item["Id"], item.get("ImageTags", {}).get("Primary", "")),
            "added_date": (item.get("DateCreated") or "")[:10],
            "created_time": _fmt_local(item.get("DateCreated")),
            "kind": "new",
            "ep_count": 0,
            "episodes": [],
            "_last_dt": _parse_dt(item.get("DateCreated")),
        }

    # 2) 话更新：按剧集聚合
    update_series_ids: list[str] = []
    for ep in (ep_data or {}).get("Items", []):
        sid = ep.get("SeriesId")
        if not sid:
            continue
        season = ep.get("ParentIndexNumber") or 0
        idx = ep.get("IndexNumber")
        if idx is None:
            continue
        if sid in entries:
            entry = entries[sid]
        else:
            if sid not in update_series_ids:
                update_series_ids.append(sid)
            entry = entries.setdefault(
                sid,
                {
                    "id": sid,
                    "name": ep.get("SeriesName", ""),
                    "name_cn": ep.get("SeriesName", ""),
                    "year": None,
                    "image_url": _series_poster(
                        sid, ep.get("SeriesPrimaryImageTag", "")
                    ),
                    "added_date": "",
                    "kind": "update",
                    "ep_count": 0,
                    "episodes": [],
                    "_last_dt": None,
                },
            )
        # 同一集存在多个媒体源 = 更换/追加了字幕组版本
        groups: list[str] = []
        for m in ep.get("MediaSources") or []:
            g = _parse_group(m.get("Path") or "")
            if g and g not in groups:
                groups.append(g)
        entry["episodes"].append(
            {
                "season": season,
                "index": idx,
                "title": ep.get("Name") or "",
                "groups": groups,
                "regroup": len(groups) > 1,
                "time": _fmt_local(ep.get("DateCreated")),
            }
        )
        entry["ep_count"] += 1
        ep_dt = _parse_dt(ep.get("DateCreated"))
        if ep_dt and (entry["_last_dt"] is None or ep_dt > entry["_last_dt"]):
            entry["_last_dt"] = ep_dt
        ep_date = (ep.get("DateCreated") or "")[:10]
        if ep_date > entry.get("added_date", ""):
            entry["added_date"] = ep_date

    # 3) 为"仅更新"的剧集批量补全年份/入库时间
    if update_series_ids:
        detail_params = {
            "UserId": config.user_id,
            "Ids": ",".join(update_series_ids[: limit * 2]),
            "Limit": limit * 2,
            "Fields": "ProductionYear,DateCreated",
        }
        detail_data = await _api_get(f"/Users/{config.user_id}/Items", detail_params)
        for item in (detail_data or {}).get("Items", []):
            if item["Id"] in entries:
                entries[item["Id"]]["year"] = item.get("ProductionYear")
                created = (item.get("DateCreated") or "")[:10]
                # 整部也是窗口内入库的话会走 new 分支；这里只补空缺
                if not entries[item["Id"]]["added_date"]:
                    entries[item["Id"]]["added_date"] = created

    # 4) 集列表按季/集号正序（E01 → E12），计算最后更新时间，排序截断
    results = []
    for entry in entries.values():
        last_dt: Optional[datetime] = entry.pop("_last_dt")
        entry["last_time"] = last_dt.astimezone().strftime("%m-%d %H:%M:%S") if last_dt else ""
        entry["episodes"].sort(key=lambda e: (e["season"], e["index"]))
        results.append(entry)

    results.sort(key=lambda e: e.get("added_date") or "", reverse=True)
    return results[:limit]


# ─────────────────────────────────────────────
# 3. 播放通知（Webhook 接收端）
# ─────────────────────────────────────────────

async def parse_webhook_payload(body: dict) -> Optional[dict]:
    """
    解析 Emby Webhook POST 的通知体，提取关键信息。

    支持事件类型:
      播放类:
        - playback.start / .stop / .pause / .unpause
        - item.markplayed / .markunplayed
        - item.favoriteadded / .favoriteremoved
      用户类:
        - user.authenticated / .authenticationfailed
        - user.created / .deleted / .lockedout
        - user.passwordchanged / .policyupdated
      插件类:
        - plugin.installed / .installationfailed
        - plugin.uninstalled / .updated / .enabled / .disabled
      服务器类:
        - system.serverstartup / .serverrestart / .servershutdown
        - system.serverupdateavailable / .serverupdated
        - system.maintenancemodeenabled / .maintenancemodedisabled
        - system.backupcompleted / .backupfailed
      定时任务类:
        - scheduledtask.completed / .failed
      媒体库类:
        - library.newitem / .itemremoved
      外部:
        - external.* 任意子事件
      第三方插件 (神医助手等, 字段名按 Emby plugin 约定):
        - item.favoriteupdated / .introupdated / .creditsupdated
        - collection.itemadded / .itemremoved
        - media.deleteddeep / .metadataupdated / .imageupdated

    Returns:
        None（完全未识别的事件），或 dict:
        {
            "event": str,      # 事件类型
            "user": str,       # 播放用户名（播放类） / 空
            "title": str,      # 媒体标题
            "media_type": str, # Series / Movie / ...
            "item_id": str,    # Emby Item ID
            "series_name": str,# Series 名称（剧集时）
            "episode_name": str,# 剧集名称
            "extra": dict,     # 事件额外信息
        }
    """
    event = body.get("Event", "")
    item = body.get("Item", {}) or {}
    user = body.get("User", {}) or {}
    series_name = item.get("SeriesName", "")
    ep_name = item.get("Name", "")

    # 播放类（含 pause/unpause/favorite）
    if event in (
        "playback.start",
        "playback.stop",
        "playback.pause",
        "playback.unpause",
        "playback.progress",
        "item.markplayed",
        "item.markunplayed",
        "item.favoriteadded",
        "item.favoriteremoved",
    ):
        return {
            "event": event,
            "user": user.get("Name", "未知用户"),
            "title": series_name or item.get("Name", "未知媒体"),
            "media_type": item.get("Type", ""),
            "item_id": item.get("Id", ""),
            "series_name": series_name,
            "episode_name": ep_name if item.get("Type") == "Episode" else "",
            "extra": {},
        }

    # 用户类
    if event.startswith("user.") or event.startswith("authentication."):
        extra = {
            "user_name": user.get("Name", ""),
            "user_id": user.get("Id", "") or body.get("UserId", ""),
            "policy": body.get("Policy", {}).get("Name", "") if isinstance(body.get("Policy"), dict) else "",
            "reason": body.get("Reason", "") or body.get("FailureReason", ""),
        }
        if event in ("user.passwordchanged", "user.policyupdated"):
            extra["changed_by"] = body.get("UpdatedBy", "") or body.get("TriggerUser", {}).get("Name", "")
        return {
            "event": event,
            "user": user.get("Name", ""),
            "title": extra["user_name"] or "未知用户",
            "media_type": "",
            "item_id": "",
            "series_name": "",
            "episode_name": "",
            "extra": extra,
        }

    # 插件类
    if event.startswith("plugin."):
        extra = {
            "plugin_name": body.get("Name", "") or body.get("Plugin", ""),
            "version": body.get("Version", ""),
            "update_version": body.get("UpdateVersion", ""),
        }
        return {
            "event": event,
            "user": "",
            "title": extra["plugin_name"] or "未知插件",
            "media_type": "",
            "item_id": "",
            "series_name": "",
            "episode_name": "",
            "extra": extra,
        }

    # 服务器类
    if event.startswith("system.") or event.startswith("server."):
        extra = {}
        if event == "system.backupcompleted" or event == "system.backupfailed":
            extra["backup_name"] = body.get("BackupName", "") or body.get("Name", "")
        if event == "system.serverupdateavailable" or event == "system.serverupdated":
            extra["version"] = body.get("ServerVersion", body.get("Version", ""))
        return {
            "event": event,
            "user": "",
            "title": "",
            "media_type": "",
            "item_id": "",
            "series_name": "",
            "episode_name": "",
            "extra": extra,
        }

    # 定时任务类
    if event.startswith("scheduledtasks.") or event.startswith("scheduledtask."):
        extra = {
            "task_name": body.get("Name", "") or body.get("Key", ""),
            "task_result": body.get("Result", ""),
        }
        return {
            "event": event,
            "user": "",
            "title": extra["task_name"],
            "media_type": "",
            "item_id": "",
            "series_name": "",
            "episode_name": "",
            "extra": extra,
        }

    # 媒体库类
    if event in ("library.newitem", "library.itemremoved"):
        return {
            "event": event,
            "user": user.get("Name", ""),
            "title": item.get("Name", "未知媒体"),
            "media_type": item.get("Type", ""),
            "item_id": item.get("Id", ""),
            "series_name": series_name,
            "episode_name": ep_name if item.get("Type") == "Episode" else "",
            "extra": {},
        }

    # 第三方插件：collection / favorite / intro / credits / media 增强事件
    if event.startswith("collection.") or event.startswith(
        ("item.favoriteupdated", "item.introupdated", "item.creditsupdated")
    ) or event.startswith("media."):
        extra = {
            "media_path": item.get("Path", ""),
            "media_type": item.get("Type", ""),
        }
        return {
            "event": event,
            "user": user.get("Name", ""),
            "title": item.get("Name", "") or series_name or "未知媒体",
            "media_type": item.get("Type", ""),
            "item_id": item.get("Id", ""),
            "series_name": series_name,
            "episode_name": ep_name if item.get("Type") == "Episode" else "",
            "extra": extra,
        }

    # 外部通知 (generic)
    if event.startswith("external."):
        extra = {
            "external_name": body.get("Name", ""),
            "external_type": body.get("Type", ""),
            "data": {k: v for k, v in body.items() if k not in ("Event", "Item", "User", "Server")},
        }
        return {
            "event": event,
            "user": user.get("Name", ""),
            "title": extra["external_name"] or "外部通知",
            "media_type": "",
            "item_id": "",
            "series_name": "",
            "episode_name": "",
            "extra": extra,
        }

    # 完全未识别但仍返回（配合 _event_to_text 的通用 fallback）
    return {
        "event": event or "unknown",
        "user": user.get("Name", ""),
        "title": item.get("Name", "") or series_name or "",
        "media_type": item.get("Type", ""),
        "item_id": item.get("Id", ""),
        "series_name": series_name,
        "episode_name": ep_name if item.get("Type") == "Episode" else "",
        "extra": {"raw_event": event},
    }
