"""
Emby 搜库插件
功能：
  1. 搜库 - 在 Emby 媒体库中搜索番剧
  2. 最近新增 - 查看最近入库的番剧
  3. 播放通知 - 接收 Emby Webhook 并私聊推送给指定用户
"""
import asyncio
import json
import threading
from datetime import datetime
from pathlib import Path

import nonebot
from aiohttp import web
from nonebot.adapters import Bot
from nonebot.drivers import Driver
from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata
from nonebot_plugin_alconna import (
    Alconna,
    Arparma,
    Args,
    Match,
    on_alconna,
)
from nonebot_plugin_session import EventSession
from zhenxun.configs.config import Config
from zhenxun.configs.utils import (
    BaseBlock,
    Command,
    PluginExtraData,
    RegisterConfig,
)
from zhenxun.services.log import logger
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

from .config import config
from .data_source import (
    get_recent_series,
    get_series_detail,
    parse_webhook_payload,
    search_series,
)

__plugin_meta__ = PluginMetadata(
    name="Emby 搜库",
    description="查询本地 Emby 媒体库，搜索番剧 / 查看最近新增 / 播放通知推送",
    usage="""
    指令：
        emby搜番 [关键词]     — 在 Emby 媒体库搜索番剧
        emby新增动态            — 查看最近的入库/更新动态
        emby新增动态 30         — 查看最近30天的动态
    """.strip(),
    extra=PluginExtraData(
        author="DaisySG",
        version="1.0",
        menu_type="媒体工具",
        commands=[
            Command(command="emby搜番 [关键词]"),
            Command(command="emby新增动态 [天数?]"),
        ],
        limits=[BaseBlock(result="搜索进行中，请稍候！")],
        configs=[
            RegisterConfig(
                key="EMBY_API_TOKEN",
                value="YOUR_EMBY_API_TOKEN",
                help="Emby 服务器的 API Token（可在 Emby 控制台生成）",
            ),
            RegisterConfig(
                key="EMBY_USER_ID",
                value="YOUR_EMBY_USER_ID",
                help="用于查询 Emby 数据的用户 ID",
            ),
            RegisterConfig(
                key="EMBY_BASE_URL",
                value="http://localhost:8094",
                help="Emby 服务器地址",
            ),
            RegisterConfig(
                key="EMBY_PLAY_NOTIFY_QQ",
                value="3109725755",
                help="Emby 播放通知推送目标 QQ 号",
            ),
            RegisterConfig(
                key="EMBY_ANIME_LIB_ID",
                value="3",
                help="番剧库 Library ID（默认3）",
            ),
            RegisterConfig(
                key="EMBY_RECENT_DAYS",
                value=1,
                help="最近新增默认查询天数（已弃用，默认固定 1 天）",
                type=int,
            ),
            RegisterConfig(
                key="EMBY_WEBHOOK_PORT",
                value=8095,
                help="Emby Webhook 接收端口（需与 Emby Webhook 配置一致）",
                type=int,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_PLAYBACK",
                value=True,
                help="是否推送播放事件（开始/停止/标记已看）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_LIBRARY",
                value=True,
                help="是否推送媒体库事件（新增/删除）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_SERVER",
                value=True,
                help="是否推送服务器事件（启动/重启/更新/备份/维护等）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_USER",
                value=True,
                help="是否推送用户事件（创建/删除/锁定/密码修改等）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_PLUGIN",
                value=True,
                help="是否推送插件事件（安装/卸载/更新）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_UNKNOWN",
                value=False,
                help="是否推送未识别的事件（第三方插件等）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_PAUSE",
                value=False,
                help="是否推送播放暂停/取消暂停（默认关，避免缓冲/拖动刷屏）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_NOTIFY_TASK_COMPLETED",
                value=True,
                help="是否推送定时任务完成（默认开，但同任务名去抖）",
                type=bool,
            ),
            RegisterConfig(
                key="EMBY_TASK_NAME_WHITELIST",
                value="",
                help="定时任务白名单（逗号分隔，仅推送名单中的任务完成；留空=不限制）",
            ),
            RegisterConfig(
                key="EMBY_TASK_DEBOUNCE_SEC",
                value=60,
                help="定时任务去抖秒数（同任务名在这个时间内只推一次）",
                type=int,
            ),
        ],
    ).to_dict(),
)

driver: Driver = nonebot.get_driver()
_log = logger  # 使用真寻 logger，日志会写入 zhenxun log 文件

# 去抖缓存：{ (event_type, dedupe_key): last_pushed_monotonic }
_dedupe_cache: dict[tuple[str, str], float] = {}
_dedupe_cache_lock = None  # threaded handler 环境加锁


def _is_deduped(event_type: str, dedupe_key: str, debounce_sec: int) -> bool:
    """返回 True 表示本次应被去抖掉（重复推送）。"""
    if debounce_sec <= 0 or not dedupe_key:
        return False
    import time as _time
    now = _time.monotonic()
    full_key = (event_type, dedupe_key)
    last = _dedupe_cache.get(full_key, 0.0)
    if (now - last) < debounce_sec:
        return True
    _dedupe_cache[full_key] = now
    return False


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────
def _load_config() -> None:
    """从数据库配置覆盖默认配置。"""
    token = Config.get_config("emby_search", "EMBY_API_TOKEN")
    if token:
        config.api_token = token
    user_id = Config.get_config("emby_search", "EMBY_USER_ID")
    if user_id:
        config.user_id = user_id
    base_url = Config.get_config("emby_search", "EMBY_BASE_URL")
    if base_url:
        config.base_url = base_url


def _event_to_text(parsed: dict) -> str | None:
    """将解析后的事件字典转为通知文本。根据配置开关过滤类别。"""
    event = parsed["event"]
    user = parsed["user"]
    title = parsed["title"]
    ep_name = parsed.get("episode_name", "")
    extra = parsed.get("extra", {}) or {}

    # 类别开关检查
    is_playback = event in (
        "playback.start",
        "playback.stop",
        "playback.pause",
        "playback.unpause",
        "playback.progress",
        "item.markplayed",
        "item.markunplayed",
        "item.favoriteadded",
        "item.favoriteremoved",
    )
    is_library = event in ("library.newitem", "library.itemremoved")
    is_server = event.startswith("system.") or event.startswith("server.")
    is_scheduled = event.startswith("scheduledtasks.") or event.startswith("scheduledtask.")
    is_user = event.startswith("user.") or event.startswith("authentication.")
    is_plugin = event.startswith("plugin.")
    is_external = event.startswith("external.")

    if is_playback and not Config.get_config("emby_search", "EMBY_NOTIFY_PLAYBACK"):
        return None
    if is_library and not Config.get_config("emby_search", "EMBY_NOTIFY_LIBRARY"):
        return None
    if (is_server or is_scheduled) and not Config.get_config("emby_search", "EMBY_NOTIFY_SERVER"):
        return None
    if is_user and not Config.get_config("emby_search", "EMBY_NOTIFY_USER"):
        return None
    if is_plugin and not Config.get_config("emby_search", "EMBY_NOTIFY_PLUGIN"):
        return None

    # 播放类
    if event == "playback.start":
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"📺 {user} 开始播放\n{suffix}"
    elif event == "playback.stop":
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"⏹ {user} 停止播放\n{suffix}"
    elif event == "playback.pause":
        if not Config.get_config("emby_search", "EMBY_NOTIFY_PAUSE"):
            return None
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"⏸ {user} 暂停播放\n{suffix}"
    elif event == "playback.unpause":
        if not Config.get_config("emby_search", "EMBY_NOTIFY_PAUSE"):
            return None
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"▶️ {user} 取消暂停\n{suffix}"
    elif event == "playback.progress":
        return None  # 进度上报太频繁，默认静默
    elif event == "item.markplayed":
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"✅ {user} 标记已看\n{suffix}"
    elif event == "item.markunplayed":
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"❌ {user} 标记未看\n{suffix}"
    elif event == "item.favoriteadded":
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"❤️ {user} 添加收藏\n{suffix}"
    elif event == "item.favoriteremoved":
        suffix = f"「{title}」{ep_name}" if ep_name else f"「{title}」"
        return f"💔 {user} 移出收藏\n{suffix}"

    # 用户类
    elif event == "user.authenticated" or event == "authentication.succeeded":
        name = title or user or "未知"
        return f"✅ 用户登录成功\n{name}"
    elif event == "user.authenticationfailed" or event == "authentication.failed":
        name = title or user or "未知"
        reason = extra.get("reason", "")
        suffix = f"\n原因：{reason}" if reason else ""
        return f"❌ 用户登录失败\n{name}{suffix}"
    elif event == "user.lockedout":
        name = title or user or "未知"
        return f"🔒 用户已被锁定\n{name}"
    elif event == "user.created":
        name = title or user or "未知"
        return f"👤 新建用户\n{name}"
    elif event == "user.deleted":
        name = title or user or "未知"
        return f"🗑 删除用户\n{name}"
    elif event == "user.passwordchanged":
        name = title or user or "未知"
        by = extra.get("changed_by", "")
        suffix = f"\n操作者：{by}" if by else ""
        return f"🔑 密码已更改\n{name}{suffix}"
    elif event == "user.policyupdated":
        name = title or user or "未知"
        by = extra.get("changed_by", "")
        suffix = f"\n操作者：{by}" if by else ""
        return f"📋 策略已更新\n{name}{suffix}"

    # 插件类
    elif event == "plugin.installed":
        return f"📦 插件已安装\n{title or extra.get('plugin_name', '')}"
    elif event == "plugin.installationfailed":
        return f"❌ 插件安装失败\n{title or extra.get('plugin_name', '')}"
    elif event == "plugin.uninstalled":
        return f"🗑 插件已卸载\n{title or extra.get('plugin_name', '')}"
    elif event == "plugin.updated":
        old = extra.get("version", "")
        new = extra.get("update_version", "")
        suffix = f"\n{old} → {new}" if old and new else ""
        return f"🔄 插件已更新\n{title or extra.get('plugin_name', '')}{suffix}"
    elif event == "plugin.enabled":
        return f"✅ 插件已启用\n{title or extra.get('plugin_name', '')}"
    elif event == "plugin.disabled":
        return f"🚫 插件已禁用\n{title or extra.get('plugin_name', '')}"

    # 服务器类
    elif event in ("system.serverstartup", "system.serverstartupcomplete"):
        return "🚀 Emby 服务器已启动"
    elif event == "system.serverrestart":
        return "⚠️ Emby 服务器需要重启"
    elif event == "system.servershutdown":
        return "🛑 Emby 服务器已关闭"
    elif event == "system.serverupdateavailable":
        ver = extra.get("version", "")
        suffix = f"（{ver}）" if ver else ""
        return f"⬆️ Emby 有新版本可用{suffix}"
    elif event == "system.serverupdated":
        ver = extra.get("version", "")
        suffix = f"（{ver}）" if ver else ""
        return f"✅ Emby 已更新{suffix}"
    elif event == "system.maintenancemodeenabled":
        return "🔧 Emby 进入维护模式"
    elif event == "system.maintenancemodedisabled":
        return "✅ Emby 退出维护模式"
    elif event == "system.backupcompleted":
        name = extra.get("backup_name", "")
        suffix = f"\n备份：{name}" if name else ""
        return f"💾 Emby 备份已完成{suffix}"
    elif event == "system.backupfailed":
        name = extra.get("backup_name", "")
        suffix = f"\n备份：{name}" if name else ""
        return f"❌ Emby 备份失败{suffix}"

    # 定时任务类
    elif event in ("scheduledtasks.completed", "scheduledtask.completed"):
        if not Config.get_config("emby_search", "EMBY_NOTIFY_TASK_COMPLETED"):
            return None
        name = extra.get("task_name", "")
        # 白名单过滤（留空 = 不限制）
        whitelist_raw = Config.get_config("emby_search", "EMBY_TASK_NAME_WHITELIST") or ""
        whitelist = [x.strip().lower() for x in whitelist_raw.split(",") if x.strip()]
        if whitelist and name.lower() not in whitelist:
            return None
        # 同任务名去抖
        debounce_sec = int(Config.get_config("emby_search", "EMBY_TASK_DEBOUNCE_SEC") or 0)
        if name and _is_deduped(event, name, debounce_sec):
            return None
        suffix = f"\n任务：{name}" if name else ""
        return f"⏰ Emby 定时任务完成{suffix}"
    elif event in ("scheduledtasks.failed", "scheduledtask.failed"):
        name = extra.get("task_name", "")
        suffix = f"\n任务：{name}" if name else ""
        return f"❌ Emby 定时任务失败{suffix}"

    # 媒体库类
    elif event == "library.newitem":
        if parsed["media_type"] == "Episode" and title and ep_name:
            return f"🆕 媒体库新增剧集\n「{title}」{ep_name}"
        else:
            return f"🆕 媒体库新增\n「{title}」"
    elif event == "library.itemremoved":
        if parsed["media_type"] == "Episode" and title and ep_name:
            return f"🗑 媒体库删除剧集\n「{title}」{ep_name}"
        else:
            return f"🗑 媒体库删除\n「{title}」"

    # 第三方插件 (神医助手等)
    elif event in ("item.favoriteupdated",):
        suffix = f"「{title}」{ep_name}" if title else ""
        return f"💖 收藏状态已更新\n{suffix}".rstrip()
    elif event in ("item.introupdated", "item.creditsupdated"):
        suffix = f"「{title}」{ep_name}" if title else ""
        label = "片头已更新" if "intro" in event else "片尾已更新"
        return f"🎬 {label}\n{suffix}".rstrip()
    elif event == "collection.itemadded":
        suffix = f"「{title}」{ep_name}" if title else ""
        return f"➕ 合集项目已添加\n{suffix}".rstrip()
    elif event == "collection.itemremoved":
        suffix = f"「{title}」{ep_name}" if title else ""
        return f"➖ 合集项目已移除\n{suffix}".rstrip()
    elif event in ("media.deleteddeep", "library.deleteddeep"):
        return f"🔥 媒体已深度删除\n{title or ep_name or '未知'}"
    elif event in ("media.metadataupdated", "library.metadataupdated"):
        return f"📝 元数据已更新\n{title or ep_name or '未知'}"
    elif event in ("media.imageupdated", "library.imageupdated"):
        return f"🖼 媒体图像已更新\n{title or ep_name or '未知'}"

    # 外部通知
    elif event.startswith("external."):
        ext_name = extra.get("external_name", "") or title or "外部通知"
        return f"🔗 外部通知\n{ext_name}"

    # 完全未识别事件 (默认静默除非开启 EMBY_NOTIFY_UNKNOWN)
    if not Config.get_config("emby_search", "EMBY_NOTIFY_UNKNOWN"):
        return None
    fallback_name = title or ep_name or extra.get("plugin_name", "") or ""
    return f"📢 Emby 事件：{event}\n{fallback_name}".rstrip()


# ─────────────────────────────────────────────
# aiohttp Webhook 处理
# ─────────────────────────────────────────────
_async_loop: asyncio.AbstractEventLoop | None = None
_web_app: web.Application | None = None


async def _handle_emby_webhook(request: web.Request) -> web.Response:
    """处理 Emby Webhook POST 请求。"""
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text="Invalid JSON")

    event_type = body.get("Event", "unknown")
    item = body.get("Item") or {}
    item_name = item.get("SeriesName") or item.get("Name", "")
    _log.info(f"收到 Emby Webhook: {event_type} | {item_name}")

    parsed = await parse_webhook_payload(body)
    if not parsed:
        return web.Response(
            status=200,
            text=json.dumps({"success": True, "ignored": True}),
            content_type="application/json",
        )

    msg_text = _event_to_text(parsed)
    if not msg_text:
        return web.Response(
            status=200,
            text=json.dumps({"success": True, "ignored": True}),
            content_type="application/json",
        )

    # 私聊推送
    target_qq = Config.get_config("emby_search", "EMBY_PLAY_NOTIFY_QQ")
    if not target_qq:
        _log.warning("[emby_search] 未配置 EMBY_PLAY_NOTIFY_QQ，跳过推送")
        return web.Response(
            status=200,
            text=json.dumps({"success": False, "reason": "no target"}),
            content_type="application/json",
        )

    # 调度异步任务推到 NoneBot 事件循环，不阻塞 webhook 响应
    if _async_loop and _async_loop.is_running():
        asyncio.run_coroutine_threadsafe(
            _send_private(target_qq, msg_text), _async_loop
        )
        return web.Response(
            status=200,
            text=json.dumps({"success": True, "scheduled": True}),
            content_type="application/json",
        )
    else:
        _log.warning("[emby_search] 事件循环未就绪")
        return web.Response(
            status=503,
            text=json.dumps({"success": False, "reason": "loop not ready"}),
            content_type="application/json",
        )


async def _send_private(qq: str, message: str) -> bool:
    """通过真寻 BOT 私聊推送消息。不等回执，避免挂死。"""
    try:
        bot = PlatformUtils._resolve_unique_qq_client_bot("[emby_search]send_private")
        if not bot:
            _log.error("[emby_search] 未找到可用的 QQ Bot 实例")
            return False

        # 直接调用 bot.send_private_msg，不走 SendQueue 不等回执
        # 避免 OneBot 协议端慢响应造成 webhook 挂起
        from nonebot.adapters.onebot.v11 import Message
        msg = Message(message)
        try:
            await bot.send_private_msg(user_id=int(qq), message=msg)
            _log.info(f"[emby_search] 私聊推送成功 -> {qq}: {message[:40]}")
            return True
        except Exception as e:
            _log.error(f"[emby_search] bot.send_private_msg 失败: {e}")
            return False
    except Exception as e:
        _log.error(f"[emby_search] 私聊推送异常: {e}")
        return False


async def _health_check(request: web.Request) -> web.Response:
    return web.Response(text="OK", content_type="text/plain")


# ─────────────────────────────────────────────
# Webhook 服务线程
# ─────────────────────────────────────────────
def _run_webhook_server(port: int) -> None:
    """在独立线程中运行 aiohttp 服务。"""
    global _web_app
    _web_app = web.Application()
    _web_app.router.add_post("/emby", _handle_emby_webhook)
    _web_app.router.add_get("/health", _health_check)

    _log.info(f"Emby Webhook 服务启动，监听 :{port}")
    _log.info(f"请在 Emby 控制台配置 Webhook URL: http://<本机IP>:{port}/emby")
    # 禁用访问日志，避免 NoneBot logger 对象与 aiohttp 不兼容
    web.run_app(_web_app, host="0.0.0.0", port=port, print=None, access_log=None)


@driver.on_startup
async def _start_webhook_server() -> None:
    """BOT 启动时在后台线程启动 Webhook HTTP 服务。"""
    global _async_loop
    _async_loop = asyncio.get_running_loop()

    webhook_port = Config.get_config("emby_search", "EMBY_WEBHOOK_PORT") or 8095
    t = threading.Thread(target=_run_webhook_server, args=(int(webhook_port),), daemon=True)
    t.start()
    _log.info(f"Emby Webhook 服务线程已启动（端口 {webhook_port}）")


# ─────────────────────────────────────────────
# 1. 搜库命令
# ─────────────────────────────────────────────
_search_matcher = on_alconna(
    Alconna("emby搜番", Args["keyword?", str]),
    priority=5,
    block=True,
    use_origin=True,
)


@_search_matcher.handle()
async def _search_handler(
    session: EventSession,
    arparma: Arparma,
    keyword: Match[str],
):
    _load_config()
    if not keyword.available:
        await MessageUtils.build_message("请输入要搜索的番剧名称或关键词～").send(
            at_sender=True
        )
        return

    kw = keyword.result.strip()
    if not kw:
        await MessageUtils.build_message("关键词不能为空哦～").send(at_sender=True)
        return

    await MessageUtils.build_message(f"正在搜索「{kw}」...").send(at_sender=True)
    results = await search_series(kw, limit=8)

    if not results:
        await MessageUtils.build_message(
            f"在 Emby 媒体库中没找到「{kw}」相关的番剧\n"
            "（可能是里番，需要换关键词试试？）"
        ).send(at_sender=True)
        return

    for item in results:
        item["is_r18"] = item["parent_id"] == config.r18_lib_id

    # 渲染卡片图，失败回退纯文本
    try:
        from nonebot_plugin_htmlrender import template_to_pic

        pic = await template_to_pic(
            template_path=str((Path(__file__).parent / "templates").absolute()),
            template_name="search.html",
            templates={"items": results, "keyword": kw},
            pages={
                "viewport": {"width": 640, "height": 10},
                "base_url": f"file://{Path(__file__).parent}",
            },
            wait=3,
        )
        await MessageUtils.build_message(pic).send(at_sender=True)
    except Exception as e:
        logger.warning(f"Emby搜番 渲染图片失败，回退文本: {e}")
        lines = [f"🔍 Emby 搜索「{kw}」结果（共 {len(results)} 条）：\n"]
        for i, item in enumerate(results, 1):
            rating_str = f" ⭐{item['rating']:.1f}" if item["rating"] else ""
            lib_tag = "【里番】" if item["is_r18"] else ""
            lines.append(
                f"{i}. {lib_tag}{item['name_cn']}"
                f"{rating_str}"
                f"（{item['year'] or '?'}）"
            )
            if item["overview"]:
                overview = item["overview"].replace("\n", " ")[:80]
                lines.append(f"   📖 {overview}…")
        await MessageUtils.build_message("\n".join(lines)).send(at_sender=True)

    logger.info(
        f"Emby搜番: {kw} -> {len(results)}条",
        arparma.header_result,
        session=session,
    )


# ─────────────────────────────────────────────
# 2. 最近新增命令
# ─────────────────────────────────────────────
_recent_matcher = on_alconna(
    Alconna("emby新增动态", Args["days?", int]),
    priority=5,
    block=True,
)


# 2026-09-30 修复: 原写法为 handle() pass + got_path("days", prompt=None)，
# got_path 会对"下一条消息"做 int 校验，带参直发（如 "emby新增动态 1"）时校验失败
# 且 prompt 为 None，导致静默挂起、无任何回复。改为在 handle 中直接取参。
@_recent_matcher.handle()
async def _recent_handler(
    session: EventSession,
    arparma: Arparma,
    days: Match[int],
):
    _load_config()
    # 默认固定 1 天：EMBY_RECENT_DAYS 的持久化配置值会被关闭时回写覆盖，
    # 改配置文件/代码默认值都无法稳定生效，故无参时硬编码为 1
    query_days = days.result if days.available else 1
    query_days = max(1, min(int(query_days), 365))

    results = await get_recent_series(limit=10, days=query_days)

    if not results:
        await MessageUtils.build_message(
            f"近 {query_days} 天内没有新增的番剧入库 🎬"
        ).send(at_sender=True)
        return

    # 2026-09-30: 渲染带海报的卡片图；失败时回退纯文本
    text_lines = [f"📅 Emby 番剧库最近 {query_days} 天新增（共 {len(results)} 部）：\n"]
    for i, item in enumerate(results, 1):
        text_lines.append(
            f"{i}. {item['name_cn']}（{item['year'] or '?'}）"
            f"  🗓 {item['added_date']}"
        )

    try:
        from nonebot_plugin_htmlrender import template_to_pic

        now_str = datetime.now().strftime("%m-%d %H:%M:%S")
        last_update = max((i.get("last_time") or "" for i in results), default="")
        pic = await template_to_pic(
            template_path=str((Path(__file__).parent / "templates").absolute()),
            template_name="recent.html",
            templates={
                "items": results,
                "days": query_days,
                "now": now_str,
                "last_update": last_update,
            },
            pages={
                "viewport": {"width": 640, "height": 10},
                "base_url": f"file://{Path(__file__).parent}",
            },
            wait=3,
        )
        await MessageUtils.build_message(pic).send(at_sender=True)
    except Exception as e:
        logger.warning(f"[emby_search] 新增列表图渲染失败，回退纯文本: {e}")
        await MessageUtils.build_message("\n".join(text_lines)).send(at_sender=True)

    logger.info(
        f"Emby新增: {query_days}天 -> {len(results)}部",
        arparma.header_result,
        session=session,
    )


# ─────────────────────────────────────────────
# 3. 调试命令（仅超级用户）
# ─────────────────────────────────────────────
_debug_matcher = on_alconna(
    Alconna("emby调试"),
    priority=5,
    block=True,
    permission=SUPERUSER,
)


@_debug_matcher.handle()
async def _debug_handler(session: EventSession, arparma: Arparma):
    _load_config()
    webhook_port = Config.get_config("emby_search", "EMBY_WEBHOOK_PORT") or 8095
    target_qq = Config.get_config("emby_search", "EMBY_PLAY_NOTIFY_QQ") or "未配置"
    msg = (
        f"📡 Emby 连接信息：\n"
        f"  地址：{config.base_url}\n"
        f"  用户：{config.user_id}\n"
        f"  番剧库ID：{config.anime_lib_id}\n"
        f"  Token：{config.api_token[:8]}…\n"
        f"  通知目标：{target_qq}\n"
        f"  Webhook端口：{webhook_port}\n"
        f"  Webhook地址：http://<本机IP>:{webhook_port}/emby"
    )
    await MessageUtils.build_message(msg).send(at_sender=True)
