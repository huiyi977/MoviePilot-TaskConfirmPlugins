from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Request

from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.search import SearchChain
from app.chain.subscribe import SubscribeChain
from app.sdk.plugin import _PluginBase
from app.sdk.services import ServiceConfigHelper
from app.schemas.types import MediaSource, MediaType, NotificationType
from app.sdk.logging import logger
from app.utils.http import RequestUtils

# =====================================================================
#  常量（与 TaskConfirmMax 保持一致）
# =====================================================================

DEFAULT_MINIMAL_TEXT = "【待处理确认】\\n{time} 你有一条待处理确认任务"

DEFAULT_BARK_SERVER = "https://api.day.app"


def _fmt_duration(sec: int) -> str:
    sec = max(0, int(sec))
    h, rem = divmod(sec, 3600)
    if rem == 0:
        return f"{h} 小时"
    return f"{h} 小时 {rem // 60} 分钟"


class TaskConfirmBridge(_PluginBase):
    """
    任务确认中心桥接（v2.0.0）：确认中心发起「mp 搜索词」→ 本插件搜索媒体 →
    候选列表作为确认任务推回中心 → 用户在中心 H5 选择媒体（阶段 1）→
    查媒体库是否已存在（命中则通知并终止）→ 按 media_source/media_id 搜索站点资源，
    编号展示标题/大小/站点/做种数（阶段 2）→ 选定资源后自动用 MP 默认下载器下载
    （阶段 3，可关闭改为推送下载器选择卡片）。

    与 TaskConfirmMax 的能力对齐（**不含**内嵌 Web 确认服务，卡片仍由外部确认中心承载）：
      · 媒体库已存在检查（check_library，默认开启）
      · 阶段 1/2 搜索为 0 条或搜索失败时主动推送通知（带 MP 主页入口），并在中心留一条提示记录
      · 内置通知直推 bark / 企业微信机器人 / gotify（极简模式可选），不依赖外部脚本
      · 可选 builtin_card_notify：卡片通知改由插件内置直推（默认关闭，仍走确认中心推送）
    """

    plugin_name = "任务确认桥接"
    plugin_desc = (
        "把任务确认中心的「mp 搜索词」转成 MP 媒体搜索，候选推回确认中心选择后再搜索站点资源，"
        "二次选定后自动调用默认下载器下载；含媒体库已存在检查、搜索无结果提醒与内置通知直推"
        "（bark/企微/gotify）。"
    )
    plugin_icon = "https://raw.githubusercontent.com/huiyi977/MoviePilot-TaskConfirmPlugins/main/icons/taskconfirmbridge.png"
    plugin_version = "2.0.0"
    plugin_author = "huiyi977"
    author_url = ""
    plugin_config_prefix = "taskconfirmbridge_"
    plugin_order = 30
    auth_level = 1

    _enabled = False
    _center_url = ""
    _center_token = ""
    _max_options = 8
    _task_timeout = 3600
    _wait_total = 21600
    _poll_interval = 5
    _send_center_notify = True
    _result_notify = True
    _torrent_mode = True
    _auto_downloader = True
    _check_library = True
    _mp_base_url = ""
    _builtin_card_notify = False
    _notify_enabled = True
    _notify_channel = "wecom"
    _notify_pic = ""
    _notify_timeout = 15
    _notify_minimal = True
    _notify_minimal_text = DEFAULT_MINIMAL_TEXT
    _notify_minimal_pic = False
    _bark_server = DEFAULT_BARK_SERVER
    _bark_key = ""
    _wecom_webhook = ""
    _gotify_url = ""
    _gotify_token = ""

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._history: List[Dict[str, Any]] = []
        self._stopping = False

    def init_plugin(self, config: dict | None = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._center_url = str(config.get("center_url") or "").strip().rstrip("/")
        self._center_token = str(config.get("center_token") or "").strip()
        try:
            self._max_options = max(2, min(20, int(config.get("max_options") or 8)))
        except Exception:
            self._max_options = 8
        try:
            self._task_timeout = max(60, min(86400, int(config.get("task_timeout") or 3600)))
        except Exception:
            self._task_timeout = 3600
        try:
            self._wait_total = max(60, int(config.get("wait_total") or 21600))
        except Exception:
            self._wait_total = 21600
        try:
            self._poll_interval = max(1, min(60, int(config.get("poll_interval") or 5)))
        except Exception:
            self._poll_interval = 5
        self._send_center_notify = bool(config.get("send_center_notify", True))
        self._result_notify = bool(config.get("result_notify", True))
        self._torrent_mode = bool(config.get("torrent_mode", True))
        self._auto_downloader = bool(config.get("auto_downloader", True))
        self._check_library = bool(config.get("check_library", True))
        self._mp_base_url = str(config.get("mp_base_url") or "").strip().rstrip("/")
        self._builtin_card_notify = bool(config.get("builtin_card_notify", False))
        self._notify_enabled = bool(config.get("notify_enabled", True))
        self._notify_channel = (str(config.get("notify_channel") or "wecom").strip().lower() or "wecom")
        self._notify_pic = str(config.get("notify_pic") or "").strip()
        try:
            self._notify_timeout = max(3, int(config.get("notify_timeout") or 15))
        except Exception:
            self._notify_timeout = 15
        self._notify_minimal = bool(config.get("notify_minimal", True))
        self._notify_minimal_text = (str(config.get("notify_minimal_text") or "").replace("\\n", "\n").strip()
                                     or DEFAULT_MINIMAL_TEXT.replace("\\n", "\n"))
        self._notify_minimal_pic = bool(config.get("notify_minimal_pic", False))
        self._bark_server = (str(config.get("bark_server") or "").strip() or DEFAULT_BARK_SERVER)
        self._bark_key = str(config.get("bark_key") or "").strip()
        self._wecom_webhook = str(config.get("wecom_webhook") or "").strip()
        self._gotify_url = str(config.get("gotify_url") or "").strip()
        self._gotify_token = str(config.get("gotify_token") or "").strip()
        # 旧配置迁移：兜底 webhook（企微）→ wecom_webhook
        if not self._wecom_webhook:
            self._wecom_webhook = str(config.get("notify_webhook") or "").strip()

        # 历史记录持久化（重启/改配置后仍可见）
        try:
            data = self.get_data("history")
            if isinstance(data, list):
                self._history = data
        except Exception:
            self._history = []

        self._stopping = False
        if self._enabled and config.get("onlyonce"):
            logger.info("TaskConfirmBridge onlyonce 触发测试搜索：西游记")
            threading.Thread(target=self._run_search_job, args=("西游记",), daemon=True).start()
            config["onlyonce"] = False
            self.update_config(config)

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> list[dict[str, Any]]:
        return []

    def get_api(self) -> list[dict[str, Any]]:
        if not self.get_state():
            return []
        return [
            {
                "path": "/search",
                "endpoint": self.api_search,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "发起 MP 媒体搜索并创建确认任务",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "查询桥接任务状态一览",
            },
        ]

    # ------------------------------------------------------------------ API
    async def api_search(self, request: Request):
        """
        POST /api/v1/plugin/TaskConfirmBridge/search
        body: {"keyword": "西游记"} 或 ?keyword=西游记
        """
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        try:
            body = await request.json()
        except Exception:
            body = {}
        keyword = str(body.get("keyword") or request.query_params.get("keyword") or "").strip()
        # 兼容把整串 "mp 西游记" 直接传进来
        if keyword[:3].lower() == "mp ":
            keyword = keyword[3:].strip()
        if not keyword:
            return {"success": False, "message": "keyword 不能为空"}
        if not self._center_url:
            return {"success": False, "message": "未配置任务确认中心地址（center_url）"}

        # 已有同关键词任务在等选择时直接复用，避免重复建任务
        with self._lock:
            for job in self._jobs.values():
                if job["keyword"] == keyword and job["state"] == "pending":
                    return {
                        "success": True,
                        "message": f"同关键词任务已在等待选择：{job['task_id']}",
                        "task_id": job["task_id"],
                        "count": job.get("count", 0),
                        "reused": True,
                    }

        result = await asyncio.to_thread(self._run_search_job, keyword)
        return result

    async def api_status(self, request: Request):
        with self._lock:
            jobs = [self._job_public(j) for j in self._jobs.values()]
            jobs.sort(key=lambda x: x.get("created_at") or 0, reverse=True)
            history = list(reversed(self._history[-20:]))
        return {
            "success": True,
            "center": {
                "url": self._center_url,
                "configured": bool(self._center_url),
                "notify": self._notify_label(),
            },
            "jobs": jobs,
            "history": history,
        }

    # ------------------------------------------------------------------ 核心流程
    def _run_search_job(self, keyword: str) -> Dict[str, Any]:
        """同步执行：搜索 → 建确认任务 → 起后台线程收割。"""
        logger.info(f"TaskConfirmBridge 收到搜索词：{keyword}")
        try:
            _meta, medias = MediaChain().search(keyword, limit=20)
        except Exception as e:
            logger.error(f"TaskConfirmBridge 搜索失败：{e}")
            self._notify_search_empty("阶段 1 媒体搜索", keyword, f"搜索失败：{e}")
            return {"success": False, "message": f"搜索失败：{e}"}
        if not medias:
            self._append_history({"task_id": "", "keyword": keyword, "state": "empty"},
                                 "媒体搜索无结果（0 条）")
            self._notify_search_empty("阶段 1 媒体搜索", keyword, "结果：0 条（MP 未返回任何媒体条目）")
            return {"success": False, "message": f"未找到与「{keyword}」相关的媒体信息"}

        medias = medias[: self._max_options]
        options: List[Dict[str, Any]] = []
        key_map: Dict[str, Any] = {}
        for idx, media in enumerate(medias, start=1):
            key = f"opt{idx}"
            options.append({
                "key": key,
                "text": self._option_text(idx, media),
                "desc": self._option_desc(media),
            })
            key_map[key] = media

        title = f"MP 搜索：{keyword}"[:64]
        sub_title = "阶段 1/3 · TaskConfirmBridge · 请选择要订阅的条目"
        payload = {
            "title": title,
            "sub_title": sub_title,
            "style": "text",
            "options": options,
            "timeout": self._task_timeout,
            "notify": self._send_center_notify,
        }
        task_id = self._create_center_task(payload)
        if not task_id:
            return {"success": False, "message": "确认中心创建任务失败（检查 center_url 与 token）"}
        if self._builtin_card_notify:
            self._notify_card_builtin(title, sub_title)

        job = {
            "task_id": task_id,
            "keyword": keyword,
            "state": "pending",
            "stage": 1,
            "created_at": time.time(),
            "count": len(options),
        }
        with self._lock:
            self._jobs[task_id] = job
        threading.Thread(
            target=self._wait_and_subscribe, args=(task_id, keyword, key_map), daemon=True,
        ).start()
        logger.info(f"TaskConfirmBridge 已创建确认任务 {task_id}（{len(options)} 个候选）")
        return {
            "success": True,
            "message": f"已创建确认任务 {task_id}（{len(options)} 个候选），请在任务确认中心选择",
            "task_id": task_id,
            "count": len(options),
        }

    def _poll_final(self, task_id: str) -> Optional[Dict[str, Any]]:
        """后台长轮询中心任务直至结束，返回最终 status 数据（None=未取到有效结果）。"""
        base = self._center_url
        params: Dict[str, Any] = {"wait": 25}
        if self._center_token:
            params["token"] = self._center_token
        deadline = time.time() + self._wait_total
        time.sleep(2)
        while time.time() < deadline:
            if self._stopping:
                return None
            try:
                resp = RequestUtils().get_res(f"{base}/api/v1/task/status/{task_id}", params=params)
                data = resp.json() if resp is not None else {}
                state = str(data.get("state") or "")
            except Exception as e:
                logger.warn(f"TaskConfirmBridge 轮询任务 {task_id} 异常：{e}")
                time.sleep(self._poll_interval)
                continue
            if state in ("done", "expired", "unknown"):
                return data
        return None

    def _wait_and_subscribe(self, task_id: str, keyword: str, key_map: Dict[str, Any]):
        """后台线程：长轮询阶段 1 卡片，用户选中媒体后进入阶段 2（或直接订阅）。"""
        final = self._poll_final(task_id)
        with self._lock:
            job = self._jobs.get(task_id)
        if job is None:
            return
        if not final or final.get("state") != "done":
            job["state"] = "expired" if (final or {}).get("state") == "expired" else "timeout"
            self._append_history(job, "用户未选择或超时")
            return

        option_key = str(final.get("option") or "")
        option_text = str(final.get("option_text") or "")
        media = key_map.get(option_key)
        job["state"] = "done"
        job["selected"] = option_text
        if media is None:
            self._append_history(job, "选中的候选不在本进程缓存中（可能重启过）")
            return

        # 媒体库已存在检查（可在配置里关闭）：命中则通知 + 跳过后续全部阶段
        if self._check_library:
            exists_info = self._library_exists(media)
            if exists_info:
                job["state"] = "exists"
                self._append_history(
                    job, f"媒体库已存在（{exists_info.get('server') or '媒体服务器'}），已跳过")
                logger.info(f"TaskConfirmBridge 媒体库已存在，跳过阶段 2/3：{option_text}")
                self._notify_library_exists(media, exists_info)
                return

        if self._torrent_mode:
            self._run_torrent_stage(job, keyword, media, option_text)
            return

        logger.info(f"TaskConfirmBridge 任务 {task_id} 用户已选：{option_text}，开始添加订阅")
        try:
            sub_id, err = SubscribeChain().add(
                title=str(media.title or keyword),
                year=str(media.year or ""),
                mtype=media.type,
                media_source=media.media_source,
                media_id=str(media.media_id or "") or None,
                exist_ok=True,
                message=False,
            )
        except Exception as e:
            logger.error(f"TaskConfirmBridge 订阅异常：{e}")
            self._append_history(job, f"订阅异常：{e}")
            self._notify_result(f"❌ 订阅失败：{option_text}", f"任务 {task_id}\n{e}")
            return
        if sub_id:
            job["state"] = "subscribed"
            job["subscribe_id"] = sub_id
            self._append_history(job, f"已添加订阅 subscribe_id={sub_id}")
            self._notify_result(
                f"✅ 已订阅：{option_text}",
                f"搜索词：{keyword}\n任务：{task_id}\n订阅号：{sub_id}",
            )
        else:
            self._append_history(job, f"订阅失败：{err}")
            self._notify_result(f"❌ 订阅失败：{option_text}", f"任务 {task_id}\n{err or '未知原因'}")

    # ------------------------------------------------------------------ 媒体库检查
    def _library_exists(self, media) -> Optional[Dict[str, Any]]:
        """查媒体服务器（Emby/Jellyfin/Plex/…）是否已有该媒体。

        返回 None 表示媒体库没有（继续后续阶段）；否则返回 {server, text, seasons}。
        查询异常一律按「不存在」处理，避免因为媒体服务器不可用卡住整条链路。
        """
        try:
            from app.chain.mediaserver import MediaServerChain
            info = MediaServerChain().media_exists(mediainfo=media)
        except Exception as e:  # noqa: BLE001
            logger.warn(f"TaskConfirmBridge 媒体库查询失败（按不存在继续）：{e}")
            return None
        if not info:
            return None
        seasons = getattr(info, "seasons", None) or {}
        parts: List[str] = []
        for sn in sorted(seasons.keys(), key=lambda x: str(x)):
            eps = seasons.get(sn) or []
            try:
                label = f"S{int(sn):02d}"
            except Exception:
                label = f"S{sn}"
            parts.append(f"{label}：{len(eps)} 集" if eps else f"{label}：已有")
        server = (getattr(info, "server", None) or getattr(info, "server_type", None) or "")
        return {"server": str(server), "seasons": parts, "text": "、".join(parts)}

    def _notify_library_exists(self, media, info: Dict[str, Any]) -> None:
        """媒体库已存在 → 发一条通知（内置直推 + MP 原生），随后终止本次任务。"""
        title = str(getattr(media, "title", "") or "未知媒体")
        year = getattr(media, "year", None) or "未知年份"
        lines = [f"ℹ️ 媒体库已存在：{title}", f"媒体：{title}（{year}）"]
        if info.get("server"):
            lines.append(f"媒体服务器：{info['server']}")
        if info.get("text"):
            lines.append(f"已入库：{info['text']}")
        lines.append("已跳过资源搜索与下载，本次任务结束。")
        text = "\n".join(lines)
        if self._notify_enabled:
            try:
                self._send_notify(text)
            except Exception as e:  # noqa: BLE001
                logger.warn(f"TaskConfirmBridge 媒体库已存在通知发送失败：{e}")
        else:
            logger.info("TaskConfirmBridge 通知总开关关闭，媒体库已存在提示未推送")
        self._notify_result(f"ℹ️ 媒体库已存在：{title}", "\n".join(lines[1:]))
        self._record_notice(f"ℹ️ 媒体库已存在：{title}", info.get("text") or f"{title}（{year}）已入库")

    # ------------------------------------------------------------------ 搜索为空/失败的通知
    def _mp_url(self) -> str:
        """MP 主页地址（用于通知里引导用户去 MP 自查站点/索引器）。

        优先用配置 `mp_base_url`；留空时尝试取 MP 自身的对外访问地址（APP_DOMAIN），
        取不到就返回空串（通知里就只给文字提示，不给链接）。
        """
        if self._mp_base_url:
            return self._mp_base_url
        try:
            from app.core.config import settings
            domain = str(getattr(settings, "APP_DOMAIN", "") or "").strip().rstrip("/")
            if domain.startswith(("http://", "https://")) and "localhost" not in domain \
                    and "127.0.0.1" not in domain:
                return domain
        except Exception:
            pass
        return ""

    def _record_notice(self, title: str, note: str) -> None:
        """在确认中心里留一条「提示记录」：只出现在历史里，不作为待确认卡片。

        做法：create（notify=false）后立即 answer，中心侧即为一条已完成记录。
        """
        if not self._center_url:
            return
        try:
            task_id = self._create_center_task({
                "title": title[:64],
                "sub_title": note[:512],
                "style": "text",
                "options": [{"key": "notice", "text": note[:128], "desc": ""}],
                "timeout": 600,
                "notify": False,
            })
            if not task_id:
                return
            params = {}
            if self._center_token:
                params["token"] = self._center_token
            RequestUtils(headers={"Content-Type": "application/json"}).post_res(
                f"{self._center_url}/api/v1/task/answer",
                json={"task_id": task_id, "key": "notice", "by": "system"},
                params=params or None,
            )
        except Exception as e:  # noqa: BLE001
            logger.warn(f"TaskConfirmBridge 写入提示记录失败：{e}")

    def _notify_search_empty(self, stage_label: str, keyword: str, detail: str = "") -> None:
        """阶段 1/2 搜索为 0 条或搜索失败 → 推一条通知，提醒用户去 MP 主页查看。

        三路反馈：①内置直推（bark/企微/gotify，受 notify_enabled 控制）；
        ②MP 原生通知播报（受 result_notify 控制）；③确认中心留一条提示记录。
        """
        lines = [f"⚠️ {stage_label}无结果：{keyword}", f"{stage_label}：{keyword}"]
        if detail:
            lines.append(detail)
        mp_url = self._mp_url()
        if mp_url:
            lines.append("请登录 MP 主页查看站点/索引器是否正常：")
            lines.append(f"👉 {mp_url}")
        else:
            lines.append("请登录 MP 主页查看站点/索引器是否正常。")
        text = "\n".join(lines)
        if self._notify_enabled:
            threading.Thread(target=self._send_notify_safe, args=(text,), daemon=True).start()
        else:
            logger.info("TaskConfirmBridge 通知总开关关闭，搜索无结果提示未推送")
        self._notify_result(f"⚠️ {stage_label}无结果：{keyword}",
                            "\n".join(lines[1:]))
        logger.info(f"TaskConfirmBridge {stage_label}无结果：{keyword}（{detail or '无补充信息'}）")
        self._record_notice(f"⚠️ {stage_label}无结果：{keyword}",
                            detail or f"未搜索到与「{keyword}」相关的结果")

    def _send_notify_safe(self, text: str, pic: str = "") -> None:
        """后台线程推送通知：失败只记日志，不阻塞主流程。"""
        try:
            self._send_notify(text, pic)
        except Exception as e:  # noqa: BLE001
            logger.warn(f"TaskConfirmBridge 通知发送失败：{e}")

    # ------------------------------------------------------------------ 阶段 2：资源搜索
    def _create_center_task(self, payload: Dict[str, Any]) -> Optional[str]:
        """向确认中心创建任务，成功返回 task_id。"""
        params = {}
        if self._center_token:
            params["token"] = self._center_token
        resp = RequestUtils(headers={"Content-Type": "application/json"}).post_res(
            f"{self._center_url}/api/v1/task/create", json=payload, params=params or None,
        )
        if resp is None or resp.status_code != 200:
            logger.error(f"TaskConfirmBridge 确认中心创建任务失败（HTTP {getattr(resp, 'status_code', 'None')}）")
            return None
        try:
            data = resp.json()
        except Exception:
            return None
        task_id = str(data.get("task_id") or "")
        return task_id or None

    def _run_torrent_stage(self, job: Dict[str, Any], keyword: str, media, media_text: str):
        """阶段 2：按 media_source/media_id 搜索资源 → 编号推卡片 → 等用户选择下载即停。"""
        m_title = str(media.title or keyword)
        m_source = str(getattr(media, "media_source", "") or "")
        m_id = str(getattr(media, "media_id", "") or "")
        logger.info(
            f"TaskConfirmBridge 阶段 2 资源搜索：{media_text}（{m_source}:{m_id}）"
        )
        if not m_id:
            job["state"] = "error"
            self._append_history(job, "选中媒体缺少 media_id，无法搜索资源")
            self._notify_search_empty("阶段 2 资源搜索", m_title,
                                      "失败：选中媒体缺少 media_id，无法搜索站点资源")
            return
        self._notify_result(
            f"🔍 正在搜索资源：{m_title}",
            f"{media_text}\n数据源：{m_source}:{m_id}\n搜索完成后将推送资源选择卡片",
        )
        try:
            msource = media.media_source
            try:
                msource = MediaSource(msource)
            except Exception:
                pass
            contexts = SearchChain().search_by_id(
                media_source=msource,
                media_id=m_id,
                mtype=media.type,
            ) or []
        except Exception as e:
            logger.error(f"TaskConfirmBridge 资源搜索异常：{e}")
            job["state"] = "error"
            self._append_history(job, f"资源搜索异常：{e}")
            self._notify_search_empty("阶段 2 资源搜索", m_title, f"搜索失败：{e}")
            return
        if not contexts:
            job["state"] = "no_torrents"
            self._append_history(job, "资源搜索无结果")
            self._notify_search_empty("阶段 2 资源搜索", m_title,
                                      "结果：0 条（站点未返回任何资源）")
            return

        contexts.sort(key=self._ctx_seeders, reverse=True)
        contexts = contexts[: self._max_options]
        options: List[Dict[str, Any]] = []
        torrent_map: Dict[str, Any] = {}
        idx = 0
        for ctx in contexts:
            ti = getattr(ctx, "torrent_info", None)
            if ti is None:
                continue
            idx += 1
            key = f"t{idx}"
            options.append({
                "key": key,
                "text": self._torrent_text(idx, ti),
                "desc": self._torrent_desc(ti),
            })
            torrent_map[key] = ctx
        if not options:
            job["state"] = "no_torrents"
            self._append_history(job, "资源搜索结果均为空种子信息")
            self._notify_search_empty("阶段 2 资源搜索", m_title,
                                      "结果：0 条有效资源（站点返回的种子信息为空）")
            return

        title = f"资源选择：{m_title}（{media.year or '未知年份'}）"[:64]
        sub_title = "阶段 2/3 · TaskConfirmBridge · 请选择要下载的资源"
        payload = {
            "title": title,
            "sub_title": sub_title,
            "style": "text",
            "options": options,
            "timeout": self._task_timeout,
            "notify": self._send_center_notify,
        }
        task_id2 = self._create_center_task(payload)
        if not task_id2:
            job["state"] = "error"
            self._append_history(job, "确认中心创建资源选择任务失败")
            self._notify_result(f"❌ 资源卡片推送失败：{m_title}", f"任务 {job.get('task_id')}")
            return
        if self._builtin_card_notify:
            self._notify_card_builtin(title, sub_title)

        job2 = {
            "task_id": task_id2,
            "keyword": keyword,
            "state": "pending",
            "stage": 2,
            "created_at": time.time(),
            "count": len(options),
            "selected": f"{m_title}（{media.year or '未知年份'}）",
        }
        with self._lock:
            self._jobs[task_id2] = job2
        logger.info(
            f"TaskConfirmBridge 已创建资源选择任务 {task_id2}（{len(options)} 个资源，按做种数排序截取）"
        )

        # 等待用户选择资源 → 进入阶段 3（下载）
        final = self._poll_final(task_id2)
        with self._lock:
            job2 = self._jobs.get(task_id2, job2)
        if not final or final.get("state") != "done":
            job2["state"] = "expired" if (final or {}).get("state") == "expired" else "timeout"
            self._append_history(job2, "用户未选择资源或超时")
            self._notify_result(f"⌛ 资源选择超时：{m_title}", f"任务 {task_id2}")
            return
        option_key = str(final.get("option") or "")
        torrent_text = str(final.get("option_text") or "")
        ctx = torrent_map.get(option_key)
        job2["state"] = "picked"
        job2["selected"] = torrent_text
        if ctx is None:
            self._append_history(job2, "选中的资源不在本进程缓存中（可能重启过）")
            return
        logger.info(f"TaskConfirmBridge 阶段 2 完成，用户选定资源：{torrent_text}")
        self._append_history(job2, "用户已选定资源，进入阶段 3 下载")
        self._run_download_stage(job2, ctx, torrent_text)

    # ------------------------------------------------------------------ 阶段 3：下载
    def _list_downloaders(self) -> List[Dict[str, Any]]:
        """列出 MP 中已启用的下载器（含默认标记）。"""
        items: List[Dict[str, Any]] = []
        try:
            for conf in (ServiceConfigHelper.get_downloader_configs() or []):
                if not getattr(conf, "enabled", False):
                    continue
                name = str(getattr(conf, "name", "") or "").strip()
                if not name:
                    continue
                items.append({
                    "name": name,
                    "type": str(getattr(conf, "type", "") or ""),
                    "default": bool(getattr(conf, "default", False)),
                })
        except Exception as e:
            logger.warn(f"TaskConfirmBridge 读取下载器配置失败：{e}")
        return items

    def _default_downloader_name(self) -> str:
        """MP 默认下载器名称（default=True 优先，否则第一个已启用）。"""
        items = self._list_downloaders()
        for it in items:
            if it.get("default"):
                return it["name"]
        return items[0]["name"] if items else ""

    def _run_download_stage(self, job: Dict[str, Any], ctx, torrent_text: str):
        """阶段 3：自动用默认下载器下载；关闭自动则先让用户选下载器。"""
        if self._auto_downloader:
            name = self._default_downloader_name()
            logger.info(
                f"TaskConfirmBridge 阶段 3 自动下载（默认下载器：{name or '由 MP 决定'}）：{torrent_text}"
            )
            self._do_download(job, ctx, torrent_text, None)
            return

        items = self._list_downloaders()
        if not items:
            self._append_history(job, "未找到已启用的下载器，回退默认下载器")
            self._notify_result(
                "⚠️ 未找到已启用的下载器，改用默认下载器",
                f"资源：{torrent_text}",
            )
            self._do_download(job, ctx, torrent_text, None)
            return

        options: List[Dict[str, Any]] = []
        name_map: Dict[str, str] = {}
        for i, it in enumerate(items, start=1):
            key = f"d{i}"
            tag = "（默认）" if it.get("default") else ""
            options.append({
                "key": key,
                "text": f"{i}. {it['name']}{tag}｜{it.get('type') or '未知类型'}",
                "desc": f"下载器：{it['name']}（{it.get('type') or '未知类型'}）{tag}",
            })
            name_map[key] = it["name"]

        title = f"下载器选择：{self._clean_torrent_title(torrent_text)}"[:64]
        sub_title = "阶段 3/3 · TaskConfirmBridge · 请选择下载器"
        payload = {
            "title": title,
            "sub_title": sub_title,
            "style": "text",
            "options": options,
            "timeout": self._task_timeout,
            "notify": self._send_center_notify,
        }
        task_id3 = self._create_center_task(payload)
        if not task_id3:
            self._append_history(job, "确认中心创建下载器选择任务失败，回退默认下载器")
            self._do_download(job, ctx, torrent_text, None)
            return
        if self._builtin_card_notify:
            self._notify_card_builtin(title, sub_title)

        job3 = {
            "task_id": task_id3,
            "keyword": job.get("keyword"),
            "state": "pending",
            "stage": 3,
            "created_at": time.time(),
            "count": len(options),
            "selected": torrent_text[:64],
        }
        with self._lock:
            self._jobs[task_id3] = job3
        logger.info(f"TaskConfirmBridge 已创建下载器选择任务 {task_id3}（{len(options)} 个下载器）")

        final = self._poll_final(task_id3)
        with self._lock:
            job3 = self._jobs.get(task_id3, job3)
        if not final or final.get("state") != "done":
            job3["state"] = "expired" if (final or {}).get("state") == "expired" else "timeout"
            self._append_history(job3, "用户未选择下载器或超时")
            self._notify_result(f"⌛ 下载器选择超时：{torrent_text[:40]}", f"任务 {task_id3}")
            return
        picked_name = name_map.get(str(final.get("option") or ""))
        if not picked_name:
            job3["state"] = "error"
            self._append_history(job3, "选中的下载器不在本进程缓存中")
            return
        job3["state"] = "picked"
        job3["selected"] = picked_name
        self._append_history(job3, f"用户已选定下载器：{picked_name}")
        self._do_download(job3, ctx, torrent_text, picked_name)

    def _do_download(self, job: Dict[str, Any], ctx, torrent_text: str, downloader: Optional[str]):
        """调用 MP 下载链把选中的资源加入下载器。"""
        try:
            did = DownloadChain().download_single(
                context=ctx,
                downloader=downloader or None,
                source="TaskConfirmBridge",
            )
        except Exception as e:
            logger.error(f"TaskConfirmBridge 添加下载异常：{e}")
            job["state"] = "download_failed"
            self._append_history(job, f"添加下载异常：{e}")
            self._notify_result(f"❌ 添加下载失败：{torrent_text[:40]}", f"{e}")
            return
        used = downloader or self._default_downloader_name() or "默认下载器"
        ti = getattr(ctx, "torrent_info", None)
        if did:
            job["state"] = "downloaded"
            job["download_id"] = str(did)
            self._append_history(job, f"已添加下载（{used}）hash={did}")
            logger.info(f"TaskConfirmBridge 已添加下载：{used} hash={did}｜{torrent_text}")
            self._notify_result(
                f"⬇️ 已开始下载（{used}）：{self._torrent_short_title(ti)}",
                f"{torrent_text}\n下载器：{used}\n任务：{job.get('task_id')}",
            )
        else:
            job["state"] = "download_failed"
            self._append_history(job, f"添加下载失败（{used}）")
            self._notify_result(
                f"❌ 添加下载失败（{used}）：{self._torrent_short_title(ti)}",
                f"{torrent_text}\n任务：{job.get('task_id')}",
            )

    @staticmethod
    def _clean_torrent_title(torrent_text: str) -> str:
        """把「1. 标题｜大小｜站点｜⬆做种」压成纯标题（去序号与后续字段）。"""
        first = str(torrent_text or "").split("｜")[0].strip()
        parts = first.split(". ", 1)
        if len(parts) == 2 and parts[0].isdigit():
            first = parts[1]
        return first[:40] or "未知资源"

    @staticmethod
    def _torrent_short_title(ti) -> str:
        title = str(getattr(ti, "title", "") or "未知资源")
        return title[:40]

    # ------------------------------------------------------------------ 通知（内置直推）
    def _notify_label(self) -> str:
        if not self._notify_enabled:
            return "已关闭"
        return f"{self._notify_channel} 开启"

    def _build_minimal_text(self, kind: str, title: str, url: str) -> str:
        now = time.strftime("%H:%M", time.localtime())
        tpl = self._notify_minimal_text
        for holder, val in (("{time}", now), ("{kind}", kind),
                            ("{title}", title), ("{url}", url)):
            tpl = tpl.replace(holder, val)
        return tpl.strip()

    def _notify_card_builtin(self, title: str, sub_title: str = "") -> None:
        """卡片通知由插件内置直推（可选：builtin_card_notify）。

        默认关闭——卡片在外部确认中心，通知由中心推送即可；开启后可用插件自己的
        通知渠道推（此时建议把「让确认中心推送候选通知」关掉，避免重复）。
        """
        if not self._notify_enabled:
            return
        url = f"{self._center_url}/ui/pending" if self._center_url else ""
        if self._notify_minimal:
            text = self._build_minimal_text("MP确认", title, url)
        else:
            lines = [f"【MP 任务确认】{title}"]
            if sub_title:
                lines.append(sub_title)
            lines.append(f"有效期 {_fmt_duration(self._task_timeout)}")
            lines.append("请选择：")
            if url:
                lines.append(f"👉 {url}")
            text = "\n".join(lines)
        pic = self._notify_pic if not (self._notify_minimal and not self._notify_minimal_pic) else ""
        threading.Thread(target=self._send_notify, args=(text, pic), daemon=True).start()

    def _send_notify(self, text: str, pic: str = ""):
        """内置通知直推（移植 bark-we-go.sh v1.4 逻辑，无脚本依赖）。

        约定与脚本一致：正文第一行视为标题；单条消息截断到 1800 字符；
        图片 URL 仅 bark 渠道生效；channel=all 时三渠道全发。
        """
        import httpx  # MP 运行时自带

        text = (text or "").strip()
        if len(text) > 1800:
            text = text[:1797] + "..."
        first, _, rest = text.partition("\n")
        title = first.strip() or "通知"
        body = rest.strip() or first.strip()

        channel = (self._notify_channel or "wecom").strip().lower()
        channels = ["bark", "wecom", "gotify"] if channel == "all" else [channel]
        sent: List[str] = []
        failed: List[str] = []
        for ch in channels:
            try:
                if ch == "bark":
                    self._push_bark(httpx, title, body, pic)
                elif ch == "wecom":
                    self._push_wecom(httpx, text)
                elif ch == "gotify":
                    self._push_gotify(httpx, title, body)
                else:
                    failed.append(f"{ch}:未知渠道")
                    continue
                sent.append(ch)
            except Exception as e:  # noqa: BLE001
                failed.append(f"{ch}:{e}")
        if sent:
            logger.info(f"TaskConfirmBridge 通知已发送（{'/'.join(sent)}）：{title[:60]}")
        if failed:
            logger.warn(f"TaskConfirmBridge 通知失败：{'；'.join(failed)}")

    def _push_bark(self, httpx, title: str, body: str, pic: str = ""):
        if not self._bark_key:
            raise RuntimeError("未配置 Bark DeviceKey")
        server = (self._bark_server or DEFAULT_BARK_SERVER).rstrip("/")
        payload: Dict[str, Any] = {"title": title, "body": body}
        if pic:
            payload["image"] = pic
        r = httpx.post(f"{server}/{self._bark_key}", json=payload,
                       timeout=self._notify_timeout)
        r.raise_for_status()

    def _push_wecom(self, httpx, text: str):
        if not self._wecom_webhook:
            raise RuntimeError("未配置企业微信机器人 Webhook")
        r = httpx.post(self._wecom_webhook,
                       json={"msgtype": "text", "text": {"content": text[:2000]}},
                       timeout=self._notify_timeout)
        r.raise_for_status()
        data = r.json() if r.content else {}
        if isinstance(data, dict) and data.get("errcode") not in (0, None):
            raise RuntimeError(f"企微 errcode={data.get('errcode')} {data.get('errmsg') or ''}")

    def _push_gotify(self, httpx, title: str, body: str):
        if not self._gotify_url or not self._gotify_token:
            raise RuntimeError("未配置 Gotify 地址/Token")
        url = f"{self._gotify_url.rstrip('/')}/message"
        r = httpx.post(url, params={"token": self._gotify_token},
                       json={"title": title, "message": body},
                       timeout=self._notify_timeout)
        r.raise_for_status()

    def _notify_result(self, title: str, text: str):
        """结果播报经 MP 原生通知。"""
        if not self._result_notify:
            return
        try:
            self.post_message(mtype=NotificationType.Plugin, title=title, text=text)
        except Exception as e:
            logger.warn(f"TaskConfirmBridge 发送结果通知失败：{e}")

    # ------------------------------------------------------------------ 工具
    @staticmethod
    def _ctx_seeders(ctx) -> int:
        ti = getattr(ctx, "torrent_info", None)
        try:
            return int(getattr(ti, "seeders", 0) or 0)
        except Exception:
            return 0

    @staticmethod
    def _fmt_size(num) -> str:
        try:
            n = float(num or 0)
        except Exception:
            return "未知"
        if n >= 1024 ** 4:
            return f"{n / 1024 ** 4:.2f} TB"
        if n >= 1024 ** 3:
            return f"{n / 1024 ** 3:.2f} GB"
        if n >= 1024 ** 2:
            return f"{n / 1024 ** 2:.1f} MB"
        return f"{n / 1024:.0f} KB"

    def _torrent_text(self, idx: int, ti) -> str:
        """单行展示：标题｜大小｜站点｜做种数（卡片模板不渲染换行）。"""
        title = str(getattr(ti, "title", "") or "未知标题")
        free = "｜🆓免费" if getattr(ti, "downloadvolumefactor", None) == 0 else ""
        text = (
            f"{idx}. {title}"
            f"｜{self._fmt_size(getattr(ti, 'size', 0))}"
            f"｜{getattr(ti, 'site_name', '') or '未知站点'}"
            f"｜⬆{getattr(ti, 'seeders', 0) or 0}{free}"
        )
        return text[:256]

    def _torrent_desc(self, ti) -> str:
        desc = str(getattr(ti, "description", "") or "").strip()
        labels = getattr(ti, "labels", None) or []
        pieces = []
        if labels:
            pieces.append("标签：" + " ".join(str(x) for x in labels[:6]))
        if desc:
            pieces.append(desc[:150])
        return " ｜ ".join(pieces)

    @staticmethod
    def _type_name(media) -> str:
        t = media.type
        if t == MediaType.MOVIE:
            return "电影"
        if t == MediaType.TV:
            return "电视剧"
        return str(t).split(".")[-1] if t else "未知"

    def _option_text(self, idx: int, media) -> str:
        try:
            vote = f"{media.vote_average:.1f}".rstrip("0").rstrip(".")
        except Exception:
            vote = ""
        pieces = [f"{idx}. {media.title}（{media.year or '未知年份'}）", self._type_name(media)]
        if vote:
            pieces.append(f"⭐{vote}")
        return "｜".join(pieces)[:128]

    @staticmethod
    def _option_desc(media) -> str:
        try:
            overview = (media.overview or "").strip()
        except Exception:
            overview = ""
        return overview[:200]

    def _append_history(self, job: Dict[str, Any], note: str):
        rec = {
            "task_id": job.get("task_id"),
            "keyword": job.get("keyword"),
            "state": job.get("state"),
            "selected": job.get("selected"),
            "note": note,
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        with self._lock:
            self._history.append(rec)
            self._history = self._history[-50:]
        try:
            self.save_data("history", self._history)
        except Exception:
            pass

    @staticmethod
    def _job_public(job: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "task_id": job.get("task_id"),
            "keyword": job.get("keyword"),
            "state": job.get("state"),
            "stage": job.get("stage"),
            "count": job.get("count"),
            "selected": job.get("selected"),
            "created_at": job.get("created_at"),
        }

    # ------------------------------------------------------------------ 页面
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        def col(model: str, label: str, md: int = 6, placeholder: str = "") -> dict:
            props = {"model": model, "label": label}
            if placeholder:
                props["placeholder"] = placeholder
            return {"component": "VCol", "props": {"cols": 12, "md": md},
                    "content": [{"component": "VTextField", "props": props}]}

        def switch(model: str, label: str, md: int = 6) -> dict:
            return {"component": "VCol", "props": {"cols": 12, "md": md},
                    "content": [{"component": "VSwitch", "props": {"model": model, "label": label}}]}

        divider = {"component": "VCol", "props": {"cols": 12, "md": 12},
                   "content": [{"component": "VDivider", "props": {"class": "my-2"}}]}

        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        switch("enabled", "启用插件"),
                        switch("onlyonce", "立即运行一次（测试搜索：西游记）"),
                        col("center_url", "任务确认中心地址", 6, "http://192.168.5.126:8005"),
                        col("center_token", "确认中心 api_token（未配置可留空）", 6),
                        col("mp_base_url", "MP 主页地址（搜索无结果时通知里引导查看，留空自动取 MP 对外地址）", 12,
                            "http://192.168.5.126:3005"),
                        col("max_options", "候选/资源数上限", 3),
                        col("task_timeout", "确认任务有效期（秒）", 3),
                        col("wait_total", "整体收割超时（秒）", 3),
                        col("poll_interval", "轮询间隔（秒）", 3),
                        switch("send_center_notify", "让确认中心推送候选通知"),
                        switch("builtin_card_notify", "卡片通知改由插件内置直推（关闭则仍由确认中心推送）"),
                        switch("torrent_mode", "选择媒体后搜索资源（二级选择，关闭则直接订阅）"),
                        switch("auto_downloader", "阶段 3 自动调用默认下载器（关闭则推送下载器选择卡片）"),
                        switch("check_library", "检查媒体库已存在（存在则通知并终止）"),
                        switch("result_notify", "结果经 MP 通知播报"),
                        divider,
                        switch("notify_enabled", "启用插件内置通知（搜索无结果/媒体库已存在等）"),
                        col("notify_channel", "通知渠道（bark/wecom/gotify/all）", 3, "wecom"),
                        col("notify_timeout", "通知超时（秒）", 3, "15"),
                        col("notify_pic", "通知图片 URL（仅 bark 生效）", 6),
                        divider,
                        col("bark_server", "Bark 服务器", 4, "https://api.day.app"),
                        col("bark_key", "Bark DeviceKey", 8),
                        col("wecom_webhook", "企业微信机器人 Webhook（完整 URL）", 12),
                        col("gotify_url", "Gotify 地址", 6, "http://192.168.5.70:18100"),
                        col("gotify_token", "Gotify Token", 6),
                        divider,
                        switch("notify_minimal", "极简模式（只发时间+一行提示）"),
                        switch("notify_minimal_pic", "极简模式仍附图片"),
                        col("notify_minimal_text", "极简正文模板（{time}{kind}{title}{url}）", 12),
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "center_url": "http://192.168.5.126:8005",
            "center_token": "",
            "mp_base_url": "",
            "max_options": 8,
            "task_timeout": 3600,
            "wait_total": 21600,
            "poll_interval": 5,
            "send_center_notify": True,
            "builtin_card_notify": False,
            "torrent_mode": True,
            "auto_downloader": True,
            "check_library": True,
            "result_notify": True,
            "notify_enabled": True,
            "notify_channel": "wecom",
            "notify_pic": "",
            "notify_timeout": 15,
            "bark_server": DEFAULT_BARK_SERVER,
            "bark_key": "",
            "wecom_webhook": "",
            "gotify_url": "",
            "gotify_token": "",
            "notify_minimal": True,
            "notify_minimal_pic": False,
            "notify_minimal_text": DEFAULT_MINIMAL_TEXT,
        }

    def get_page(self) -> List[dict]:
        with self._lock:
            history = list(reversed(self._history[-20:]))
        lines = "\n".join(
            f"[{h.get('time')}] {h.get('keyword')} → {h.get('state')}"
            + (f"（{h.get('selected')}）" if h.get("selected") else "")
            + (f"｜{h.get('note')}" if h.get("note") else "")
            for h in history
        ) or "暂无记录"
        head = (f"确认中心：{self._center_url or '（未配置）'}\n"
                f"内置通知：{self._notify_label()}｜"
                f"卡片通知：{'插件内置直推' if self._builtin_card_notify else '由确认中心推送'}\n"
                f"媒体库检查：{'开' if self._check_library else '关'}\n\n")
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": (
                        head +
                        "用法：在任务确认中心指令框输入「mp 搜索词」→ 本插件搜索媒体 → "
                        "候选推回确认中心（阶段 1）→ 选中媒体先查媒体库（已存在则通知并终止），"
                        "否则自动搜索站点资源并推送资源卡片（阶段 2，含标题/大小/站点/做种数）→ "
                        "选定资源后自动用默认下载器下载（阶段 3，关闭自动开关则先推送下载器选择卡片）。\n"
                        "阶段 1/2 搜索为 0 条或搜索失败时会主动推送通知（带 MP 主页入口），"
                        "并在确认中心留一条提示记录，不再静默。\n\n最近记录：\n" + lines
                    ),
                },
            }
        ]

    def stop_service(self):
        self._stopping = True
