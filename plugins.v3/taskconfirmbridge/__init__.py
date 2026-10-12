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


class TaskConfirmBridge(_PluginBase):
    """
    任务确认中心桥接：确认中心发起「mp 搜索词」→ 本插件搜索媒体 →
    候选列表作为确认任务推回中心 → 用户在中心 H5 选择媒体（阶段 1）→
    按 media_source/media_id 搜索站点资源，编号展示标题/大小/站点/做种数（阶段 2）→
    选定资源后自动用 MP 默认下载器下载（阶段 3，可关闭改为推送下载器选择卡片）。
    """

    plugin_name = "任务确认桥接"
    plugin_desc = "把任务确认中心的「mp 搜索词」转成 MP 媒体搜索，候选推回确认中心选择后再搜索站点资源，二次选定后自动调用默认下载器下载。"
    plugin_icon = "https://raw.githubusercontent.com/huiyi977/MoviePilot-TaskConfirmPlugins/main/icons/taskconfirmbridge.png"
    plugin_version = "1.2.0"
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
        return {"success": True, "jobs": jobs, "history": history}

    # ------------------------------------------------------------------ 核心流程
    def _run_search_job(self, keyword: str) -> Dict[str, Any]:
        """同步执行：搜索 → 建确认任务 → 起后台线程收割。"""
        logger.info(f"TaskConfirmBridge 收到搜索词：{keyword}")
        try:
            _meta, medias = MediaChain().search(keyword, limit=20)
        except Exception as e:
            logger.error(f"TaskConfirmBridge 搜索失败：{e}")
            return {"success": False, "message": f"搜索失败：{e}"}
        if not medias:
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

        payload = {
            "title": f"MP 搜索：{keyword}"[:64],
            "sub_title": "TaskConfirmBridge · 请选择要订阅的条目",
            "style": "text",
            "options": options,
            "timeout": self._task_timeout,
            "notify": self._send_center_notify,
        }
        params = {}
        if self._center_token:
            params["token"] = self._center_token
        resp = RequestUtils(headers={"Content-Type": "application/json"}).post_res(
            f"{self._center_url}/api/v1/task/create", json=payload, params=params or None,
        )
        if resp is None or resp.status_code != 200:
            msg = f"确认中心创建任务失败（HTTP {getattr(resp, 'status_code', 'None')}）"
            logger.error(f"TaskConfirmBridge {msg}")
            return {"success": False, "message": msg}
        try:
            data = resp.json()
        except Exception:
            return {"success": False, "message": "确认中心返回内容无法解析"}
        task_id = str(data.get("task_id") or "")
        if not task_id:
            return {"success": False, "message": f"确认中心未返回 task_id：{data}"}

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
            self._notify_result(f"❌ 无法搜索资源：{m_title}", f"选中媒体缺少 media_id\n{media_text}")
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
            self._notify_result(f"❌ 资源搜索失败：{m_title}", f"任务 {job.get('task_id')}\n{e}")
            return
        if not contexts:
            job["state"] = "no_torrents"
            self._append_history(job, "资源搜索无结果")
            self._notify_result(f"❌ 未搜索到资源：{m_title}", f"任务 {job.get('task_id')}")
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
            self._notify_result(f"❌ 未搜索到有效资源：{m_title}", f"任务 {job.get('task_id')}")
            return

        payload = {
            "title": f"资源选择：{m_title}（{media.year or '未知年份'}）"[:64],
            "sub_title": "TaskConfirmBridge · 阶段 2/3 · 请选择要下载的资源",
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

        payload = {
            "title": f"下载器选择：{torrent_text.split('｜')[0][:28]}",
            "sub_title": "TaskConfirmBridge · 阶段 3/3 · 请选择下载器",
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
    def _torrent_short_title(ti) -> str:
        title = str(getattr(ti, "title", "") or "未知资源")
        return title[:40]

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

    def _notify_result(self, title: str, text: str):
        if not self._result_notify:
            return
        try:
            self.post_message(mtype=NotificationType.Plugin, title=title, text=text)
        except Exception as e:
            logger.warn(f"TaskConfirmBridge 发送结果通知失败：{e}")

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
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即运行一次（测试搜索：西游记）"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "center_url",
                                            "label": "任务确认中心地址",
                                            "placeholder": "http://192.168.5.126:8005",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "center_token",
                                            "label": "确认中心 api_token（未配置可留空）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "max_options", "label": "候选数上限"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "task_timeout", "label": "确认任务有效期（秒）"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "wait_total", "label": "整体收割超时（秒）"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "poll_interval", "label": "轮询间隔（秒）"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "send_center_notify", "label": "让确认中心推送候选通知"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "torrent_mode", "label": "选择媒体后搜索资源（二级选择，关闭则直接订阅）"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "auto_downloader", "label": "阶段 3 自动调用默认下载器（关闭则推送下载器选择卡片）"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "result_notify", "label": "结果经 MP 通知播报"},
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "center_url": "http://192.168.5.126:8005",
            "center_token": "",
            "max_options": 8,
            "task_timeout": 3600,
            "wait_total": 21600,
            "poll_interval": 5,
            "send_center_notify": True,
            "result_notify": True,
            "torrent_mode": True,
            "auto_downloader": True,
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
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": (
                        "用法：在任务确认中心指令框输入「mp 搜索词」→ 本插件搜索媒体 → "
                        "候选推回确认中心（阶段 1）→ 选择媒体后自动搜索站点资源并推送资源卡片 "
                        "（阶段 2，含标题/大小/站点/做种数）→ 选定资源后自动用默认下载器下载 "
                        "（阶段 3，关闭自动开关则先推送下载器选择卡片）。\n\n最近记录：\n" + lines
                    ),
                },
            }
        ]

    def stop_service(self):
        self._stopping = True
