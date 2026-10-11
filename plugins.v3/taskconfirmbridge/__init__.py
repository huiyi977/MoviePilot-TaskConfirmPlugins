from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Request

from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.sdk.plugin import _PluginBase
from app.schemas.types import MediaType, NotificationType
from app.sdk.logging import logger
from app.utils.http import RequestUtils


class TaskConfirmBridge(_PluginBase):
    """
    任务确认中心桥接：确认中心发起「mp 搜索词」→ 本插件搜索媒体 →
    候选列表作为确认任务推回中心 → 用户在中心 H5 选择 → 本插件收割结果并添加订阅。
    """

    plugin_name = "任务确认桥接"
    plugin_desc = "把任务确认中心的「mp 搜索词」转成 MP 媒体搜索，候选推回确认中心选择后自动添加订阅。"
    plugin_icon = "https://raw.githubusercontent.com/huiyi977/MoviePilot-TaskConfirmPlugins/main/icons/taskconfirmbridge.png"
    plugin_version = "1.0.1"
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

    def __init__(self):
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

    def _wait_and_subscribe(self, task_id: str, keyword: str, key_map: Dict[str, Any]):
        """后台线程：长轮询中心任务状态，确认后添加订阅。"""
        base = self._center_url
        params: Dict[str, Any] = {"wait": 25}
        if self._center_token:
            params["token"] = self._center_token
        deadline = time.time() + self._wait_total
        final: Optional[Dict[str, Any]] = None
        time.sleep(2)
        while time.time() < deadline:
            if self._stopping:
                return
            try:
                resp = RequestUtils().get_res(f"{base}/api/v1/task/status/{task_id}", params=params)
                data = resp.json() if resp is not None else {}
                state = str(data.get("state") or "")
            except Exception as e:
                logger.warn(f"TaskConfirmBridge 轮询任务 {task_id} 异常：{e}")
                time.sleep(self._poll_interval)
                continue
            if state == "done":
                final = data
                break
            if state in ("expired", "unknown"):
                final = data
                break
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

    # ------------------------------------------------------------------ 工具
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
                                        "props": {"model": "result_notify", "label": "订阅结果经 MP 通知播报"},
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
                        "候选推回确认中心 → 选择后自动添加订阅。\n\n最近记录：\n" + lines
                    ),
                },
            }
        ]

    def stop_service(self):
        self._stopping = True
