# -*- coding: utf-8 -*-
"""TaskConfirmMax —— 任务确认中心 Max（一体化：内置 Web 确认服务 + MoviePilot 桥接）

把原先独立部署的「任务确认中心」（server.py）**内嵌进插件**：
插件启动时在容器内拉起一个 FastAPI 服务（默认 0.0.0.0:8005），
只保留「确认卡片」这一条业务线，指令框**直接输入搜索词**，专配 MoviePilot。

链路：H5 指令框 `搜索词` → 插件搜索媒体 → 候选卡片（阶段 1）
      → 选媒体 → 站点资源卡片（阶段 2）→ 选资源 → 自动/指定下载器下载（阶段 3）。

与原 server.py 的差异（精简）：
  · 去掉「电影海报 2:3 截图（crop）」与「视频截图多选（frame）」两条业务线
  · 去掉图床上传 / 图片代理 / 设备切换，指令框只接收搜索词（无需前缀）
  · 去掉了与外部确认中心之间的 HTTP 往返：任务在进程内直接创建与等待
  · 通知**内置直推**：bark / wecom（企微机器人）/ gotify，界面上直接填各渠道 Key 即可，
    不再依赖 bark-we-go.sh 等外部脚本（凭证只存在插件配置里，不写死在代码中）
  · 阶段 1 选中媒体后先查媒体库（Emby/Jellyfin/Plex…）：已存在则通知并终止，不再搜资源/下载
    （开关 `check_library`，默认开启）
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.search import SearchChain
from app.chain.subscribe import SubscribeChain
from app.schemas.types import MediaSource, MediaType, NotificationType
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.sdk.services import ServiceConfigHelper


# =====================================================================
#  内嵌确认中心（原 server.py 的精简版，仅保留「确认卡片」一条业务线）
# =====================================================================

MAX_FIELDS = 15
MAX_BUTTONS = 8          # card 模式选项上限
MAX_OPTIONS_TEXT = 100   # text 模式选项上限

DEFAULT_MINIMAL_TEXT = "【待处理确认】\\n{time} 你有一条待处理确认任务"

DEFAULT_BARK_SERVER = "https://api.day.app"

HELP_TEXT = """⚡ 任务确认中心 Max - 指令帮助

【MP 媒体搜索】
  直接输入搜索词  例如：蜘蛛侠
    说明：把搜索词交给 MoviePilot 搜索媒体 → 候选列表推回本页（阶段 1）
          → 选中媒体后自动搜索站点资源（阶段 2，含 标题/大小/站点/做种数）
          → 选中资源后自动调用下载器下载（阶段 3）。

【使用提示】
  · 输入搜索词后点「发送」或敲回车即可执行
  · 本版本为 MP 专配版，输入的内容一律当作搜索词处理
"""

PAGE_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">
<title>MP 任务确认</title>
<style>
:root{--bg:#f2f4f7;--card:#ffffff;--text:#1f2937;--sub:#6b7280;--line:#e5e7eb;--optbg:#f9fafb;--accent:#2563eb;--accent-soft:#eef2ff}
@media (prefers-color-scheme: dark){
  :root{--bg:#0f141a;--card:#171e27;--text:#e5e7eb;--sub:#9ca3af;--line:#2a3442;--optbg:#1d2634;--accent:#3b82f6;--accent-soft:#1e2a44}
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
body{font-family:system-ui,-apple-system,"PingFang SC","HarmonyOS Sans SC","Microsoft YaHei",sans-serif;background:var(--bg);color:var(--text);padding:18px 14px 40px}
.wrap{max-width:520px;margin:0 auto}
header{display:flex;justify-content:space-between;align-items:flex-start}
header h1{font-size:20px;font-weight:700}
header .count{margin-top:6px;font-size:13px;color:var(--sub)}
header .count b{color:var(--accent)}
.header-btns{display:flex;gap:6px;align-items:center}
.icon-btn{padding:6px 10px;border:1px solid var(--line);border-radius:10px;background:var(--card);color:var(--text);font-size:15px;cursor:pointer;transition:background .15s,border-color .15s;display:inline-flex;align-items:center;justify-content:center;white-space:nowrap}
.icon-btn:active{opacity:.8}
.cmd-card{background:var(--card);border-radius:16px;padding:12px 14px;margin-top:12px;box-shadow:0 1px 3px rgba(0,0,0,.07)}
.cmd-box{display:flex;gap:6px}
.cmd-box input{flex:1;min-width:0;padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--optbg);color:var(--text);font-size:14px;outline:none}
.cmd-box input:focus{border-color:var(--accent)}
.cmd-box button{padding:10px 14px;border:none;border-radius:10px;background:var(--accent);color:#fff;font-weight:700;font-size:14px;cursor:pointer;white-space:nowrap;transition:opacity .15s}
.cmd-box button:active{opacity:.8}
.cmd-box button:disabled{opacity:.5;cursor:not-allowed}
.card{background:var(--card);border-radius:16px;padding:16px 16px 12px;margin-top:14px;box-shadow:0 1px 3px rgba(0,0,0,.07);transition:opacity .45s ease,transform .45s ease}
.card.done{opacity:0;transform:translateY(10px) scale(.97);pointer-events:none}
.card h2{font-size:17px;font-weight:700;line-height:1.4}
.card-head{display:flex;align-items:flex-start;gap:10px}
.card-head h2{flex:1;min-width:0}
.btn-revoke{flex:0 0 auto;border:1px solid var(--line);background:transparent;color:var(--sub);
  font-size:12px;padding:4px 10px;border-radius:8px;cursor:pointer;white-space:nowrap;transition:all .15s}
.btn-revoke:active{opacity:.7}
.btn-revoke.armed{border-color:#ef4444;border-color:#f87171;color:#f87171;font-weight:600}
.card .sub{margin-top:6px;font-size:14px;color:var(--sub);line-height:1.55;white-space:pre-wrap;word-break:break-word}
.fields{margin-top:10px;border-top:1px dashed var(--line);padding-top:8px}
.fields .frow{display:flex;gap:10px;padding:4px 0;font-size:13px;line-height:1.5}
.fields .fk{flex:0 0 32%;color:var(--sub);word-break:break-all}
.fields .fv{flex:1;word-break:break-all}
.opts{margin-top:12px}
.opt{display:flex;align-items:center;gap:12px;width:100%;min-height:52px;padding:10px 14px;margin-top:10px;border:1px solid var(--line);border-radius:12px;background:var(--optbg);color:var(--text);text-align:left;cursor:pointer;transition:background .15s,border-color .15s,color .15s,opacity .15s}
.opt .num{flex:0 0 30px;height:30px;border-radius:50%;background:var(--accent-soft);color:var(--accent);font-size:14px;font-weight:700;display:flex;align-items:center;justify-content:center}
.opt .body{flex:1;min-width:0}
.opt .txt{display:block;font-size:16px;line-height:1.4;word-break:break-word}
.opt .desc{display:block;margin-top:2px;font-size:12.5px;color:var(--sub);word-break:break-word}
.opt:active{background:var(--accent-soft);border-color:var(--accent)}
.opt.chosen{background:var(--accent);border-color:var(--accent);color:#fff}
.opt.chosen .num{background:rgba(255,255,255,.22);color:#fff}
.opt.chosen .desc{color:rgba(255,255,255,.8)}
.opt:disabled{opacity:.55;cursor:default}
.opt.chosen:disabled{opacity:1}
.meta{margin-top:10px;font-size:12px;color:var(--sub);text-align:right}
#empty{display:none;margin-top:60px;text-align:center;color:var(--sub)}
#empty .emoji{font-size:44px}
#empty p{margin-top:10px;font-size:14px}
#toast{position:fixed;left:50%;bottom:36px;transform:translateX(-50%) translateY(20px);max-width:85%;padding:10px 18px;border-radius:12px;background:rgba(17,24,39,.92);color:#fff;font-size:14px;line-height:1.4;opacity:0;transition:all .3s;pointer-events:none;z-index:99;text-align:center;word-break:break-word}
#toast.show{opacity:1;transform:translateX(-50%) translateY(0)}

/* Modal 弹窗通用样式 */
.modal-overlay{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.5);z-index:100;align-items:center;justify-content:center;padding:16px}
.modal-overlay.show{display:flex}
.modal-box{background:var(--card);border-radius:16px;width:100%;max-width:480px;max-height:80vh;display:flex;flex-direction:column;box-shadow:0 10px 25px rgba(0,0,0,.2);overflow:hidden}
.modal-head{padding:14px 16px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center}
.modal-head h3{font-size:16px;font-weight:700}
.modal-close{font-size:18px;cursor:pointer;color:var(--sub);border:none;background:none;padding:0 6px;line-height:1}
.modal-body{padding:14px;overflow-y:auto;flex:1}

/* 历史记录条目 */
.hist-item{padding:10px 12px;border:1px solid var(--line);border-radius:10px;margin-bottom:8px;background:var(--optbg);font-size:13px}
.hist-item:last-child{margin-bottom:0}
.hist-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;gap:8px}
.hist-title{font-weight:500;font-size:14px;color:var(--text);word-break:break-word}
.badge{padding:2px 6px;border-radius:6px;font-size:11px;font-weight:500;white-space:nowrap}
.badge.done{background:#dcfce7;color:#15803d}
.badge.expired{background:#f3f4f6;color:#6b7280}
.badge.pending{background:#dbeafe;color:#1d4ed8}
.badge.error{background:#fee2e2;color:#b91c1c}
.hist-info{color:var(--sub);font-size:12px;line-height:1.4}
@media (prefers-color-scheme: dark){
  .badge.done{background:#14532d;color:#86efac}
  .badge.expired{background:#374151;color:#d1d5db}
  .badge.pending{background:#1e3a8a;color:#93c5fd}
  .badge.error{background:#7f1d1d;color:#fca5a5}
}
.modal-body pre{margin:0}

footer{margin-top:26px;text-align:center;font-size:12px;color:var(--sub)}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>📋 MP 任务确认</h1>
      <div class="count">待确认任务：<b id="cnt">__COUNT__</b> 个 · 每 5 秒自动刷新</div>
    </div>
    <div class="header-btns">
      <button id="help-btn" class="icon-btn" title="指令帮助">❓</button>
      <button id="hist-btn" class="icon-btn" title="历史记录">📜</button>
    </div>
  </header>

  <!-- MP 搜索指令区域 -->
  <div class="cmd-card">
    <div class="cmd-box">
      <input type="text" id="cmd-input" placeholder="输入搜索词，如：蜘蛛侠">
      <button id="cmd-btn">发送</button>
    </div>
  </div>

  <div id="tasks" data-ids="__IDS__">__CARDS__</div>
  <div id="empty"><div class="emoji">🎉</div><p>暂无待确认任务</p></div>
  <footer>TaskConfirmMax v__VERSION__ · 端口 __PORT__ · 默认有效期 __TTL__ · 通知 __NOTIFY__</footer>
</div>

<!-- 指令帮助 Modal 弹窗 -->
<div id="help-modal" class="modal-overlay">
  <div class="modal-box">
    <div class="modal-head">
      <h3>💡 指令帮助</h3>
      <button id="help-close" class="modal-close">✕</button>
    </div>
    <div class="modal-body">
      <pre id="help-content" style="white-space:pre-wrap;word-break:break-word;font-family:inherit;font-size:13.5px;line-height:1.6;color:var(--text)">加载中...</pre>
    </div>
  </div>
</div>

<!-- 历史记录 Modal 弹窗 -->
<div id="hist-modal" class="modal-overlay">
  <div class="modal-box">
    <div class="modal-head">
      <h3>📜 历史与操作记录</h3>
      <button id="hist-close" class="modal-close">✕</button>
    </div>
    <div class="modal-body" id="hist-list">
      <div style="text-align:center;color:var(--sub);padding:20px 0">加载中...</div>
    </div>
  </div>
</div>

<div id="toast"></div>
<script>
(function(){
  var box=document.getElementById('tasks');
  var toastEl=document.getElementById('toast'),toastTimer=null;
  var CUR_IDS=box.dataset.ids;
  function toast(m){toastEl.textContent=m;toastEl.classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(function(){toastEl.classList.remove('show')},2500)}
  function refreshEmpty(){if(box.querySelectorAll('.card:not(.done)').length===0){document.getElementById('empty').style.display='block'}}
  function fadeOut(card){card.classList.add('done');setTimeout(function(){card.style.display='none';refreshEmpty()},480)}
  function dropId(id){CUR_IDS=CUR_IDS.split(',').filter(function(x){return x&&x!==id}).join(',')}
  function pageToken(){return new URLSearchParams(window.location.search).get('token')||''}

  // 指令发送（直接输入搜索词）
  var cmdBtn=document.getElementById('cmd-btn'), cmdInput=document.getElementById('cmd-input');
  function sendCmd(){
    var val=(cmdInput.value||'').trim();
    if(!val) return toast('请先输入搜索词，如：蜘蛛侠');
    cmdBtn.disabled=true;
    var token=pageToken();
    fetch('/api/v1/command',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({ cmd: val, token: token })
    })
    .then(function(r){return r.json()})
    .then(function(d){
      if(d.ok){toast('✅ '+(d.msg||'指令已成功执行'));cmdInput.value='';}
      else{toast('❌ '+(d.msg||d.detail||'处理失败'));}
    })
    .catch(function(err){toast('请求出错: '+err.message);})
    .finally(function(){cmdBtn.disabled=false;});
  }
  cmdBtn.addEventListener('click', sendCmd);
  cmdInput.addEventListener('keydown', function(e){if(e.key==='Enter') sendCmd();});

  // 指令帮助弹窗
  var helpBtn = document.getElementById('help-btn');
  var helpModal = document.getElementById('help-modal');
  var helpClose = document.getElementById('help-close');
  var helpContent = document.getElementById('help-content');

  helpBtn.addEventListener('click', function(){
    helpModal.classList.add('show');
    helpContent.textContent = '加载中...';
    var token = pageToken();
    fetch('/api/v1/command/help' + (token ? '?token=' + encodeURIComponent(token) : ''))
      .then(function(r){ return r.json(); })
      .then(function(d){
        helpContent.textContent = d.ok ? (d.content || '暂无帮助内容')
                                       : '加载失败: ' + (d.detail || '未知错误');
      })
      .catch(function(err){ helpContent.textContent = '请求出错: ' + err.message; });
  });
  helpClose.addEventListener('click', function(){ helpModal.classList.remove('show'); });
  helpModal.addEventListener('click', function(e){ if (e.target === helpModal) helpModal.classList.remove('show'); });

  // 历史记录弹窗
  var histBtn = document.getElementById('hist-btn');
  var histModal = document.getElementById('hist-modal');
  var histClose = document.getElementById('hist-close');
  var histList = document.getElementById('hist-list');

  function esc(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }
  function formatTime(ts){
    if(!ts) return '';
    var d = new Date(ts * 1000);
    var p = function(n){ return ('0' + n).slice(-2); };
    return p(d.getMonth()+1) + '-' + p(d.getDate()) + ' ' + p(d.getHours()) + ':' + p(d.getMinutes());
  }
  function histEmpty(msg){
    histList.innerHTML = '<div style="text-align:center;color:var(--sub);padding:20px 0">' + esc(msg) + '</div>';
  }

  function loadHistory(){
    histEmpty('加载中...');
    var token = pageToken();
    fetch('/api/v1/tasks?limit=50' + (token ? '&token=' + encodeURIComponent(token) : ''))
      .then(function(r){ return r.json(); })
      .then(function(j){
        var items = j.items || [];
        if(!items.length){ histEmpty('暂无历史记录'); return; }
        var htmlStr = '';
        items.forEach(function(t){
          var st = t.state || 'pending';
          var revoked = st === 'expired' && t.option === '__revoke__';
          var badgeText = st === 'done' ? '已完成'
                        : st === 'expired' ? (revoked ? '已撤回' : '已过期')
                        : st === 'error' ? '推送失败' : '待确认';
          var resultText = '';
          if(st === 'done' && t.option_text){
            resultText = '结果: ' + t.option_text + (t.clicked_by ? ' (' + t.clicked_by + ')' : '');
          }else if(st === 'expired'){
            resultText = revoked ? '被撤回，未确认' : '超时未确认';
          }else if(t.sub_title){
            resultText = t.sub_title;
          }
          htmlStr += '<div class="hist-item">'
            + '<div class="hist-top"><span class="hist-title">' + esc(t.title || '无标题') + '</span>'
            + '<span class="badge ' + esc(st) + '">' + esc(badgeText) + '</span></div>'
            + '<div class="hist-info">'
            + (resultText ? '<div>' + esc(resultText) + '</div>' : '')
            + '<div style="margin-top:2px;color:var(--sub);font-size:11px">'
            + esc(formatTime(t.clicked_at || t.created_at)) + '</div></div></div>';
        });
        histList.innerHTML = htmlStr;
      })
      .catch(function(err){ histEmpty('加载失败: ' + err.message); });
  }

  histBtn.addEventListener('click', function(){ histModal.classList.add('show'); loadHistory(); });
  histClose.addEventListener('click', function(){ histModal.classList.remove('show'); });
  histModal.addEventListener('click', function(e){ if (e.target === histModal) histModal.classList.remove('show'); });

  // 任务点选逻辑
  box.addEventListener('click',function(e){
    var btn=e.target.closest('.opt');if(!btn||btn.disabled)return;
    var card=btn.closest('.card');
    var opts=card.querySelectorAll('.opt');
    opts.forEach(function(b){b.disabled=true});
    btn.classList.add('chosen');

    var token=pageToken();

    fetch('/api/v1/task/answer',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({task_id:btn.dataset.task,key:btn.dataset.key,s:card.dataset.sig||'',token:token,by:'h5'})})
    .then(function(r){return r.json().catch(function(){throw new Error('HTTP '+r.status)})})
    .then(function(d){
      if(d.accepted){toast('已选择：'+d.option_text);dropId(btn.dataset.task);fadeOut(card)}
      else if(d.state==='done'){toast('该任务已被其他人确认');dropId(btn.dataset.task);fadeOut(card)}
      else if(d.state==='expired'){toast('任务已过期，未提交');dropId(btn.dataset.task);fadeOut(card)}
      else{throw new Error(d.detail||'提交失败')}
    })
    .catch(function(err){
      toast('提交失败 '+err.message);
      opts.forEach(function(b){b.disabled=false});btn.classList.remove('chosen');
    });
  });

  // 撤回按钮：两段确认，4 秒内再点一次才真正撤回
  box.addEventListener('click',function(e){
    var rb=e.target.closest('.btn-revoke');if(!rb||rb.disabled)return;
    var card=rb.closest('.card');
    if(!rb.classList.contains('armed')){
      rb.classList.add('armed');rb.textContent='确认撤回？';
      setTimeout(function(){rb.classList.remove('armed');rb.textContent='撤回';},4000);
      return;
    }
    rb.disabled=true;
    var token=pageToken();
    fetch('/api/v1/task/revoke',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({task_id:rb.dataset.task,s:card.dataset.sig||'',token:token,by:'h5'})})
    .then(function(r){return r.json().catch(function(){throw new Error('HTTP '+r.status)})})
    .then(function(d){
      if(d.revoked){toast('已撤回');dropId(rb.dataset.task);fadeOut(card)}
      else{throw new Error(d.detail||'撤回失败')}
    })
    .catch(function(err){
      toast('撤回失败 '+err.message);
      rb.disabled=false;rb.classList.remove('armed');rb.textContent='撤回';
    });
  });

  // 5 秒轮询：待确认任务集合变化时整页刷新
  setInterval(function(){
    var token = pageToken();
    var tq = '/api/v1/tasks?limit=50' + (token ? '&token=' + encodeURIComponent(token) : '');
    fetch(tq).then(function(r){return r.json()}).then(function(rs){
      var live=(rs.items||[]).filter(function(t){return t.state==='pending'||t.state==='error'})
        .map(function(t){return t.task_id}).join(',');
      if(live!==CUR_IDS)location.reload();
    }).catch(function(){});
  },5000);
  refreshEmpty();
})();
</script>
</body>
</html>
"""


def _as_bool(v) -> bool:
    """宽松布尔解析：true/1/yes/on/y/t 为真（JSON 的 true 与字符串 "true" 都吃）。"""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in ("1", "true", "yes", "on", "y", "t")


# ---------------------------------------------------------------- 数据规范化
def _normalize_fields(fields) -> List[dict]:
    out: List[dict] = []
    if not fields:
        return out
    if isinstance(fields, dict):
        items = list(fields.items())
    elif isinstance(fields, (list, tuple)):
        items = []
        for it in fields:
            if isinstance(it, dict):
                items.append((it.get("keyname") or it.get("name") or it.get("key") or "",
                              it.get("value") or it.get("val") or ""))
            elif isinstance(it, (list, tuple)) and len(it) >= 2:
                items.append((str(it[0]), str(it[1])))
            else:
                items.append(("", str(it)))
    else:
        return out
    for k, v in items[:MAX_FIELDS]:
        if not k and not v:
            continue
        out.append({"keyname": str(k)[:32], "value": str(v)[:512]})
    return out


def _normalize_options(options, limit: int = MAX_OPTIONS_TEXT) -> List[dict]:
    out: List[dict] = []
    if not options:
        return out
    if isinstance(options, dict):
        items = []
        for k, v in options.items():
            if isinstance(v, dict):
                items.append({"key": k, **v})
            else:
                items.append({"key": k, "text": v})
        options = items
    for o in options[:limit]:
        if isinstance(o, str):
            out.append({"key": o, "text": o, "style": 1})
        elif isinstance(o, dict):
            key = str(o.get("key") or o.get("value") or o.get("text") or "").strip()
            text = str(o.get("text") or o.get("label") or key).strip()
            if not key:
                continue
            try:
                style = int(o.get("style", 1))
            except Exception:
                style = 1
            item = {"key": key[:64], "text": text[:128], "style": style}
            desc = str(o.get("desc") or o.get("description") or "").strip()
            if desc:
                item["desc"] = desc[:256]
            out.append(item)
    return out


def _const_eq(a: str, b: str) -> bool:
    if not a or not b or len(a) != len(b):
        return False
    r = 0
    for x, y in zip(a, b):
        r |= ord(x) ^ ord(y)
    return r == 0


def _h5_sign(task_id: str, token: str) -> str:
    if not token:
        return ""
    return hashlib.sha256(("%s|%s" % (token, task_id)).encode("utf-8")).hexdigest()[:16]


def _fmt_duration(sec: int) -> str:
    sec = max(0, int(sec))
    h, rem = divmod(sec, 3600)
    if rem == 0:
        return f"{h} 小时"
    return f"{h} 小时 {rem // 60} 分钟"


# ---------------------------------------------------------------- 任务存储
class _Task:
    """一条待确认任务（选项由调用方给出，用户点选后终结）。"""

    __slots__ = ("task_id", "title", "sub_title", "options", "fields", "style",
                 "created_at", "timeout_at", "timeout_sec", "state", "result", "push_err",
                 "extra_clicks", "event")

    def __init__(self, task_id, title, sub_title, options, timeout_sec, style="text"):
        self.task_id = task_id
        self.title = title
        self.sub_title = sub_title or ""
        self.options = options
        self.fields: List[dict] = []
        self.style = style
        self.created_at = time.time()
        self.timeout_sec = max(5, int(timeout_sec))
        self.timeout_at = self.created_at + self.timeout_sec
        self.state = "pending"
        self.result: Optional[dict] = None
        self.push_err: Optional[str] = None
        self.extra_clicks: List[dict] = []
        self.event = threading.Event()

    def is_expired(self, now=None):
        return self.state in ("pending", "error") and not self.result and (now or time.time()) > self.timeout_at

    def to_public(self, now=None):
        now = now or time.time()
        state = self.state
        if state in ("pending", "error") and now > self.timeout_at:
            state = "expired" if not self.result else "done"
        d = {
            "task_id": self.task_id,
            "state": state,
            "title": self.title,
            "sub_title": self.sub_title,
            "style": self.style,
            "fields": self.fields,
            "options": self.options,
            "created_at": self.created_at,
            "timeout_at": self.timeout_at,
            "timeout_sec": self.timeout_sec,
        }
        if self.result:
            d.update(self.result)
        if self.push_err:
            d["push_error"] = self.push_err
        return d


def _gen_task_id() -> str:
    return "tc-" + uuid.uuid4().hex[:24]


class _Store:
    """内存任务表（与原生 server.py 同构，事件用 threading.Event 便于线程内同步等待）。"""

    KEEP_DONE = 24 * 3600
    KEEP_LOST = 1 * 3600

    def __init__(self, max_tasks: int = 2000):
        self._tasks: Dict[str, _Task] = {}
        self._order: List[str] = []
        self.max_tasks = max_tasks
        self._lock = threading.Lock()

    def __len__(self):
        return len(self._tasks)

    def create(self, title, sub_title, options, timeout_sec, style="text") -> _Task:
        t = _Task(_gen_task_id(), title, sub_title, options, timeout_sec, style=style)
        with self._lock:
            self._tasks[t.task_id] = t
            self._order.append(t.task_id)
        self._trim()
        return t

    def get(self, task_id) -> Optional[_Task]:
        return self._tasks.get(task_id)

    def remove(self, task_id) -> bool:
        with self._lock:
            t = self._tasks.pop(task_id, None)
            if t is None:
                return False
            try:
                self._order.remove(task_id)
            except ValueError:
                pass
        return True

    def resolve(self, task_id, key, clicked_by, card_type=""):
        """返回 (选中文本 or None, task or None)。"""
        t = self._tasks.get(task_id)
        if t is None:
            return None, None
        opt = next((o for o in t.options if o.get("key") == key), None)
        if opt is None:
            return None, t
        text = opt.get("text")
        rec = {"option": key, "option_text": text, "clicked_by": clicked_by,
               "clicked_at": time.time(), "card_type": card_type}
        if t.state in ("pending", "error"):
            if t.is_expired():
                t.state = "expired"
                t.event.set()
                t.extra_clicks.append(rec)
                return None, t
            t.result = rec
            t.state = "done"
            t.event.set()
            return text, t
        t.extra_clicks.append(rec)
        return None, t

    def revoke(self, task_id, by="h5") -> Optional[_Task]:
        """撤回：未终结的任务立即终结为 expired，并留下列标记（option=__revoke__）。"""
        t = self._tasks.get(task_id)
        if t is None or t.result is not None or t.state not in ("pending", "error"):
            return None
        if t.is_expired():
            t.state = "expired"
            t.event.set()
            return None
        t.result = {"option": "__revoke__", "option_text": "已撤回", "clicked_by": by,
                    "clicked_at": time.time(), "card_type": "revoke"}
        t.state = "expired"
        t.event.set()
        return t

    def reaper_once(self):
        now = time.time()
        for t in list(self._tasks.values()):
            if t.is_expired(now):
                t.state = "expired"
                t.event.set()
        self._trim()

    def _trim(self):
        if len(self._tasks) <= self.max_tasks:
            return
        now = time.time()
        dead = [tid for tid, t in self._tasks.items()
                if (t.state == "done" and t.result and now - t.result["clicked_at"] > self.KEEP_DONE)
                or (t.state == "expired" and now - t.created_at > self.KEEP_LOST)]
        for tid in dead:
            self.remove(tid)
        with self._lock:
            while len(self._order) > self.max_tasks:
                self.remove(self._order[0])

    def recent(self, limit=50):
        with self._lock:
            order = list(self._order[-limit:])
        return [self._tasks[tid].to_public() for tid in reversed(order) if tid in self._tasks]

    def pending_tasks(self):
        now = time.time()
        out = []
        with self._lock:
            order = list(reversed(self._order))
        for tid in order:
            t = self._tasks.get(tid)
            if t and t.result is None and t.state in ("pending", "error") and not t.is_expired(now):
                out.insert(0, t)
        return out

    def wait_final(self, task_id: str, total_timeout: float, poll_interval: float,
                   is_stopping=None) -> Optional[dict]:
        """阻塞等待任务终结（done / expired），返回 to_public()。

        · 到 total_timeout 仍未终结 → 返回 None
        · is_stopping() 为真时提前返回 None（插件停机）
        """
        deadline = time.time() + max(0.0, total_timeout)
        while True:
            if is_stopping and is_stopping():
                return None
            t = self._tasks.get(task_id)
            if t is None:
                return {"task_id": task_id, "state": "unknown"}
            if t.result is not None or t.state in ("done", "expired"):
                return t.to_public()
            if t.is_expired():
                t.state = "expired"
                t.event.set()
                return t.to_public()
            remain = deadline - time.time()
            if remain <= 0:
                return None
            t.event.wait(min(max(0.2, poll_interval), remain))


# ---------------------------------------------------------------- 内嵌 Web 服务
class _ConfirmServer:
    """内嵌确认中心：FastAPI 应用 + uvicorn（后台线程），仅「确认卡片」一条业务线。"""

    def __init__(self, *, host: str, port: int, api_token: str, public_base_url: str,
                 ttl_default: int, ttl_max: int, max_tasks: int, max_wait: int,
                 reaper_interval: int, version: str,
                 notify_hook=None, command_hook=None, notify_label_hook=None):
        self.host = host
        self.port = int(port)
        self.api_token = api_token or ""
        self.public_base_url = (public_base_url or "").rstrip("/")
        self.ttl_default = max(5, int(ttl_default))
        self.ttl_max = max(self.ttl_default, int(ttl_max))
        self.max_wait = max(0, int(max_wait))
        self.reaper_interval = max(1, int(reaper_interval))
        self.version = version
        self.notify_hook = notify_hook                  # (task) -> None
        self.command_hook = command_hook                # (keyword) -> dict
        self.notify_label_hook = notify_label_hook      # () -> str（页脚通知状态文案）
        self.store = _Store(max_tasks=max_tasks)

        self._uv: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None
        self._reaper: Optional[threading.Thread] = None
        self._reaper_stop = threading.Event()
        self.app = self._build()

    # ---------------- 鉴权 ----------------
    def _provided_token(self, request: Request, body_token: str = "") -> str:
        if body_token:
            return str(body_token).strip()
        t = request.headers.get("x-api-token")
        if t:
            return t.strip()
        auth = request.headers.get("authorization") or ""
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return (request.query_params.get("token") or "").strip()

    async def _require_api_token(self, request: Request):
        if not self.api_token:
            return
        if not _const_eq(self._provided_token(request), self.api_token):
            raise HTTPException(status_code=401, detail="api_token 校验失败")

    # ---------------- 路由 ----------------
    def _build(self) -> FastAPI:
        S = self
        app = FastAPI(title="TaskConfirmMax", version=self.version, docs_url=None, redoc_url=None)

        def render_card(t: _Task) -> str:
            sig = _h5_sign(t.task_id, S.api_token)
            fields_html = ""
            if t.fields:
                rows = "".join(
                    f'<div class="frow"><span class="fk">{html.escape(f["keyname"])}</span>'
                    f'<span class="fv">{html.escape(f["value"])}</span></div>'
                    for f in t.fields)
                fields_html = f'<div class="fields">{rows}</div>'
            sub_html = f'<p class="sub">{html.escape(t.sub_title)}</p>' if t.sub_title else ""
            opts_html = "".join(
                f'<button class="opt" data-task="{t.task_id}" data-key="{html.escape(o["key"])}">'
                f'<span class="num">{i + 1}</span>'
                f'<span class="body"><span class="txt">{html.escape(o["text"])}</span>'
                + (f'<span class="desc">{html.escape(o["desc"])}</span>' if o.get("desc") else "")
                + '</span></button>'
                for i, o in enumerate(t.options))
            remain = max(0, int(t.timeout_at - time.time()))
            remain_h = (f"{remain // 3600}h{remain % 3600 // 60:02d}m" if remain >= 3600
                        else f"{remain // 60}m{remain % 60:02d}s")
            warn = " ⚠推送失败，仍可点选" if t.state == "error" else ""
            revoke_btn = f'<button class="btn-revoke" data-task="{t.task_id}" type="button">撤回</button>'
            return (
                f'<div class="card" data-task-id="{t.task_id}" data-sig="{html.escape(sig)}">'
                f'<div class="card-head"><h2>{html.escape(t.title)}</h2>{revoke_btn}</div>{sub_html}{fields_html}'
                f'<div class="opts">{opts_html}</div>'
                f'<div class="meta">{time.strftime("%H:%M:%S", time.localtime(t.created_at))} '
                f'创建 · 剩余 {remain_h}{warn}</div></div>'
            )

        @app.get("/api/health")
        async def health():
            done = sum(1 for t in S.store.recent(500) if t.get("state") == "done")
            return {
                "ok": True,
                "status": "ok",
                "time": int(time.time()),
                "version": S.version,
                "auth_enabled": bool(S.api_token),
                "public_base_url": S.public_base_url,
                "default_task_timeout": S.ttl_default,
                "tasks_recent": len(S.store),
                "tasks_done": done,
                "page": "/ui/pending",
            }

        @app.get("/api/v1/tasks")
        async def tasks_list(request: Request, limit: int = 30):
            await S._require_api_token(request)
            return {"items": S.store.recent(max(1, min(limit, 200)))}

        @app.post("/api/v1/task/create")
        async def task_create(request: Request):
            await S._require_api_token(request)
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="请求体必须是 JSON")

            title = str(body.get("title") or "").strip()
            style = str(body.get("style") or "text").strip().lower()
            if style not in ("text", "card"):
                style = "text"
            options = _normalize_options(body.get("options"),
                                         limit=MAX_OPTIONS_TEXT if style == "text" else MAX_BUTTONS)
            if not title:
                raise HTTPException(status_code=400, detail="title 不能为空")
            if not options:
                raise HTTPException(status_code=400, detail="options 至少需要 1 个（key/text）")

            timeout = body.get("timeout") or S.ttl_default
            try:
                timeout = int(timeout)
            except Exception:
                raise HTTPException(status_code=400, detail="timeout 必须是秒数（整数）")
            timeout = max(5, min(timeout, S.ttl_max))

            task = S.store.create(
                title=title[:64],
                sub_title=str(body.get("sub_title") or "")[:512],
                options=options,
                timeout_sec=timeout,
                style=style,
            )
            task.fields = _normalize_fields(body.get("fields"))
            logger.info(f"TaskConfirmMax 外部创建任务 {task.task_id}：{title}（{len(options)} 个选项）")
            if _as_bool(body.get("notify", True)) and S.notify_hook:
                try:
                    S.notify_hook(task)
                except Exception as e:  # noqa: BLE001
                    logger.warn(f"TaskConfirmMax 创建任务通知失败：{e}")
            return {"task_id": task.task_id, "state": "pending", "style": style,
                    "timeout": timeout, "page_url": "/ui/pending",
                    "status_url": f"/api/v1/task/status/{task.task_id}"}

        @app.get("/api/v1/task/status/{task_id}")
        async def task_status(request: Request, task_id: str, wait: int = 0):
            await S._require_api_token(request)
            if wait < 0:
                wait = 0
            wait = min(wait, S.max_wait)
            task = S.store.get(task_id)
            if task is None:
                return {"task_id": task_id, "state": "unknown"}
            if wait > 0 and task.result is None and task.state in ("pending", "error"):
                await asyncio.to_thread(task.event.wait, wait)
                task = S.store.get(task_id) or task
            return task.to_public()

        @app.post("/api/v1/task/answer")
        async def task_answer(request: Request):
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="请求体必须是 JSON")
            task_id = str(body.get("task_id") or "")
            key = str(body.get("key") or "")
            if not task_id or not key:
                raise HTTPException(status_code=400, detail="task_id / key 不能为空")

            if S.api_token:
                sig_ok = _const_eq(str(body.get("s") or request.query_params.get("s") or ""),
                                   _h5_sign(task_id, S.api_token))
                tok_ok = _const_eq(S._provided_token(request, body.get("token") or ""), S.api_token)
                if not (sig_ok or tok_ok):
                    raise HTTPException(status_code=401, detail="签名校验失败")

            text, task = S.store.resolve(task_id, key, str(body.get("by") or "h5"), "h5")
            if task is None:
                raise HTTPException(status_code=404, detail="任务不存在或已过期")
            state = task.to_public()["state"]
            if text is None:
                if state == "expired":
                    return {"task_id": task_id, "state": state, "accepted": False,
                            "msg": "任务已过期，未接受本次点击"}
                if state == "done" and task.result and task.result["option"] != key:
                    return {"task_id": task_id, "state": state, "accepted": False,
                            "option_text": task.result["option_text"]}
                raise HTTPException(status_code=400, detail="无效的选项 key")
            logger.info(f"TaskConfirmMax 任务 {task_id} 已确认：{key}（{text}）by={body.get('by') or 'h5'}")
            return {"task_id": task_id, "state": state, "accepted": True, "option_text": text}

        @app.post("/api/v1/task/revoke")
        async def task_revoke(request: Request):
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="请求体必须是 JSON")
            task_id = str(body.get("task_id") or "")
            if not task_id:
                raise HTTPException(status_code=400, detail="task_id 不能为空")
            if S.api_token:
                sig_ok = _const_eq(str(body.get("s") or request.query_params.get("s") or ""),
                                   _h5_sign(task_id, S.api_token))
                tok_ok = _const_eq(S._provided_token(request, body.get("token") or ""), S.api_token)
                if not (sig_ok or tok_ok):
                    raise HTTPException(status_code=401, detail="签名校验失败")
            task = S.store.revoke(task_id, str(body.get("by") or "h5"))
            if task is None:
                raise HTTPException(status_code=404, detail="任务不存在、已确认或已过期")
            logger.info(f"TaskConfirmMax 任务 {task_id} 已撤回 by={body.get('by') or 'h5'}")
            return {"task_id": task_id, "state": "expired", "revoked": True}

        @app.get("/api/v1/command/help")
        async def get_command_help(request: Request):
            if S.api_token:
                if not _const_eq(S._provided_token(request), S.api_token):
                    raise HTTPException(status_code=401, detail="鉴权失败")
            return {"ok": True, "content": HELP_TEXT}

        @app.post("/api/v1/command")
        async def exec_command(request: Request):
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="请求体必须是 JSON")
            cmd = str(body.get("cmd") or body.get("command") or "").strip()
            if not cmd:
                raise HTTPException(status_code=400, detail="指令内容不能为空")
            if S.api_token:
                if not _const_eq(S._provided_token(request, body.get("token") or ""), S.api_token):
                    raise HTTPException(status_code=401, detail="鉴权失败：无权的指令发送")

            # MP 专配版：整条输入即搜索词（兼容旧习惯，写了 `mp ` 前缀会被自动剥掉）
            keyword = cmd
            if keyword.lower() == "mp":
                keyword = ""
            elif keyword[:3].lower() == "mp ":
                keyword = keyword[3:].strip()
            if not keyword:
                return {"ok": False, "msg": "请直接输入搜索词，例如「蜘蛛侠」"}
            if S.command_hook is None:
                return {"ok": False, "msg": "插件命令处理器未就绪"}
            try:
                res = S.command_hook(keyword) or {}
            except Exception as e:  # noqa: BLE001
                logger.error(f"TaskConfirmMax 处理搜索指令失败：{e}")
                return {"ok": False, "msg": f"处理失败：{e}"}
            return {"ok": bool(res.get("success", True)),
                    "msg": res.get("message") or f"已开始搜索：{keyword}"}

        @app.get("/ui/pending", response_class=HTMLResponse)
        async def ui_pending():
            ts = S.store.pending_tasks()
            ids = ",".join(t.task_id for t in ts)
            cards = "".join(render_card(t) for t in ts)
            notify_label = "已开启"
            if S.notify_label_hook is not None:
                try:
                    notify_label = str(S.notify_label_hook())
                except Exception:
                    notify_label = "已开启"
            body = (PAGE_TEMPLATE.replace("__COUNT__", str(len(ts)))
                                 .replace("__IDS__", ids)
                                 .replace("__CARDS__", cards)
                                 .replace("__VERSION__", S.version)
                                 .replace("__PORT__", str(S.port))
                                 .replace("__TTL__", _fmt_duration(S.ttl_default))
                                 .replace("__NOTIFY__", notify_label))
            return HTMLResponse(body)

        @app.get("/", include_in_schema=False)
        async def root():
            return RedirectResponse("/ui/pending")

        return app

    # ---------------- 生命周期 ----------------
    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        # 端口可能刚被上一个实例释放（TIME_WAIT/优雅停机中），重试几次绑定
        for attempt in range(1, 4):
            cfg = uvicorn.Config(self.app, host=self.host, port=self.port,
                                 log_level="warning", access_log=False)
            self._uv = uvicorn.Server(cfg)
            self._thread = threading.Thread(target=self._uv.run,
                                            name=f"taskconfirmmax-web-{attempt}", daemon=True)
            self._thread.start()
            time.sleep(1.2)
            if self._thread.is_alive():
                break
            logger.warn(f"TaskConfirmMax 端口 {self.port} 绑定失败（第 {attempt} 次），重试")
            time.sleep(1.0)
        else:
            logger.error(f"TaskConfirmMax 内嵌确认中心启动失败：端口 {self.port} 一直无法绑定")
            return
        self._reaper_stop.clear()
        self._reaper = threading.Thread(target=self._reap_loop, name="taskconfirmmax-reaper", daemon=True)
        self._reaper.start()
        logger.info(f"TaskConfirmMax 内嵌确认中心已启动：http://{self.host}:{self.port}/ui/pending")

    def _reap_loop(self):
        while not self._reaper_stop.wait(self.reaper_interval):
            try:
                self.store.reaper_once()
            except Exception as e:  # noqa: BLE001
                logger.warn(f"TaskConfirmMax 过期巡检异常：{e}")

    def stop(self):
        self._reaper_stop.set()
        uv_server, thread = self._uv, self._thread
        try:
            if uv_server is not None:
                uv_server.should_exit = True
        except Exception:
            pass
        # 必须等旧线程真正退出（套接字已关闭），否则改配置重载时会撞 EADDRINUSE
        try:
            if thread is not None and thread.is_alive():
                thread.join(timeout=15)
        except Exception:
            pass
        self._uv = None
        self._thread = None
        logger.info("TaskConfirmMax 内嵌确认中心已停止")


# =====================================================================
#  插件：TaskConfirmMax
# =====================================================================
class TaskConfirmMax(_PluginBase):
    """任务确认中心 Max：内置 Web 确认服务（容器内 8005）+ MP 媒体搜索/资源搜索/下载。

    阶段 1  H5 指令框 `mp 搜索词` → MediaChain 搜索 → 候选卡片
    阶段 2  选中媒体 → SearchChain.search_by_id 搜站点资源 → 资源卡片
    阶段 3  选中资源 → DownloadChain 自动（默认下载器）或按所选下载器下载
    """

    plugin_name = "任务确认中心 Max"
    plugin_desc = ("一体化：插件内置 Web 确认服务（默认 0.0.0.0:8005，页面 /ui/pending），"
                   "指令框直接输入搜索词；选媒体→选资源→自动下载，三阶段闭环，专配 MoviePilot。")
    plugin_icon = "https://raw.githubusercontent.com/huiyi977/MoviePilot-TaskConfirmPlugins/main/icons/taskconfirmmax.png"
    plugin_version = "2.3.0"
    plugin_author = "huiyi977"
    author_url = ""
    plugin_config_prefix = "taskconfirmmax_"
    plugin_order = 29
    auth_level = 1

    _enabled = False
    _web_host = "0.0.0.0"
    _web_port = 8005
    _public_base_url = ""
    _api_token = ""
    _max_options = 8
    _task_timeout = 3600
    _wait_total = 21600
    _poll_interval = 5
    _send_center_notify = True
    _result_notify = True
    _torrent_mode = True
    _auto_downloader = True
    _check_library = True
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
        self._srv: Optional[_ConfirmServer] = None

    # ------------------------------------------------------------------ 生命周期
    def init_plugin(self, config: dict | None = None):
        config = config or {}
        self._stopping = False
        self._enabled = bool(config.get("enabled"))
        self._web_host = str(config.get("web_host") or "0.0.0.0").strip() or "0.0.0.0"
        try:
            self._web_port = max(1, min(65535, int(config.get("web_port") or 8005)))
        except Exception:
            self._web_port = 8005
        self._public_base_url = str(config.get("public_base_url") or "").strip().rstrip("/")
        if not self._public_base_url:
            self._public_base_url = f"http://192.168.5.126:{self._web_port}"
        self._api_token = str(config.get("api_token") or "").strip()
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
        self._notify_enabled = bool(config.get("notify_enabled", True))
        self._notify_channel = (str(config.get("notify_channel") or "wecom").strip().lower() or "wecom")
        self._notify_pic = str(config.get("notify_pic") or "").strip()
        try:
            self._notify_timeout = max(3, int(config.get("notify_timeout") or 15))
        except Exception:
            self._notify_timeout = 15
        self._notify_minimal = bool(config.get("notify_minimal", True))
        self._notify_minimal_text = (str(config.get("notify_minimal_text") or "").replace("\\n", "\n").strip()
                                     or DEFAULT_MINIMAL_TEXT)
        self._notify_minimal_pic = bool(config.get("notify_minimal_pic", False))
        self._bark_server = (str(config.get("bark_server") or "").strip() or DEFAULT_BARK_SERVER)
        self._bark_key = str(config.get("bark_key") or "").strip()
        self._wecom_webhook = str(config.get("wecom_webhook") or "").strip()
        self._gotify_url = str(config.get("gotify_url") or "").strip()
        self._gotify_token = str(config.get("gotify_token") or "").strip()
        # v2.1.x 旧配置迁移：兜底 webhook（企微）→ wecom_webhook
        if not self._wecom_webhook:
            self._wecom_webhook = str(config.get("notify_webhook") or "").strip()

        # 载入历史记录
        try:
            data = self.get_data("history")
            if isinstance(data, list):
                self._history = data
        except Exception:
            self._history = []

        # 重建内嵌服务（配置变了就重启）
        self._stop_server()
        if self._enabled:
            self._start_server()

        if self._enabled and config.get("onlyonce"):
            logger.info("TaskConfirmMax onlyonce 触发测试搜索：蜘蛛侠")
            threading.Thread(target=self._run_search_job, args=("蜘蛛侠",), daemon=True).start()
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
                "summary": "发起 MP 媒体搜索并创建确认任务（等价于 H5 指令框直接输入搜索词）",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "查询桥接任务状态一览",
            },
        ]

    # ------------------------------------------------------------------ 内嵌服务
    def _start_server(self):
        try:
            self._srv = _ConfirmServer(
                host=self._web_host,
                port=self._web_port,
                api_token=self._api_token,
                public_base_url=self._public_base_url,
                ttl_default=self._task_timeout,
                ttl_max=86400,
                max_tasks=2000,
                max_wait=300,
                reaper_interval=5,
                version=self.plugin_version,
                notify_hook=self._notify_card,
                command_hook=self._web_command,
                notify_label_hook=self._notify_label,
            )
            self._srv.start()
        except Exception as e:  # noqa: BLE001
            self._srv = None
            logger.error(f"TaskConfirmMax 内嵌确认中心启动失败（端口 {self._web_port} 可能被占用）：{e}")

    def _stop_server(self):
        try:
            if self._srv is not None:
                self._srv.stop()
        except Exception as e:  # noqa: BLE001
            logger.warn(f"TaskConfirmMax 停止内嵌服务异常：{e}")
        self._srv = None

    def _web_command(self, keyword: str) -> Dict[str, Any]:
        """H5 指令框发来的搜索词：后台线程执行搜索，立即返回。"""
        with self._lock:
            for job in self._jobs.values():
                if job.get("keyword") == keyword and job.get("state") == "pending":
                    return {"success": True,
                            "message": f"同关键词任务已在等待选择：{job.get('task_id')}"}
        threading.Thread(target=self._run_search_job, args=(keyword,), daemon=True).start()
        return {"success": True, "message": f"已开始搜索：{keyword}，结果稍后推送到本页"}

    # ------------------------------------------------------------------ API
    async def api_search(self, request: Request):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        try:
            body = await request.json()
        except Exception:
            body = {}
        keyword = str(body.get("keyword") or request.query_params.get("keyword") or "").strip()
        if keyword[:3].lower() == "mp ":
            keyword = keyword[3:].strip()
        if not keyword:
            return {"success": False, "message": "keyword 不能为空"}
        with self._lock:
            for job in self._jobs.values():
                if job.get("keyword") == keyword and job.get("state") == "pending":
                    return {"success": True,
                            "message": f"同关键词任务已在等待选择：{job['task_id']}",
                            "task_id": job["task_id"], "count": job.get("count", 0), "reused": True}
        return await asyncio.to_thread(self._run_search_job, keyword)

    async def api_status(self, request: Request):
        with self._lock:
            jobs = [self._job_public(j) for j in self._jobs.values()]
            jobs.sort(key=lambda x: x.get("created_at") or 0, reverse=True)
            history = list(reversed(self._history[-20:]))
        return {"success": True, "web": self._web_health(), "jobs": jobs, "history": history}

    def _web_health(self) -> Dict[str, Any]:
        srv = self._srv
        return {
            "running": bool(srv is not None and srv._thread is not None and srv._thread.is_alive()),
            "url": f"http://{self._web_host}:{self._web_port}/ui/pending" if srv else "",
            "public_base_url": self._public_base_url,
            "pending": len(srv.store.pending_tasks()) if srv else 0,
            "notify": self._notify_label(),
        }

    def _notify_label(self) -> str:
        if not self._notify_enabled:
            return "已关闭"
        label = f"{self._notify_channel} 开启"
        if self._notify_minimal:
            label += " · 极简"
        return label

    # ------------------------------------------------------------------ 核心流程
    def _create_task(self, title: str, sub_title: str, options: List[Dict[str, Any]],
                     timeout: Optional[int] = None, notify: bool = True) -> Optional[str]:
        """在进程内创建确认任务（可选推送通知），返回 task_id。"""
        srv = self._srv
        if srv is None:
            logger.error("TaskConfirmMax 内嵌确认中心未运行，无法创建任务")
            return None
        t = srv.store.create(title=title[:64], sub_title=sub_title[:512], options=options,
                             timeout_sec=timeout or self._task_timeout, style="text")
        if notify and self._send_center_notify:
            try:
                self._notify_card(t)
            except Exception as e:  # noqa: BLE001
                logger.warn(f"TaskConfirmMax 任务通知失败：{e}")
        return t.task_id

    def _wait_final(self, task_id: str) -> Optional[Dict[str, Any]]:
        srv = self._srv
        if srv is None:
            return None
        return srv.store.wait_final(task_id, self._wait_total, self._poll_interval,
                                    is_stopping=lambda: self._stopping or self._srv is not srv)

    def _run_search_job(self, keyword: str) -> Dict[str, Any]:
        """同步执行：搜索媒体 → 建阶段 1 确认任务 → 起后台线程收割。"""
        logger.info(f"TaskConfirmMax 收到搜索词：{keyword}")
        try:
            _meta, medias = MediaChain().search(keyword, limit=20)
        except Exception as e:
            logger.error(f"TaskConfirmMax 搜索失败：{e}")
            return {"success": False, "message": f"搜索失败：{e}"}
        if not medias:
            return {"success": False, "message": f"未找到与「{keyword}」相关的媒体信息"}

        medias = medias[: self._max_options]
        options: List[Dict[str, Any]] = []
        key_map: Dict[str, Any] = {}
        for idx, media in enumerate(medias, start=1):
            key = f"opt{idx}"
            options.append({"key": key,
                            "text": self._option_text(idx, media),
                            "desc": self._option_desc(media)})
            key_map[key] = media

        task_id = self._create_task(
            title=f"MP 搜索：{keyword}",
            sub_title="阶段 1/3 · 请选择要订阅的条目",
            options=options,
        )
        if not task_id:
            return {"success": False, "message": "内嵌确认中心未运行，创建任务失败"}
        job = {"task_id": task_id, "keyword": keyword, "state": "pending",
               "stage": 1, "created_at": time.time(), "count": len(options)}
        with self._lock:
            self._jobs[task_id] = job
        threading.Thread(target=self._wait_and_subscribe,
                         args=(task_id, keyword, key_map), daemon=True).start()
        logger.info(f"TaskConfirmMax 已创建确认任务 {task_id}（{len(options)} 个候选）")
        return {"success": True,
                "message": f"已创建确认任务 {task_id}（{len(options)} 个候选），请在本页选择",
                "task_id": task_id, "count": len(options)}

    def _wait_and_subscribe(self, task_id: str, keyword: str, key_map: Dict[str, Any]):
        """后台线程：等待阶段 1 卡片，用户选中媒体后进入阶段 2（或直接订阅）。"""
        final = self._wait_final(task_id)
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
                self._append_history(job, f"媒体库已存在（{exists_info.get('server') or '媒体服务器'}），已跳过")
                logger.info(f"TaskConfirmMax 媒体库已存在，跳过阶段 2/3：{option_text}")
                self._notify_library_exists(media, exists_info)
                return

        if self._torrent_mode:
            self._run_torrent_stage(job, keyword, media, option_text)
            return

        logger.info(f"TaskConfirmMax 任务 {task_id} 用户已选：{option_text}，开始添加订阅")
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
            logger.error(f"TaskConfirmMax 订阅异常：{e}")
            self._append_history(job, f"订阅异常：{e}")
            self._notify_result(f"❌ 订阅失败：{option_text}", f"任务 {task_id}\n{e}")
            return
        if sub_id:
            job["state"] = "subscribed"
            job["subscribe_id"] = sub_id
            self._append_history(job, f"已添加订阅 subscribe_id={sub_id}")
            self._notify_result(f"✅ 已订阅：{option_text}",
                                f"搜索词：{keyword}\n任务：{task_id}\n订阅号：{sub_id}")
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
            logger.warn(f"TaskConfirmMax 媒体库查询失败（按不存在继续）：{e}")
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
        """媒体库已存在 → 发一条通知（走插件内置通知渠道），随后终止本次任务。"""
        title = str(getattr(media, "title", "") or "未知媒体")
        year = getattr(media, "year", None) or "未知年份"
        first = f"ℹ️ 媒体库已存在：{title}"
        lines = [first, f"媒体：{title}（{year}）"]
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
                logger.warn(f"TaskConfirmMax 媒体库已存在通知发送失败：{e}")
        else:
            logger.info("TaskConfirmMax 通知总开关关闭，媒体库已存在提示未推送")

    # ------------------------------------------------------------------ 阶段 2：资源搜索
    def _run_torrent_stage(self, job: Dict[str, Any], keyword: str, media, media_text: str):
        """阶段 2：按 media_source/media_id 搜索资源 → 编号推卡片 → 等用户选择。"""
        m_title = str(media.title or keyword)
        m_source = str(getattr(media, "media_source", "") or "")
        m_id = str(getattr(media, "media_id", "") or "")
        logger.info(f"TaskConfirmMax 阶段 2 资源搜索：{media_text}（{m_source}:{m_id}）")
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
            logger.error(f"TaskConfirmMax 资源搜索异常：{e}")
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
            options.append({"key": key,
                            "text": self._torrent_text(idx, ti),
                            "desc": self._torrent_desc(ti)})
            torrent_map[key] = ctx
        if not options:
            job["state"] = "no_torrents"
            self._append_history(job, "资源搜索结果均为空种子信息")
            self._notify_result(f"❌ 未搜索到有效资源：{m_title}", f"任务 {job.get('task_id')}")
            return

        task_id2 = self._create_task(
            title=f"资源选择：{m_title}（{media.year or '未知年份'}）",
            sub_title="阶段 2/3 · 请选择要下载的资源",
            options=options,
        )
        if not task_id2:
            job["state"] = "error"
            self._append_history(job, "内嵌确认中心创建资源选择任务失败")
            self._notify_result(f"❌ 资源卡片推送失败：{m_title}", f"任务 {job.get('task_id')}")
            return

        job2 = {"task_id": task_id2, "keyword": keyword, "state": "pending", "stage": 2,
                "created_at": time.time(), "count": len(options),
                "selected": f"{m_title}（{media.year or '未知年份'}）"}
        with self._lock:
            self._jobs[task_id2] = job2
        logger.info(f"TaskConfirmMax 已创建资源选择任务 {task_id2}（{len(options)} 个资源，按做种数排序截取）")

        final = self._wait_final(task_id2)
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
        logger.info(f"TaskConfirmMax 阶段 2 完成，用户选定资源：{torrent_text}")
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
                items.append({"name": name,
                              "type": str(getattr(conf, "type", "") or ""),
                              "default": bool(getattr(conf, "default", False))})
        except Exception as e:
            logger.warn(f"TaskConfirmMax 读取下载器配置失败：{e}")
        return items

    def _default_downloader_name(self) -> str:
        items = self._list_downloaders()
        for it in items:
            if it.get("default"):
                return it["name"]
        return items[0]["name"] if items else ""

    def _run_download_stage(self, job: Dict[str, Any], ctx, torrent_text: str):
        """阶段 3：自动用默认下载器下载；关闭自动则先让用户选下载器。"""
        if self._auto_downloader:
            name = self._default_downloader_name()
            logger.info(f"TaskConfirmMax 阶段 3 自动下载（默认下载器：{name or '由 MP 决定'}）：{torrent_text}")
            self._do_download(job, ctx, torrent_text, None)
            return

        items = self._list_downloaders()
        if not items:
            self._append_history(job, "未找到已启用的下载器，回退默认下载器")
            self._notify_result("⚠️ 未找到已启用的下载器，改用默认下载器", f"资源：{torrent_text}")
            self._do_download(job, ctx, torrent_text, None)
            return

        options: List[Dict[str, Any]] = []
        name_map: Dict[str, str] = {}
        for i, it in enumerate(items, start=1):
            key = f"d{i}"
            tag = "（默认）" if it.get("default") else ""
            options.append({"key": key,
                            "text": f"{it['name']}{tag}｜{it.get('type') or '未知类型'}",
                            "desc": f"下载器：{it['name']}（{it.get('type') or '未知类型'}）{tag}"})
            name_map[key] = it["name"]

        task_id3 = self._create_task(
            title=f"下载器选择：{self._clean_torrent_title(torrent_text)}",
            sub_title="阶段 3/3 · 请选择下载器",
            options=options,
        )
        if not task_id3:
            self._append_history(job, "内嵌确认中心创建下载器选择任务失败，回退默认下载器")
            self._do_download(job, ctx, torrent_text, None)
            return

        job3 = {"task_id": task_id3, "keyword": job.get("keyword"), "state": "pending", "stage": 3,
                "created_at": time.time(), "count": len(options), "selected": torrent_text[:64]}
        with self._lock:
            self._jobs[task_id3] = job3
        logger.info(f"TaskConfirmMax 已创建下载器选择任务 {task_id3}（{len(options)} 个下载器）")

        final = self._wait_final(task_id3)
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
                source="TaskConfirmMax",
            )
        except Exception as e:
            logger.error(f"TaskConfirmMax 添加下载异常：{e}")
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
            logger.info(f"TaskConfirmMax 已添加下载：{used} hash={did}｜{torrent_text}")
            self._notify_result(f"⬇️ 已开始下载（{used}）：{self._torrent_short_title(ti)}",
                                f"{torrent_text}\n下载器：{used}\n任务：{job.get('task_id')}")
        else:
            job["state"] = "download_failed"
            self._append_history(job, f"添加下载失败（{used}）")
            self._notify_result(f"❌ 添加下载失败（{used}）：{self._torrent_short_title(ti)}",
                                f"{torrent_text}\n任务：{job.get('task_id')}")

    @staticmethod
    def _clean_torrent_title(torrent_text: str) -> str:
        first = str(torrent_text or "").split("｜")[0].strip()
        parts = first.split(". ", 1)
        if len(parts) == 2 and parts[0].isdigit():
            first = parts[1]
        return first[:40] or "未知资源"

    @staticmethod
    def _torrent_short_title(ti) -> str:
        title = str(getattr(ti, "title", "") or "未知资源")
        return title[:40]

    # ------------------------------------------------------------------ 通知
    def _build_minimal_text(self, kind: str, title: str, url: str) -> str:
        now = time.strftime("%H:%M", time.localtime())
        tpl = self._notify_minimal_text
        for holder, val in (("{time}", now), ("{kind}", kind),
                            ("{title}", title), ("{url}", url)):
            tpl = tpl.replace(holder, val)
        return tpl.strip()

    def _notify_card(self, task: _Task) -> None:
        """确认卡片推送（内置直推 bark/wecom/gotify，极简模式可用）。非阻塞：另起线程发送。"""
        if not self._notify_enabled:
            logger.info("TaskConfirmMax 通知总开关关闭，跳过任务通知 %s", task.task_id)
            return
        url = f"{self._public_base_url}/ui/pending"
        if self._api_token:
            url += f"?token={self._api_token}"
        if self._notify_minimal:
            text = self._build_minimal_text("MP确认", task.title, url)
        else:
            lines = [f"【MP 任务确认】{task.title}"]
            if task.sub_title:
                lines.append(task.sub_title)
            lines.append(f"有效期 {_fmt_duration(task.timeout_sec)}")
            lines.append("请选择：")
            lines.extend(f"{i + 1}. {o['text']}" + (f"（{o['desc']}）" if o.get("desc") else "")
                         for i, o in enumerate(task.options))
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
            logger.info(f"TaskConfirmMax 通知已发送（{'/'.join(sent)}）：{title[:60]}")
        if failed:
            logger.warn(f"TaskConfirmMax 通知失败：{'；'.join(failed)}")

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
            logger.warn(f"TaskConfirmMax 发送结果通知失败：{e}")

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
        return (f"{idx}. {title}"
                f"｜{self._fmt_size(getattr(ti, 'size', 0))}"
                f"｜{getattr(ti, 'site_name', '') or '未知站点'}"
                f"｜⬆{getattr(ti, 'seeders', 0) or 0}{free}")[:256]

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
        rec = {"task_id": job.get("task_id"), "keyword": job.get("keyword"),
               "state": job.get("state"), "selected": job.get("selected"), "note": note,
               "time": time.strftime("%Y-%m-%d %H:%M:%S")}
        with self._lock:
            self._history.append(rec)
            self._history = self._history[-50:]
        try:
            self.save_data("history", self._history)
        except Exception:
            pass

    @staticmethod
    def _job_public(job: Dict[str, Any]) -> Dict[str, Any]:
        return {"task_id": job.get("task_id"), "keyword": job.get("keyword"),
                "state": job.get("state"), "stage": job.get("stage"),
                "count": job.get("count"), "selected": job.get("selected"),
                "created_at": job.get("created_at")}

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

        return [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        switch("enabled", "启用插件"),
                        switch("onlyonce", "立即运行一次（测试搜索：蜘蛛侠）"),
                        col("web_host", "监听地址", 6, "0.0.0.0"),
                        col("web_port", "监听端口", 6, "8005"),
                        col("public_base_url", "对外访问地址（通知里的页面链接）", 12, "http://192.168.5.126:8005"),
                        col("api_token", "页面/接口鉴权 token（可留空）", 12),
                        col("max_options", "候选/资源数上限", 3),
                        col("task_timeout", "确认任务有效期（秒）", 3),
                        col("wait_total", "整体收割超时（秒）", 3),
                        col("poll_interval", "轮询间隔（秒）", 3),
                        switch("send_center_notify", "任务推送时发通知"),
                        switch("torrent_mode", "选媒体后搜索资源（关闭则直接订阅）"),
                        switch("auto_downloader", "阶段 3 自动调用默认下载器（关闭则推送下载器选择卡）"),
                        switch("check_library", "检查媒体库已存在（存在则通知并终止）"),
                        switch("result_notify", "结果经 MP 通知播报"),
                        {"component": "VCol", "props": {"cols": 12, "md": 12}, "content": [
                            {"component": "VDivider", "props": {"class": "my-2"}}]},
                        switch("notify_enabled", "启用确认卡片通知"),
                        col("notify_channel", "通知渠道（bark/wecom/gotify/all）", 3, "wecom"),
                        col("notify_timeout", "通知超时（秒）", 3, "15"),
                        col("notify_pic", "通知图片 URL（仅 bark 生效）", 6),
                        {"component": "VCol", "props": {"cols": 12, "md": 12}, "content": [
                            {"component": "VDivider", "props": {"class": "my-2"}}]},
                        col("bark_server", "Bark 服务器", 4, "https://api.day.app"),
                        col("bark_key", "Bark DeviceKey", 8),
                        col("wecom_webhook", "企业微信机器人 Webhook（完整 URL）", 12),
                        col("gotify_url", "Gotify 地址", 6, "http://192.168.5.70:18100"),
                        col("gotify_token", "Gotify Token", 6),
                        {"component": "VCol", "props": {"cols": 12, "md": 12}, "content": [
                            {"component": "VDivider", "props": {"class": "my-2"}}]},
                        switch("notify_minimal", "极简模式（只发时间+一行提示）"),
                        switch("notify_minimal_pic", "极简模式仍附图片"),
                        col("notify_minimal_text", "极简正文模板（{time}{kind}{title}{url}）", 12),
                    ]},
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "web_host": "0.0.0.0",
            "web_port": 8005,
            "public_base_url": "http://192.168.5.126:8005",
            "api_token": "",
            "max_options": 8,
            "task_timeout": 3600,
            "wait_total": 21600,
            "poll_interval": 5,
            "send_center_notify": True,
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
        wh = self._web_health()
        head = (f"内嵌确认中心：{'运行中' if wh['running'] else '未运行'}\n"
                f"页面地址：{wh['url'] or '（未启动）'}\n"
                f"对外地址：{wh['public_base_url']}\n"
                f"待确认：{wh['pending']} 个 · 通知：{wh['notify']}\n\n")
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": (
                        head +
                        "用法：打开页面指令框直接输入搜索词 → 选择媒体（阶段 1）→ "
                        "选择站点资源（阶段 2）→ 自动/指定下载器下载（阶段 3）。\n\n最近记录：\n" + lines
                    ),
                },
            }
        ]

    def stop_service(self):
        self._stopping = True
        self._stop_server()
