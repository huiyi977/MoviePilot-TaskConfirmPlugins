# MoviePilot-TaskConfirmPlugins

[MoviePilot V3](https://github.com/jxxghp/MoviePilot) 自定义插件仓库（个人用）。

## 插件列表

| 插件 | 说明 |
|---|---|
| TaskConfirmBridge 任务确认桥接 | 把「任务确认中心」指令框的 `mp <搜索词>` 转成 MoviePilot 媒体搜索，候选作为确认卡片推回中心，用户在 H5 选定后自动添加订阅。 |

## 安装

MoviePilot → 设定 → 插件 → 插件市场，加入本仓库地址：

```
https://github.com/huiyi977/MoviePilot-TaskConfirmPlugins
```

刷新后即可在「在线插件」里安装 **任务确认桥接 TaskConfirmBridge**。

## 插件配置

| 配置项 | 默认 | 说明 |
|---|---|---|
| 中心地址 | `http://192.168.5.126:8005` | 任务确认中心服务地址 |
| 中心 token | 空 | 确认中心配置了 api_token 时填写 |
| 候选上限 | 8 | 推回确认中心的候选条数上限 |
| 任务有效期 | 3600s | 确认任务超时时间 |
| 收割窗口 | 21600s | 用户点选后插件等待收割的最长时间 |

## 使用

在任务确认中心（`/ui/pending`）指令框输入：

```
mp 西游记
```

确认中心会弹出候选卡片（标题/年份/类型/评分/简介），点选后 MoviePilot 自动添加订阅并播报结果。
