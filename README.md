# zhenxun-plugin-emby-search

基于 [zhenxun_bot](https://github.com/zhenxun-bot/zhenxun_bot) 的 Emby 媒体库插件，提供**库内搜番**与**新增动态**两个功能，结果均渲染为带海报的卡片图。

## 功能

| 指令 | 说明 |
|------|------|
| `emby搜番 [关键词]` | 搜索 Emby 媒体库，卡片图显示海报、年份、⭐评分、季话数、入库时间、简介；支持常见缩写（如 `RE0` → Re：从零） |
| `emby新增动态 [天数?]` | 查看最近入库/更新动态，区分「整部入库」与「话更新」，逐集显示标题与字幕组，时间精确到秒 |

卡片图底部带署名（头像 + DaisySG），模板可自行修改。

## 安装

```bash
cd zhenxun/plugins
git clone https://github.com/DaisySG297/zhenxun-plugin-emby-search emby_search
```

重启 bot 后生效。

## 配置

编辑 `config.py`（或通过 zhenxun WebUI 插件配置）：

| 配置项 | 说明 |
|--------|------|
| `base_url` | Emby 服务器地址，如 `http://localhost:8094` |
| `api_token` | Emby API Token（Emby 控制台 → 高级 → API 密钥） |
| `user_id` | 用于查询的 Emby 用户 ID（建议管理员） |
| `anime_lib_id` | 番剧库的 Library ID |
| `r18_lib_id` | 里番库 Library ID，留空则不启用过滤 |

获取 `user_id` / 库 ID：浏览器登录 Emby 后，在库页面 URL 或用户接口中可见。

## 依赖

- zhenxun_bot 自带依赖（httpx、jinja2）
- `nonebot_plugin_htmlrender`（zhenxun 标配，用于渲染卡片图）

## 其他说明

- Emby 搜索接口必须 `Recursive=true`，否则会返回媒体库目录而非剧集
- 缩写别名在 `data_source.py` 的 `_ANIME_ALIASES` 中维护，可自行扩充
- 插件内置一个可选的 Emby Webhook 接收服务（默认端口 8095），用于接收播放入库推送

## License

MIT
