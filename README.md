# ⚡ Down-Streamer

基于全球官方 Linux 发行版镜像站的 Docker 下行流量生成容器。  
通过 HTTP Range 请求从 ISO 文件的随机偏移处读取数据，模拟真实用户下载行为，天然抗封禁。

---

## 设计思路

| 方案 | 说明 |
|------|------|
| **官方镜像站** | 镜像站的存在意义就是给人下载 ISO，循环下载完全合规，不会触发任何限制 |
| **Range 请求 + 随机偏移** | 每次从文件的随机位置读取，绕过 CDN 缓存，流量特征等同于"下载到一半断开连接的普通用户" |
| **数据直接丢弃** | 下载的数据不写磁盘，纯刷下行流量，不占用容器存储 |
| **多镜像站轮换** | 每个 ISO 源对应 3~7 个国内镜像站，自动跳过不可达节点 |

---

## 快速启动

```bash
# 1. 拉取镜像
docker pull deeplakehss/down-streamer:latest

# 2. 启动（首次会自动创建 ./data 目录）
docker compose up -d

# 3. 查看实时日志
docker compose logs -f

# 4. 停止
docker compose down
```

> 无需本地构建，镜像已托管在 [Docker Hub](https://hub.docker.com/r/deeplakehss/down-streamer)。

---

## 核心参数

在 `docker-compose.yml` 的 `environment` 中修改：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `INTERVAL_SEC` | 30 | 每轮下载间隔（秒），建议 ≥ 20 |
| `TARGET_MB` | 1024 | 每轮目标下载量（MB），Range 请求从 ISO 随机偏移处读取 |
| `MAX_TOTAL_GB` | 0 | 总量上限（GB），0 = 无限 |
| `TIMEOUT_SEC` | 120 | 单次下载超时（秒），ISO 较大建议 ≥ 90 |
| `MAX_CONSEC_FAIL` | 8 | 连续失败上限，触发电路中断器 |
| `JITTER_PCT` | 0.3 | 间隔抖动系数（0~1），实际间隔 = `INTERVAL_SEC ± JITTER_PCT%` |
| `SIMULATE_HUMAN` | 1 | 下载完随机停顿 0.5~3s（模拟人类），0 = 关闭 |
| `SCHEDULE_HOURS` | *(空)* | 下载时间窗口（本地时区），格式 `"START-END"`（小时），空 = 全天 |
| `LOG_LEVEL` | INFO | 日志级别：DEBUG / INFO / WARNING / ERROR |
| `TZ` | Asia/Shanghai | 容器时区 |

### `SCHEDULE_HOURS` 示例

| 配置值 | 效果 |
|--------|------|
| `""`（空） | 全天 24 小时运行 |
| `"0-6"` | 只在凌晨 0:00 ~ 6:00 下载 |
| `"22-6"` | 晚上 22:00 ~ 次日早 6:00 下载（跨午夜） |
| `"9-18"` | 只在工作时间 9:00 ~ 18:00 下载 |

---

## 常见配置场景

### 轻度（日均 ~5 GB）

```yaml
- INTERVAL_SEC=120
- TARGET_MB=50
```

### 中度（日均 ~30 GB）

```yaml
- INTERVAL_SEC=30
- TARGET_MB=100
```

### 高强度（日均 ~100 GB+）

```yaml
- INTERVAL_SEC=10
- TARGET_MB=200
- MAX_TOTAL_GB=0        # 无限
- SCHEDULE_HOURS=0-6    # 只在凌晨跑
```

---

## ISO 源池（2026-06 实测验证）

| 源名称 | 大小 | 可用镜像站 |
|---------|------|-------------|
| Ubuntu 24.04.4 Desktop | 5.8 GB | 清华 / 阿里 / 中科大 / 哈工大 / 北外 / 上交 / 华为云 |
| Ubuntu 24.04.4 Server | 2.6 GB | 同上 |
| Ubuntu 22.04.5 Desktop | 4.4 GB | 清华 / 阿里 / 中科大 / 哈工大 / 华为云 |
| Debian 13.5 (Trixie) DVD | 3.7 GB | 清华 / 阿里 / 中科大 / 哈工大 / 北外 / 上交 |
| Debian 13.5 Netinst | 0.74 GB | 同上 |
| CentOS Stream 9 DVD | 14.4 GB | 清华 / 阿里 / 中科大 / 哈工大 |
| CentOS Stream 9 Boot | 1.4 GB | 同上（小文件备选） |
| Arch Linux Latest | 1.5 GB | 清华 / 阿里 / 中科大 / 哈工大 / 北外 |

> 每次下载从 ISO 文件的**随机偏移**处读取 `TARGET_MB` 大小的片段，文件指针位置每次不同，CDN 缓存自然绕过。

---

## 防封策略

| # | 策略 | 实现方式 |
|---|--------|----------|
| 1 | **官方镜像站** | 镜像站天然承载海量下载，循环下载完全合规 |
| 2 | **Range 随机偏移** | 每次请求的文件位置不同，流量特征 = 正常用户下载中断 |
| 3 | **UA 轮换** | 5 种主流浏览器 User-Agent 随机选取 |
| 4 | **间隔抖动** | 实际间隔 = `INTERVAL_SEC ± JITTER_PCT%`，消除定时器指纹 |
| 5 | **HEAD 预检** | 下载前先发 HEAD 请求确认 URL 有效，404 立即跳过 |
| 6 | **健康状态管理** | DNS 失败 / 网络不可达 / HTTP 4xx 自动标记 dead，定期重置重探 |
| 7 | **电路中断器** | 连续失败 N 次 → 自动暂停冷却 → 恢复重试 |
| 8 | **DNS 绕过代理** | 启动时覆盖 `/etc/resolv.conf`，绕过 Docker 内部 DNS 代理（Windows 环境兼容） |
| 9 | **资源限制** | CPU 0.5 核 / 内存 512 MB 上限，防止失控 |

---

## 统计与监控

运行进度保存在 `./data/stats.json`，容器重启自动恢复：

```json
{
  "total_bytes": 10737418240,
  "total_gb": 10.0,
  "daily_bytes": 1073741824,
  "daily_gb": 1.0,
  "daily_date": "2026-06-11",
  "rounds_completed": 10,
  "downloads_ok": 10,
  "downloads_fail": 1,
  "consec_fail": 0,
  "circuit_breaker_trips": 0,
  "mirror_hits": { "mirrors.tuna.tsinghua.edu.cn": 5, "...": 3 },
  "started_at": "2026-06-11T15:00:00+00:00",
  "last_activity": "2026-06-11T15:30:00+00:00"
}
```

- **`daily_gb`** 每日自动归零（跨日检测，基于 `TZ` 时区）
- **`mirror_hits`** 记录各镜像站命中次数，便于排查哪家速度快

日志示例：

```
2026-06-11 23:30:01 [INFO] ⚡ Down-Streamer v2.3 启动
2026-06-11 23:30:01 [INFO]   间隔: 30s | 每轮: 1024MB | 超时: 120s
2026-06-11 23:30:01 [INFO]   下载窗口: 全天
2026-06-11 23:30:01 [INFO] DNS 预检通过：7/7 个域名可解析
2026-06-11 23:30:01 [INFO] 探测完成：8/8 个源可用
2026-06-11 23:30:02 [INFO] → 轮次 1 | 源: Ubuntu-24.04-Desktop | 目标: 1024 MB
2026-06-11 23:30:05 [INFO] ✓ Ubuntu-24.04-Desktop @ mirrors.tuna.tsinghua.edu.cn | 1024.0 MB (偏移 2048 MB) | 3.2s | 2560.0 Mbps
2026-06-11 23:30:05 [INFO] 📊 当日: 1.00 GB | 总累计: 1.00 GB | 成功: 1 | 失败: 0
2026-06-11 23:30:05 [INFO] ⏳ 等待 28.4s ...
```

---

## 项目结构

```
down-streamer/
├── docker-compose.yml          ← 一键启动配置
├── Dockerfile                 ← 容器镜像（python:3.12-alpine，~50MB）
├── .dockerignore
├── .github/
│   └── workflows/
│       └── docker-build-push.yml   ← 自动构建推送 Docker Hub
├── scripts/
│   ├── entrypoint.sh         ← DNS 修复 + 启动引导
│   └── downloader.py        ← 核心下载引擎
└── README.md
```

---

## 镜像构建（开发者）

```bash
# 本地构建
docker build -t deeplakehss/down-streamer:latest .

# 推送
docker push deeplakehss/down-streamer:latest
```

或触发 GitHub Actions 自动构建：

```bash
gh workflow run docker-build-push.yml -R hushenshen/down-streamer
```

---

## 版本历史

| 版本 | 核心变更 |
|------|---------|
| v1.0 | 初版，基于全球测速节点 |
| v1.1 | 动态 DNS 健康检查，自动跳过死源 |
| v1.2 | Cloudflare 为主力源（90%），慢速检测 |
| **v2.0** | **完全重写：改用官方 Linux ISO 镜像站 + Range 随机偏移** |
| v2.1 | 验证镜像 URL 有效性，新增 HEAD 预检 |
| v2.2 | 默认下载量 100→1024 MB，新增 `SCHEDULE_HOURS` 时间窗口 |
| **v2.3** | **新增当日/总累计双维度流量统计，跨日自动归零** |

---

## 仓库地址

- **GitHub**：https://github.com/hushenshen/down-streamer
- **Docker Hub**：https://hub.docker.com/r/deeplakehss/down-streamer
