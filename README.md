# ⚡ Down-Streamer

基于全球公开测速节点的 Docker 下行流量生成容器。

## 快速启动

```bash
# 1. 按需修改 docker-compose.yml 中的参数
# 2. 构建并启动
docker compose up -d

# 3. 查看日志
docker compose logs -f

# 4. 停止
docker compose down
```

## 核心参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `INTERVAL_SEC` | 30 | 每轮下载间隔（秒），建议 ≥ 20 |
| `TARGET_MB` | 100 | 每轮目标下载量（MB） |
| `MAX_TOTAL_GB` | 0 | 总量上限 GB，0=无限 |
| `TIMEOUT_SEC` | 60 | 单次下载超时（秒） |
| `MAX_CONSEC_FAIL` | 5 | 连续失败上限（触发电路中断） |
| `JITTER_PCT` | 0.3 | 间隔抖动系数（0~1，防封关键参数） |
| `LOG_LEVEL` | INFO | 日志级别 |

## 常见配置场景

### 轻度刷量（日均 ~5GB）
```yaml
- INTERVAL_SEC=120
- TARGET_MB=50
```

### 中度刷量（日均 ~30GB）
```yaml
- INTERVAL_SEC=30
- TARGET_MB=100
```

### 高强度刷量（日均 ~100GB+）
```yaml
- INTERVAL_SEC=10
- TARGET_MB=200
```

## 防封策略

1. **多源轮换** — 15+ 全球公开测速节点随机切换，避免单点特征
2. **User-Agent 轮换** — 6 种主流浏览器 UA 随机选择
3. **间隔抖动** — 每轮实际间隔 = `INTERVAL_SEC ± JITTER_PCT%`，模拟人类行为
4. **电路中断器** — 连续失败自动暂停，避免异常流量特征暴露
5. **禁用压缩** — `Accept-Encoding: identity`，确保真实下行流量计入
6. **资源限制** — CPU/内存上限，防止失控

## 统计文件

运行进度保存在 `./data/stats.json`，容器重启自动恢复。
