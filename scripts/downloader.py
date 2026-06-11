#!/usr/bin/env python3
"""
Down-Streamer v1.1
- 多源轮换：从全球公开测速节点下载，避免单点封锁
- 动态 DNS 健康检查：启动时探测所有源，运行时自动跳过死源
- 智能调度：随机化 User-Agent / 间隔 / 文件大小
- 电路中断：连续失败自动暂停，防止异常流量特征
- 流量统计：实时记录已刷流量、成功率、耗时
"""

import os
import sys
import time
import json
import random
import signal
import socket
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

# ──────────────────────────────────────────────
# 配置（从环境变量注入）
# ──────────────────────────────────────────────
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "30"))          # 每轮下载间隔（秒）
TARGET_MB = int(os.getenv("TARGET_MB", "100"))               # 每轮目标下载量（MB）
MAX_TOTAL_GB = float(os.getenv("MAX_TOTAL_GB", "0"))         # 总量上限 GB（0=无限）
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "60"))            # 单次下载超时（秒）
MAX_CONSEC_FAIL = int(os.getenv("MAX_CONSEC_FAIL", "5"))     # 连续失败上限 → 触发电路中断
JITTER_PCT = float(os.getenv("JITTER_PCT", "0.3"))          # 间隔抖动系数（0~1）
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")                   # 日志级别
STATS_FILE = os.getenv("STATS_FILE", "/app/data/stats.json")   # 统计文件路径

# ──────────────────────────────────────────────
# 公开测速节点池
# ──────────────────────────────────────────────
SPEED_TEST_SOURCES = [
    # ┌─────────────────────────────────────────────┐
    # │  Cloudflare — 主力源，全球 CDN，动态大小    │
    # └─────────────────────────────────────────────┘
    {"url": "https://speed.cloudflare.com/__down?bytes={size}", "name": "Cloudflare", "type": "dynamic", "priority": 1.0},
    # Cachefly CDN 测速
    {"url": "https://speedtest.cachefly.net/10mb.test", "name": "Cachefly-10MB", "type": "static", "size_mb": 10, "priority": 0.6},
    {"url": "https://speedtest.cachefly.net/100mb.test", "name": "Cachefly-100MB", "type": "static", "size_mb": 100, "priority": 0.6},
    {"url": "https://speedtest.cachefly.net/1000mb.test", "name": "Cachefly-1GB", "type": "static", "size_mb": 1024, "priority": 0.6},
    # OVH 测速
    {"url": "https://proof.ovh.net/files/100Mb.dat", "name": "OVH-100MB", "type": "static", "size_mb": 100, "priority": 0.5},
    {"url": "https://proof.ovh.net/files/1Gb.dat", "name": "OVH-1GB", "type": "static", "size_mb": 1024, "priority": 0.5},
    # Leaseweb
    {"url": "https://mirror.nl.leaseweb.net/speedtest/100mb.bin", "name": "Leaseweb-100MB", "type": "static", "size_mb": 100, "priority": 0.5},
    {"url": "https://mirror.nl.leaseweb.net/speedtest/1000mb.bin", "name": "Leaseweb-1GB", "type": "static", "size_mb": 1024, "priority": 0.5},
    # Scaleway (欧洲)
    {"url": "https://speedtest.scaleway.com/100MB.bin", "name": "Scaleway-100MB", "type": "static", "size_mb": 100, "priority": 0.4},
    {"url": "https://speedtest.scaleway.com/1GB.bin", "name": "Scaleway-1GB", "type": "static", "size_mb": 1024, "priority": 0.4},
    # Hetzner（部分网络环境 DNS 受限，作低优先级备选）
    {"url": "https://speed.hetzner.de/10MB.bin", "name": "Hetzner-10MB", "type": "static", "size_mb": 10, "priority": 0.2},
    {"url": "https://speed.hetzner.de/100MB.bin", "name": "Hetzner-100MB", "type": "static", "size_mb": 100, "priority": 0.2},
    {"url": "https://speed.hetzner.de/1GB.bin", "name": "Hetzner-1GB", "type": "static", "size_mb": 1024, "priority": 0.2},
]

# User-Agent 轮换池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# ──────────────────────────────────────────────
# 日志
# ──────────────────────────────────────────────
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("down-streamer")

# ──────────────────────────────────────────────
# 源健康检查
# ──────────────────────────────────────────────
# 记录每个源的 DNS 是否可达
source_dns_ok: dict[str, bool] = {}

def check_source_dns(source: dict) -> bool:
    """检查下载源域名是否可解析"""
    hostname = urlparse(source["url"]).hostname
    if not hostname:
        return False
    try:
        socket.getaddrinfo(hostname, 443, socket.AF_INET)
        return True
    except (socket.gaierror, socket.herror):
        return False

def probe_all_sources():
    """启动时探测所有源的 DNS 可达性"""
    log.info("🔍 探测下载源 DNS 可达性...")
    for source in SPEED_TEST_SOURCES:
        ok = check_source_dns(source)
        source_dns_ok[source["name"]] = ok
        status = "✓" if ok else "✗"
        log.info(f"  {status} {source['name']}")
    healthy = sum(1 for v in source_dns_ok.values() if v)
    log.info(f"探测完成：{healthy}/{len(SPEED_TEST_SOURCES)} 个源可用")

def refresh_source_dns(source_name: str):
    """重新探测某个源的 DNS（失败后重试用）"""
    for source in SPEED_TEST_SOURCES:
        if source["name"] == source_name:
            ok = check_source_dns(source)
            was_ok = source_dns_ok.get(source_name, False)
            source_dns_ok[source_name] = ok
            if ok and not was_ok:
                log.info(f"🔄 {source_name} DNS 恢复")
            elif not ok and was_ok:
                log.warning(f"🔄 {source_name} DNS 失效")
            break

# ──────────────────────────────────────────────
# 统计
# ──────────────────────────────────────────────
stats = {
    "total_bytes": 0,
    "total_mb": 0.0,
    "total_gb": 0.0,
    "rounds_completed": 0,
    "downloads_ok": 0,
    "downloads_fail": 0,
    "consec_fail": 0,
    "circuit_breaker_trips": 0,
    "started_at": None,
    "last_activity": None,
}

def load_stats():
    p = Path(STATS_FILE)
    if p.exists():
        try:
            with open(p) as f:
                saved = json.load(f)
                stats.update(saved)
                log.info(f"恢复上次统计：已下载 {stats['total_gb']:.2f} GB，完成 {stats['rounds_completed']} 轮")
        except Exception:
            pass

def save_stats():
    p = Path(STATS_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(stats, f, indent=2)

# ──────────────────────────────────────────────
# 下载引擎
# ──────────────────────────────────────────────
def select_source(target_mb: int) -> dict:
    """智能选择下载源：仅从 DNS 可达的源中选择，按权重随机"""
    candidates = []

    # Cloudflare 动态源 — 可精确控制大小
    for s in SPEED_TEST_SOURCES:
        if not source_dns_ok.get(s["name"], False):
            continue
        if s["type"] == "dynamic":
            candidates.append((s, s.get("priority", 1.0) * 1.5))  # 动态源加权
        elif s.get("size_mb", 0) >= target_mb:
            candidates.append((s, s.get("priority", 0.5)))
        else:
            candidates.append((s, s.get("priority", 0.3) * 0.5))

    if not candidates:
        # 极端情况：所有源 DNS 都挂了，强制用 Cloudflare 再试一次
        for s in SPEED_TEST_SOURCES:
            if s["type"] == "dynamic":
                log.warning("所有源 DNS 不可达，尝试 Cloudflare 直连...")
                candidates.append((s, 1.0))
                break

    if not candidates:
        raise RuntimeError("无可用的下载源")

    # 按权重随机选择
    total_weight = sum(w for _, w in candidates)
    r = random.uniform(0, total_weight)
    cumulative = 0
    for source, weight in candidates:
        cumulative += weight
        if r <= cumulative:
            return source

    return candidates[0][0]

def build_url(source: dict, target_mb: int) -> str:
    if source["type"] == "dynamic":
        return source["url"].format(size=target_mb * 1024 * 1024)
    return source["url"]

def download_one(source: dict, target_mb: int) -> int:
    """执行一次下载，返回实际下载字节数"""
    url = build_url(source, target_mb)
    ua = random.choice(USER_AGENTS)

    req = urllib.request.Request(url, headers={
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Encoding": "identity",   # 禁用压缩，确保真实流量
        "Connection": "keep-alive",
    })

    bytes_downloaded = 0
    start = time.monotonic()

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            # 读取并丢弃 — 不存磁盘
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                bytes_downloaded += len(chunk)

                # 如果是静态源且已超过目标，提前终止
                if source["type"] == "static" and source.get("size_mb", 0) > target_mb:
                    if bytes_downloaded >= target_mb * 1024 * 1024:
                        break

                # 超时保护
                if time.monotonic() - start > TIMEOUT_SEC * 3:
                    log.warning(f"下载超时保护触发，已下载 {bytes_downloaded / 1048576:.1f} MB")
                    break

    except urllib.error.HTTPError as e:
        log.warning(f"HTTP {e.code} from {source['name']}: {e.reason}")
        raise
    except urllib.error.URLError as e:
        log.warning(f"连接失败 {source['name']}: {e.reason}")
        # DNS 解析失败的源标记为不可达
        if "Name or service not known" in str(e.reason) or "Name does not resolve" in str(e.reason):
            source_dns_ok[source["name"]] = False
            log.info(f"  → 已标记 {source['name']} 为 DNS 不可达")
        raise
    except Exception as e:
        log.warning(f"下载异常 {source['name']}: {e}")
        raise

    elapsed = time.monotonic() - start
    speed_mbps = (bytes_downloaded * 8) / (elapsed * 1_000_000) if elapsed > 0 else 0
    log.info(
        f"✓ {source['name']} | {bytes_downloaded / 1048576:.1f} MB | "
        f"{elapsed:.1f}s | {speed_mbps:.1f} Mbps"
    )
    return bytes_downloaded

# ──────────────────────────────────────────────
# 电路中断器
# ──────────────────────────────────────────────
circuit_breaker_active = False
circuit_breaker_cooldown = 60  # 秒

def trip_circuit_breaker(reason: str):
    global circuit_breaker_active
    circuit_breaker_active = True
    stats["circuit_breaker_trips"] += 1
    stats["consec_fail"] = 0
    save_stats()
    log.error(f"🚨 电路中断器触发！原因：{reason}。冷却 {circuit_breaker_cooldown}s 后重试。")

# ──────────────────────────────────────────────
# 主循环
# ──────────────────────────────────────────────
running = True

def handle_signal(signum, frame):
    global running
    log.info("收到终止信号，优雅退出...")
    running = False

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

def dns_preflight():
    """启动前 DNS 预检：确认能解析至少一个下载源域名"""
    test_domains = [
        "speed.cloudflare.com",
        "speedtest.cachefly.net",
        "proof.ovh.net",
    ]
    resolved = 0
    for domain in test_domains:
        try:
            addr = socket.getaddrinfo(domain, 443, socket.AF_INET)
            if addr:
                resolved += 1
                log.info(f"  DNS ✓ {domain} → {addr[0][4][0]}")
        except socket.gaierror as e:
            log.warning(f"  DNS ✗ {domain}: {e}")

    if resolved == 0:
        log.error("❌ DNS 预检全部失败！请检查容器网络和 DNS 配置")
        log.error("   提示：entrypoint.sh 应已覆盖 /etc/resolv.conf")
        sys.exit(1)

    log.info(f"DNS 预检通过：{resolved}/{len(test_domains)} 个域名可解析")


def main():
    global circuit_breaker_active

    log.info("=" * 60)
    log.info("⚡ Down-Streamer v1.1 启动")
    log.info(f"  间隔: {INTERVAL_SEC}s | 每轮: {TARGET_MB}MB | 超时: {TIMEOUT_SEC}s")
    log.info(f"  抖动: {JITTER_PCT*100:.0f}% | 连续失败上限: {MAX_CONSEC_FAIL}")
    log.info(f"  总量上限: {'无限' if MAX_TOTAL_GB == 0 else f'{MAX_TOTAL_GB} GB'}")
    log.info("=" * 60)

    # DNS 预检
    dns_preflight()

    # 探测所有源
    probe_all_sources()

    # 确保至少有一个源可用
    available = sum(1 for v in source_dns_ok.values() if v)
    if available == 0:
        log.error("❌ 所有下载源不可达！退出。")
        sys.exit(1)

    load_stats()
    stats["started_at"] = stats.get("started_at") or datetime.now(timezone.utc).isoformat()

    # DNS 刷新计数器 — 每 20 轮重新探测一次 DNS
    dns_refresh_counter = 0

    while running:
        # 电路中断器冷却
        if circuit_breaker_active:
            log.info(f"电路中断器激活中，等待 {circuit_breaker_cooldown}s...")
            for _ in range(circuit_breaker_cooldown):
                if not running:
                    break
                time.sleep(1)
            circuit_breaker_active = False
            log.info("电路中断器重置，恢复下载。")
            # 重新探测 DNS
            probe_all_sources()

        # 检查总量上限
        if MAX_TOTAL_GB > 0 and stats["total_gb"] >= MAX_TOTAL_GB:
            log.info(f"🎯 已达总量上限 {MAX_TOTAL_GB} GB，停止。")
            break

        # 定期刷新 DNS 状态
        dns_refresh_counter += 1
        if dns_refresh_counter >= 20:
            dns_refresh_counter = 0
            probe_all_sources()

        # 选择下载源
        source = select_source(TARGET_MB)
        log.info(f"→ 轮次 {stats['rounds_completed'] + 1} | 选择源: {source['name']} | 目标: {TARGET_MB} MB")

        try:
            downloaded = download_one(source, TARGET_MB)

            if downloaded < 1024:  # 小于 1KB 视为失败
                raise ValueError(f"下载量异常: {downloaded} bytes")

            stats["total_bytes"] += downloaded
            stats["total_mb"] = stats["total_bytes"] / 1048576
            stats["total_gb"] = stats["total_bytes"] / 1073741824
            stats["rounds_completed"] += 1
            stats["downloads_ok"] += 1
            stats["consec_fail"] = 0
            stats["last_activity"] = datetime.now(timezone.utc).isoformat()

            log.info(
                f"📊 累计: {stats['total_gb']:.2f} GB | "
                f"成功: {stats['downloads_ok']} | "
                f"失败: {stats['downloads_fail']}"
            )

        except Exception:
            stats["downloads_fail"] += 1
            stats["consec_fail"] += 1

            # DNS 失败的源，尝试刷新其 DNS 状态
            refresh_source_dns(source["name"])

            if stats["consec_fail"] >= MAX_CONSEC_FAIL:
                trip_circuit_breaker(
                    f"连续 {stats['consec_fail']} 次下载失败"
                )

        save_stats()

        # 间隔 + 抖动
        jitter = INTERVAL_SEC * JITTER_PCT
        sleep_time = INTERVAL_SEC + random.uniform(-jitter, jitter)
        sleep_time = max(5, sleep_time)  # 最少 5 秒

        log.info(f"⏳ 等待 {sleep_time:.1f}s ...")
        # 可中断的等待
        for _ in range(int(sleep_time)):
            if not running:
                break
            time.sleep(1)

    # 优雅退出
    save_stats()
    log.info(f"🏁 退出。累计下载: {stats['total_gb']:.2f} GB，共 {stats['rounds_completed']} 轮")

if __name__ == "__main__":
    main()
