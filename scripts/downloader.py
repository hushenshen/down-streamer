#!/usr/bin/env python3
"""
Down-Streamer v1.0
- 多源轮换：从全球公开测速节点下载，避免单点封锁
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
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

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
# 公开测速节点池（全球合法测速文件）
# ──────────────────────────────────────────────
SPEED_TEST_SOURCES = [
    # Cloudflare 边缘节点测速文件（动态大小，最可靠）
    {"url": "https://speed.cloudflare.com/__down?bytes={size}", "name": "Cloudflare", "type": "dynamic"},
    # Hetzner 全球测速
    {"url": "https://speed.hetzner.de/1GB.bin", "name": "Hetzner-1GB", "type": "static", "size_mb": 1024},
    {"url": "https://speed.hetzner.de/100MB.bin", "name": "Hetzner-100MB", "type": "static", "size_mb": 100},
    {"url": "https://speed.hetzner.de/10MB.bin", "name": "Hetzner-10MB", "type": "static", "size_mb": 10},
    # Vultr 测速（HTTPS）
    {"url": "https://nj-us-ping.vultr.com/vultr.com.1GB.bin", "name": "Vultr-NJ-1GB", "type": "static", "size_mb": 1024},
    {"url": "https://nj-us-ping.vultr.com/vultr.com.100MB.bin", "name": "Vultr-NJ-100MB", "type": "static", "size_mb": 100},
    # Cachefly CDN 测速
    {"url": "https://speedtest.cachefly.net/1mb.test", "name": "Cachefly-1MB", "type": "static", "size_mb": 1},
    {"url": "https://speedtest.cachefly.net/10mb.test", "name": "Cachefly-10MB", "type": "static", "size_mb": 10},
    {"url": "https://speedtest.cachefly.net/100mb.test", "name": "Cachefly-100MB", "type": "static", "size_mb": 100},
    {"url": "https://speedtest.cachefly.net/1000mb.test", "name": "Cachefly-1GB", "type": "static", "size_mb": 1024},
    # OVH 测速
    {"url": "https://proof.ovh.net/files/100Mb.dat", "name": "OVH-100MB", "type": "static", "size_mb": 100},
    {"url": "https://proof.ovh.net/files/1Gb.dat", "name": "OVH-1GB", "type": "static", "size_mb": 1024},
    # Leaseweb
    {"url": "https://mirror.nl.leaseweb.net/speedtest/1000mb.bin", "name": "Leaseweb-1GB", "type": "static", "size_mb": 1024},
    {"url": "https://mirror.nl.leaseweb.net/speedtest/100mb.bin", "name": "Leaseweb-100MB", "type": "static", "size_mb": 100},
    # Bouygues Telecom (法国)
    {"url": "https://speedtest.bouygues.box.fr/1G.iso", "name": "Bouygues-1GB", "type": "static", "size_mb": 1024},
    {"url": "https://speedtest.bouygues.box.fr/100M.iso", "name": "Bouygues-100MB", "type": "static", "size_mb": 100},
    # Scaleway (欧洲)
    {"url": "https://speedtest.scaleway.com/1GB.bin", "name": "Scaleway-1GB", "type": "static", "size_mb": 1024},
    {"url": "https://speedtest.scaleway.com/100MB.bin", "name": "Scaleway-100MB", "type": "static", "size_mb": 100},
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
    """智能选择下载源：优先匹配目标大小，加随机轮换"""
    candidates = []

    # Cloudflare 动态源 — 可精确控制大小
    for s in SPEED_TEST_SOURCES:
        if s["type"] == "dynamic":
            candidates.append((s, 1.0))  # 最高优先级

    # 静态源 — 选大小 >= target 的
    for s in SPEED_TEST_SOURCES:
        if s["type"] == "static" and s.get("size_mb", 0) >= target_mb:
            candidates.append((s, 0.8))

    # 如果没有足够大的静态源，退而求其次
    if not any(c[1] == 0.8 for c in candidates):
        for s in SPEED_TEST_SOURCES:
            if s["type"] == "static":
                candidates.append((s, 0.5))

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

def main():
    global circuit_breaker_active

    log.info("=" * 60)
    log.info("⚡ Down-Streamer v1.0 启动")
    log.info(f"  间隔: {INTERVAL_SEC}s | 每轮: {TARGET_MB}MB | 超时: {TIMEOUT_SEC}s")
    log.info(f"  抖动: {JITTER_PCT*100:.0f}% | 连续失败上限: {MAX_CONSEC_FAIL}")
    log.info(f"  总量上限: {'无限' if MAX_TOTAL_GB == 0 else f'{MAX_TOTAL_GB} GB'}")
    log.info("=" * 60)

    load_stats()
    stats["started_at"] = stats.get("started_at") or datetime.now(timezone.utc).isoformat()

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

        # 检查总量上限
        if MAX_TOTAL_GB > 0 and stats["total_gb"] >= MAX_TOTAL_GB:
            log.info(f"🎯 已达总量上限 {MAX_TOTAL_GB} GB，停止。")
            break

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
