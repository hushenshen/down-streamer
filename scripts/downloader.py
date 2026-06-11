#!/usr/bin/env python3
"""
Down-Streamer v1.2
- 源质量分级：Cloudflare 主力 + 静态源偶尔探测，自动降级慢源/不可达源
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
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "30"))
TARGET_MB = int(os.getenv("TARGET_MB", "100"))
MAX_TOTAL_GB = float(os.getenv("MAX_TOTAL_GB", "0"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "60"))
MAX_CONSEC_FAIL = int(os.getenv("MAX_CONSEC_FAIL", "5"))
JITTER_PCT = float(os.getenv("JITTER_PCT", "0.3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATS_FILE = os.getenv("STATS_FILE", "/app/data/stats.json")
PROBE_PCT = float(os.getenv("PROBE_PCT", "0.1"))  # 每轮探测备用源的概率 (10%)

# ──────────────────────────────────────────────
# 公开测速节点池
# ──────────────────────────────────────────────
SPEED_TEST_SOURCES = [
    # ┌─────────────────────────────────────────────────────┐
    # │  Cloudflare — 全球 CDN 边缘，主力源，动态大小      │
    # │  几乎所有网络环境都能访问，作为核心流量来源         │
    # └─────────────────────────────────────────────────────┘
    {"url": "https://speed.cloudflare.com/__down?bytes={size}", "name": "Cloudflare", "type": "dynamic"},
    # Cachefly CDN（备选）
    {"url": "https://speedtest.cachefly.net/10mb.test", "name": "Cachefly-10MB", "type": "static", "size_mb": 10},
    {"url": "https://speedtest.cachefly.net/100mb.test", "name": "Cachefly-100MB", "type": "static", "size_mb": 100},
    {"url": "https://speedtest.cachefly.net/1000mb.test", "name": "Cachefly-1GB", "type": "static", "size_mb": 1024},
    # OVH（备选）
    {"url": "https://proof.ovh.net/files/100Mb.dat", "name": "OVH-100MB", "type": "static", "size_mb": 100},
    {"url": "https://proof.ovh.net/files/1Gb.dat", "name": "OVH-1GB", "type": "static", "size_mb": 1024},
    # Leaseweb（备选）
    {"url": "https://mirror.nl.leaseweb.net/speedtest/100mb.bin", "name": "Leaseweb-100MB", "type": "static", "size_mb": 100},
    {"url": "https://mirror.nl.leaseweb.net/speedtest/1000mb.bin", "name": "Leaseweb-1GB", "type": "static", "size_mb": 1024},
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
# 源健康状态
# ──────────────────────────────────────────────
# 三级状态：ok / slow / dead
SOURCE_STATUS_OK = "ok"
SOURCE_STATUS_SLOW = "slow"      # 速度 < 1 Mbps
SOURCE_STATUS_DEAD = "dead"      # DNS 失败 / 连接被拒 / 404

source_health: dict[str, dict] = {}  # name -> {status, last_check, fail_count}

def init_source_health():
    """初始化所有源的健康状态"""
    for s in SPEED_TEST_SOURCES:
        source_health[s["name"]] = {
            "status": SOURCE_STATUS_OK,
            "last_check": 0,
            "fail_count": 0,
        }

def mark_source(source_name: str, status: str):
    """更新源的健康状态"""
    h = source_health.get(source_name)
    if not h:
        return
    old = h["status"]
    h["status"] = status
    h["last_check"] = time.monotonic()
    if status == SOURCE_STATUS_DEAD:
        h["fail_count"] += 1
    if old != status:
        log.info(f"  🔄 {source_name}: {old} → {status}")

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
    """探测所有源的 DNS 可达性，更新健康状态"""
    log.info("🔍 探测下载源...")
    for source in SPEED_TEST_SOURCES:
        ok = check_source_dns(source)
        if ok:
            mark_source(source["name"], SOURCE_STATUS_OK)
        else:
            mark_source(source["name"], SOURCE_STATUS_DEAD)
        status_icon = "✓" if ok else "✗"
        log.info(f"  {status_icon} {source['name']}")
    ok_count = sum(1 for h in source_health.values() if h["status"] != SOURCE_STATUS_DEAD)
    log.info(f"探测完成：{ok_count}/{len(SPEED_TEST_SOURCES)} 个源可用")

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
    """选择下载源：Cloudflare 主力 (1-PROBE_PCT)，备用源探测 (PROBE_PCT)"""
    # 判断这轮是否探测备用源
    is_probe = random.random() < PROBE_PCT

    if not is_probe:
        # 主力模式：只用 Cloudflare（最可靠）
        for s in SPEED_TEST_SOURCES:
            if s["type"] == "dynamic":
                h = source_health.get(s["name"])
                if h and h["status"] != SOURCE_STATUS_DEAD:
                    return s

    # 探测模式或 Cloudflare 挂了：从非 dead 的备用源中随机选
    backup_candidates = []
    for s in SPEED_TEST_SOURCES:
        if s["type"] == "dynamic":
            continue  # 备用模式不选 Cloudflare
        h = source_health.get(s["name"])
        if not h or h["status"] == SOURCE_STATUS_DEAD:
            continue
        # slow 的源降低权重
        weight = 0.3 if h["status"] == SOURCE_STATUS_SLOW else 1.0
        backup_candidates.append((s, weight))

    if backup_candidates:
        total = sum(w for _, w in backup_candidates)
        r = random.uniform(0, total)
        cumul = 0
        for source, weight in backup_candidates:
            cumul += weight
            if r <= cumul:
                return source
        return backup_candidates[0][0]

    # 所有备用源都挂了，回退 Cloudflare
    for s in SPEED_TEST_SOURCES:
        if s["type"] == "dynamic":
            return s

    raise RuntimeError("无可用的下载源")


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
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    })

    bytes_downloaded = 0
    start = time.monotonic()

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                bytes_downloaded += len(chunk)

                # 静态源超过目标大小，提前终止
                if source["type"] == "static" and source.get("size_mb", 0) > target_mb:
                    if bytes_downloaded >= target_mb * 1024 * 1024:
                        break

                # 超时硬保护（TIMEOUT_SEC * 2，不是 *3）
                elapsed_so_far = time.monotonic() - start
                if elapsed_so_far > TIMEOUT_SEC * 2:
                    log.warning(f"下载超时硬保护触发 ({elapsed_so_far:.0f}s)，已下载 {bytes_downloaded / 1048576:.1f} MB")
                    break

                # 慢速检测：前 5 秒内速度 < 0.5 Mbps → 标记为 slow 并放弃
                if bytes_downloaded >= 65536 and elapsed_so_far > 5:
                    speed_mbps = (bytes_downloaded * 8) / (elapsed_so_far * 1_000_000)
                    if speed_mbps < 0.5:
                        log.warning(f"🐌 {source['name']} 速度过慢 ({speed_mbps:.2f} Mbps)，放弃本轮")
                        mark_source(source["name"], SOURCE_STATUS_SLOW)
                        raise ValueError(f"速度过慢: {speed_mbps:.2f} Mbps")

    except urllib.error.HTTPError as e:
        log.warning(f"HTTP {e.code} from {source['name']}: {e.reason}")
        if e.code in (404, 410, 451):
            mark_source(source["name"], SOURCE_STATUS_DEAD)
        raise
    except urllib.error.URLError as e:
        reason = str(e.reason)
        log.warning(f"连接失败 {source['name']}: {e.reason}")
        # DNS 失败 / 网络不可达 → 标记 dead
        if any(kw in reason for kw in [
            "Name does not resolve", "Name or service not known",
            "Network unreachable", "No route to host",
            "Connection refused", "bad address",
        ]):
            mark_source(source["name"], SOURCE_STATUS_DEAD)
        raise
    except (OSError, IOError) as e:
        reason = str(e)
        log.warning(f"IO 异常 {source['name']}: {e}")
        if "Network unreachable" in reason or "No route" in reason:
            mark_source(source["name"], SOURCE_STATUS_DEAD)
        raise
    except ValueError:
        raise  # slow 检测抛出的
    except Exception as e:
        log.warning(f"下载异常 {source['name']}: {e}")
        raise

    elapsed = time.monotonic() - start
    speed_mbps = (bytes_downloaded * 8) / (elapsed * 1_000_000) if elapsed > 0 else 0

    # 下载完成但速度慢 → 标记 slow
    if speed_mbps < 1.0 and source["type"] != "dynamic":
        mark_source(source["name"], SOURCE_STATUS_SLOW)

    log.info(
        f"✓ {source['name']} | {bytes_downloaded / 1048576:.1f} MB | "
        f"{elapsed:.1f}s | {speed_mbps:.1f} Mbps"
    )
    return bytes_downloaded

# ──────────────────────────────────────────────
# 电路中断器
# ──────────────────────────────────────────────
circuit_breaker_active = False
circuit_breaker_cooldown = 60

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
    """启动前 DNS 预检"""
    test_domains = ["speed.cloudflare.com"]
    for domain in test_domains:
        try:
            addr = socket.getaddrinfo(domain, 443, socket.AF_INET)
            if addr:
                log.info(f"  DNS ✓ {domain} → {addr[0][4][0]}")
        except socket.gaierror as e:
            log.error(f"  DNS ✗ {domain}: {e}")
            log.error("❌ Cloudflare DNS 解析失败！无法启动。")
            sys.exit(1)

    log.info("DNS 预检通过")


def main():
    global circuit_breaker_active

    log.info("=" * 60)
    log.info("⚡ Down-Streamer v1.2 启动")
    log.info(f"  间隔: {INTERVAL_SEC}s | 每轮: {TARGET_MB}MB | 超时: {TIMEOUT_SEC}s")
    log.info(f"  抖动: {JITTER_PCT*100:.0f}% | 连续失败上限: {MAX_CONSEC_FAIL}")
    log.info(f"  总量上限: {'无限' if MAX_TOTAL_GB == 0 else f'{MAX_TOTAL_GB} GB'}")
    log.info("=" * 60)

    # DNS 预检
    dns_preflight()

    # 初始化源健康状态
    init_source_health()

    # 探测所有源（仅作信息展示，不阻止启动）
    probe_all_sources()

    load_stats()
    stats["started_at"] = stats.get("started_at") or datetime.now(timezone.utc).isoformat()

    # DNS 刷新计数器 — 每 50 轮重新探测一次
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
            probe_all_sources()

        # 检查总量上限
        if MAX_TOTAL_GB > 0 and stats["total_gb"] >= MAX_TOTAL_GB:
            log.info(f"🎯 已达总量上限 {MAX_TOTAL_GB} GB，停止。")
            break

        # 定期刷新 DNS 状态
        dns_refresh_counter += 1
        if dns_refresh_counter >= 50:
            dns_refresh_counter = 0
            probe_all_sources()

        # 选择下载源
        source = select_source(TARGET_MB)
        round_num = stats["rounds_completed"] + 1
        is_probe = source["type"] != "dynamic"
        probe_tag = " [探测]" if is_probe else ""
        log.info(f"→ 轮次 {round_num}{probe_tag} | 源: {source['name']} | 目标: {TARGET_MB} MB")

        try:
            downloaded = download_one(source, TARGET_MB)

            if downloaded < 1024:
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
        sleep_time = max(5, sleep_time)

        log.info(f"⏳ 等待 {sleep_time:.1f}s ...")
        for _ in range(int(sleep_time)):
            if not running:
                break
            time.sleep(1)

    # 优雅退出
    save_stats()
    log.info(f"🏁 退出。累计下载: {stats['total_gb']:.2f} GB，共 {stats['rounds_completed']} 轮")

if __name__ == "__main__":
    main()
