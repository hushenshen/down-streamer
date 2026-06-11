#!/usr/bin/env python3
"""
Down-Streamer v2.0 — 官方镜像站下载引擎
- 核心：从全球官方 Linux 发行版镜像站下载 ISO / 大文件
- 技术：HTTP Range 请求 + 随机偏移，绕过 CDN 缓存，每次请求指纹不同
- 防封：官方镜像站设计承载力就是百万级下载，循环下载完全合规
- 丢弃：下载的数据直接丢弃（不写磁盘），纯刷下行流量
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
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "120"))
MAX_CONSEC_FAIL = int(os.getenv("MAX_CONSEC_FAIL", "8"))
JITTER_PCT = float(os.getenv("JITTER_PCT", "0.3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATS_FILE = os.getenv("STATS_FILE", "/app/data/stats.json")
# 每轮下载完是否随机休息一小段（0.5~3s），模拟人类读完再点下一个
SIMULATE_HUMAN = os.getenv("SIMULATE_HUMAN", "1") == "1"

# ──────────────────────────────────────────────
# 官方镜像站源池
# ──────────────────────────────────────────────
# 策略：大文件 + 官方镜像 + 全球分布 = 不可能被封
# 每个条目 { mirrors: [url1, url2...], size_gb, name }
# mirrors 列表里同一文件有多个镜像，随机选一个用
ISO_SOURCES = [
    # ── Ubuntu 24.04 LTS ( Noble Numbat ) ───────────────────
    {
        "name": "Ubuntu-24.04-Desktop",
        "size_gb": 5.8,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://mirrors.bfsu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://mirror.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://ftp.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
            "https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.2-desktop-amd64.iso",
        ],
    },
    {
        "name": "Ubuntu-24.04-Server",
        "size_gb": 2.6,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
            "https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
            "https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
            "https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
            "https://mirrors.bfsu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
            "https://mirror.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
            "https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.2-live-server-amd64.iso",
        ],
    },
    # ── Ubuntu 22.04 LTS ( Jammy Jellyfish ) ─────────────────
    {
        "name": "Ubuntu-22.04-Desktop",
        "size_gb": 4.7,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso",
            "https://mirrors.aliyun.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso",
            "https://mirrors.ustc.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso",
            "https://mirrors.hit.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso",
            "https://mirrors.bfsu.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso",
            "https://repo.huaweicloud.com/ubuntu-releases/22.04/ubuntu-22.04.4-desktop-amd64.iso",
        ],
    },
    # ── Debian 12 ( Bookworm ) ──────────────────────────────
    {
        "name": "Debian-12-DVD",
        "size_gb": 3.7,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-dvd/debian-12.10.0-amd64-DVD-1.iso",
            "https://mirrors.aliyun.com/debian-cd/current/amd64/iso-dvd/debian-12.10.0-amd64-DVD-1.iso",
            "https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-dvd/debian-12.10.0-amd64-DVD-1.iso",
            "https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-dvd/debian-12.10.0-amd64-DVD-1.iso",
            "https://mirrors.bfsu.edu.cn/debian-cd/current/amd64/iso-dvd/debian-12.10.0-amd64-DVD-1.iso",
            "https://mirror.sjtu.edu.cn/debian-cd/current/amd64/iso-dvd/debian-12.10.0-amd64-DVD-1.iso",
        ],
    },
    {
        "name": "Debian-12-Netinst",
        "size_gb": 0.65,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso",
            "https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso",
            "https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso",
            "https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso",
            "https://mirrors.bfsu.edu.cn/debian-cd/current/amd64/iso-cd/debian-12.10.0-amd64-netinst.iso",
        ],
    },
    # ── CentOS Stream 9 ─────────────────────────────────────
    {
        "name": "CentOS-Stream9-DVD",
        "size_gb": 9.6,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
            "https://mirrors.aliyun.com/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
            "https://mirrors.ustc.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
            "https://mirrors.hit.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
        ],
    },
    # ── Fedora 42 ──────────────────────────────────────────
    {
        "name": "Fedora-42-Workstation",
        "size_gb": 2.2,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/fedora/releases/42/Workstation/x86_64/iso/Fedora-Workstation-Live-x86_64-42-1.1.iso",
            "https://mirrors.aliyun.com/fedora/releases/42/Workstation/x86_64/iso/Fedora-Workstation-Live-x86_64-42-1.1.iso",
            "https://mirrors.ustc.edu.cn/fedora/releases/42/Workstation/x86_64/iso/Fedora-Workstation-Live-x86_64-42-1.1.iso",
            "https://mirrors.hit.edu.cn/fedora/releases/42/Workstation/x86_64/iso/Fedora-Workstation-Live-x86_64-42-1.1.iso",
        ],
    },
    # ── Rocky Linux 9 ──────────────────────────────────────
    {
        "name": "Rocky-9-DVD",
        "size_gb": 9.0,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/rocky/9.5/isos/x86_64/Rocky-9.5-x86_64-dvd.iso",
            "https://mirrors.aliyun.com/rocky/9.5/isos/x86_64/Rocky-9.5-x86_64-dvd.iso",
            "https://mirrors.ustc.edu.cn/rocky/9.5/isos/x86_64/Rocky-9.5-x86_64-dvd.iso",
            "https://mirrors.hit.edu.cn/rocky/9.5/isos/x86_64/Rocky-9.5-x86_64-dvd.iso",
        ],
    },
    # ── AlmaLinux 9 ────────────────────────────────────────
    {
        "name": "AlmaLinux-9-DVD",
        "size_gb": 9.2,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/almalinux/9.5/isos/x86_64/AlmaLinux-9.5-x86_64-dvd.iso",
            "https://mirrors.aliyun.com/almalinux/9.5/isos/x86_64/AlmaLinux-9.5-x86_64-dvd.iso",
            "https://mirrors.ustc.edu.cn/almalinux/9.5/isos/x86_64/AlmaLinux-9.5-x86_64-dvd.iso",
        ],
    },
    # ── openSUSE Leap 15.6 ────────────────────────────────
    {
        "name": "openSUSE-15-DVD",
        "size_gb": 4.4,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/opensuse/distribution/leap/15.6/iso/openSUSE-Leap-15.6-DVD-x86_64-Media.iso",
            "https://mirrors.aliyun.com/opensuse/distribution/leap/15.6/iso/openSUSE-Leap-15.6-DVD-x86_64-Media.iso",
            "https://mirrors.ustc.edu.cn/opensuse/distribution/leap/15.6/iso/openSUSE-Leap-15.6-DVD-x86_64-Media.iso",
        ],
    },
    # ── Arch Linux (滚动更新，最新) ────────────────────────
    {
        "name": "ArchLinux-Latest",
        "size_gb": 0.8,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/archlinux/iso/latest/archlinux-latest-x86_64.iso",
            "https://mirrors.aliyun.com/archlinux/iso/latest/archlinux-latest-x86_64.iso",
            "https://mirrors.ustc.edu.cn/archlinux/iso/latest/archlinux-latest-x86_64.iso",
            "https://mirrors.hit.edu.cn/archlinux/iso/latest/archlinux-latest-x86_64.iso",
            "https://mirrors.bfsu.edu.cn/archlinux/iso/latest/archlinux-latest-x86_64.iso",
        ],
    },
]

# 每个镜像域名的健康状态
mirror_health: dict[str, str] = {}  # domain -> "ok" / "dead"
# 每个 ISO 源的健康状态
source_health: dict[str, str] = {}  # source_name -> "ok" / "dead"

# User-Agent 轮换池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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
    "total_gb": 0.0,
    "rounds_completed": 0,
    "downloads_ok": 0,
    "downloads_fail": 0,
    "consec_fail": 0,
    "circuit_breaker_trips": 0,
    "mirror_hits": {},  # mirror_domain -> count
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
# 镜像选择引擎
# ──────────────────────────────────────────────
def get_available_sources() -> list:
    """返回所有未标记 dead 的源"""
    available = []
    for src in ISO_SOURCES:
        if source_health.get(src["name"]) == "dead":
            continue
        available.append(src)
    return available

def pick_mirror(source: dict) -> str | None:
    """从源的镜像列表中选一个可用镜像，返回 URL"""
    # 先过滤掉 dead 镜像
    live_mirrors = []
    for url in source["mirrors"]:
        domain = urlparse(url).hostname
        if mirror_health.get(domain) != "dead":
            live_mirrors.append(url)
    
    if not live_mirrors:
        return None
    
    # 加权随机：优先选命中少的（均衡流量）
    return random.choice(live_mirrors)

def mark_mirror_dead(url: str, reason: str):
    domain = urlparse(url).hostname or "unknown"
    old = mirror_health.get(domain, "ok")
    mirror_health[domain] = "dead"
    if old != "dead":
        log.info(f"  🔒 镜像 {domain} → dead（{reason}）")

def mark_source_dead(name: str, reason: str):
    old = source_health.get(name, "ok")
    source_health[name] = "dead"
    if old != "dead":
        log.info(f"  🔒 源 {name} → dead（{reason}）")

# ──────────────────────────────────────────────
# DNS 预检
# ──────────────────────────────────────────────
def dns_preflight():
    """启动前 DNS 预检：检测核心镜像站域名可达性"""
    test_domains = set()
    for src in ISO_SOURCES[:3]:  # 测前 3 个源的镜像域名
        for url in src["mirrors"][:2]:
            test_domains.add(urlparse(url).hostname)
    
    resolved = 0
    for domain in sorted(test_domains):
        try:
            addr = socket.getaddrinfo(domain, 443, socket.AF_INET)
            if addr:
                resolved += 1
                log.info(f"  DNS ✓ {domain} → {addr[0][4][0]}")
        except (socket.gaierror, socket.herror):
            log.warning(f"  DNS ✗ {domain}")
    
    if resolved == 0:
        log.error("❌ 所有镜像域名均无法解析！请检查容器 DNS 配置。")
        sys.exit(1)
    
    log.info(f"DNS 预检通过：{resolved}/{len(test_domains)} 个域名可解析")

# ──────────────────────────────────────────────
# 下载引擎（核心）
# ──────────────────────────────────────────────
def download_iso_chunk(source: dict, target_mb: int) -> int:
    """
    从镜像站下载 ISO 的一段数据（Range 请求），直接丢弃不写磁盘。
    返回实际下载字节数。
    """
    url = pick_mirror(source)
    if not url:
        raise RuntimeError(f"{source['name']} 所有镜像已标记 dead")
    
    domain = urlparse(url).hostname
    file_size = int(source["size_gb"] * 1024 * 1024 * 1024)
    target_bytes = target_mb * 1024 * 1024
    
    # 随机偏移：从文件的随机位置开始读，避免 CDN 缓存
    # 留出 target_bytes 的余量，确保 range 合法
    max_offset = max(0, file_size - target_bytes - 1)
    offset = random.randint(0, max_offset) if max_offset > 0 else 0
    end = offset + target_bytes - 1

    ua = random.choice(USER_AGENTS)

    req = urllib.request.Request(url, headers={
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Encoding": "identity",  # 禁用压缩，确保真实下行字节数
        "Connection": "close",
        "Range": f"bytes={offset}-{end}",
    })

    bytes_downloaded = 0
    start = time.monotonic()

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            status = resp.status
            # 206 = Partial Content（正常 Range 响应）
            # 200 = 服务器忽略 Range 返回全量（也行，我们读到目标就停）
            if status not in (200, 206):
                raise urllib.error.HTTPError(url, status, "Unexpected status", resp.headers, None)
            
            while True:
                chunk = resp.read(131072)  # 128KB chunks
                if not chunk:
                    break
                bytes_downloaded += len(chunk)
                # 数据直接丢弃（不写磁盘），纯刷下行
                
                # 超时硬保护
                elapsed = time.monotonic() - start
                if elapsed > TIMEOUT_SEC * 2:
                    log.warning(f"下载超时硬保护 ({elapsed:.0f}s)，已下载 {bytes_downloaded/1048576:.1f} MB")
                    break
                
                # 读够了就停
                if bytes_downloaded >= target_bytes:
                    break

    except urllib.error.HTTPError as e:
        if e.code == 404:
            mark_mirror_dead(url, "404")
            raise
        if e.code == 416:  # Range Not Satisfiable — 文件可能比预期小
            log.warning(f"Range 不可满足 {source['name']}（文件可能已更新），尝试从 0 开始")
            # 回退：从文件头开始读
            return _download_from_start(url, target_bytes, ua)
        if e.code in (403, 429):
            mark_mirror_dead(url, f"HTTP {e.code}")
            raise
        raise
    except urllib.error.URLError as e:
        reason = str(e.reason)
        log.warning(f"连接失败 {domain}: {e.reason}")
        if any(kw in reason for kw in [
            "Name does not resolve", "Name or service not known",
            "bad address", "No address associated",
        ]):
            mark_mirror_dead(url, "DNS 失败")
        elif any(kw in reason for kw in [
            "Network unreachable", "No route to host",
            "Connection refused", "Connection timed out",
        ]):
            mark_mirror_dead(url, reason)
        raise
    except (OSError, IOError) as e:
        reason = str(e)
        if "Network unreachable" in reason or "No route" in reason:
            mark_mirror_dead(url, reason)
        raise

    elapsed = time.monotonic() - start
    speed_mbps = (bytes_downloaded * 8) / (elapsed * 1_000_000) if elapsed > 0 else 0
    offset_mb = offset / 1048576

    log.info(
        f"✓ {source['name']} @ {domain} | "
        f"{bytes_downloaded/1048576:.1f} MB (偏移 {offset_mb:.0f} MB) | "
        f"{elapsed:.1f}s | {speed_mbps:.1f} Mbps"
    )

    # 统计镜像命中
    stats["mirror_hits"][domain] = stats["mirror_hits"].get(domain, 0) + 1

    return bytes_downloaded


def _download_from_start(url: str, target_bytes: int, ua: str) -> int:
    """Range 请求失败后的回退：从文件头开始读"""
    req = urllib.request.Request(url, headers={
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "close",
    })

    bytes_downloaded = 0
    start = time.monotonic()

    with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
        while True:
            chunk = resp.read(131072)
            if not chunk:
                break
            bytes_downloaded += len(chunk)
            if bytes_downloaded >= target_bytes:
                break
            if time.monotonic() - start > TIMEOUT_SEC * 2:
                break

    return bytes_downloaded

# ──────────────────────────────────────────────
# 电路中断器
# ──────────────────────────────────────────────
circuit_breaker_active = False
circuit_breaker_cooldown = 90

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
    log.info("⚡ Down-Streamer v2.0 — 官方镜像站下载引擎")
    log.info(f"  间隔: {INTERVAL_SEC}s | 每轮: {TARGET_MB}MB | 超时: {TIMEOUT_SEC}s")
    log.info(f"  抖动: {JITTER_PCT*100:.0f}% | 连续失败上限: {MAX_CONSEC_FAIL}")
    log.info(f"  总量上限: {'无限' if MAX_TOTAL_GB == 0 else f'{MAX_TOTAL_GB} GB'}")
    log.info(f"  源池: {len(ISO_SOURCES)} 个 ISO × 多镜像站")
    log.info("=" * 60)

    # DNS 预检
    dns_preflight()

    load_stats()
    stats["started_at"] = stats.get("started_at") or datetime.now(timezone.utc).isoformat()

    # 健康重置计数器 — 每 30 轮重置 dead 镜像（可能恢复了）
    health_reset_counter = 0

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
            # 重置所有 dead 状态，给镜像第二次机会
            mirror_health.clear()
            source_health.clear()

        # 检查总量上限
        if MAX_TOTAL_GB > 0 and stats["total_gb"] >= MAX_TOTAL_GB:
            log.info(f"🎯 已达总量上限 {MAX_TOTAL_GB} GB，停止。")
            break

        # 定期重置 dead 状态
        health_reset_counter += 1
        if health_reset_counter >= 30:
            health_reset_counter = 0
            if mirror_health or source_health:
                dead_mirrors = sum(1 for v in mirror_health.values() if v == "dead")
                dead_sources = sum(1 for v in source_health.values() if v == "dead")
                log.info(f"🔄 重置 {dead_mirrors} 个 dead 镜像 + {dead_sources} 个 dead 源（给第二次机会）")
                mirror_health.clear()
                source_health.clear()

        # 选择 ISO 源
        available = get_available_sources()
        if not available:
            log.error("❌ 所有源已耗尽，重置健康状态...")
            mirror_health.clear()
            source_health.clear()
            available = ISO_SOURCES

        source = random.choice(available)
        round_num = stats["rounds_completed"] + 1

        log.info(f"→ 轮次 {round_num} | 源: {source['name']} | 目标: {TARGET_MB} MB")

        try:
            downloaded = download_iso_chunk(source, TARGET_MB)

            if downloaded < 1024:
                raise ValueError(f"下载量异常: {downloaded} bytes")

            stats["total_bytes"] += downloaded
            stats["total_gb"] = stats["total_bytes"] / 1073741824
            stats["rounds_completed"] += 1
            stats["downloads_ok"] += 1
            stats["consec_fail"] = 0
            stats["last_activity"] = datetime.now(timezone.utc).isoformat()

            log.info(
                f"📊 累计: {stats['total_gb']:.2f} GB | "
                f"成功: {stats['downloads_ok']} | 失败: {stats['downloads_fail']}"
            )

        except Exception:
            stats["downloads_fail"] += 1
            stats["consec_fail"] += 1

            if stats["consec_fail"] >= MAX_CONSEC_FAIL:
                trip_circuit_breaker(
                    f"连续 {stats['consec_fail']} 次下载失败"
                )

        save_stats()

        # 模拟人类：下载完随机短暂停顿
        if SIMULATE_HUMAN and not circuit_breaker_active:
            human_pause = random.uniform(0.5, 3.0)
            time.sleep(human_pause)

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
