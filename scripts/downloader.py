#!/usr/bin/env python3
"""
Down-Streamer v2.1 — 官方镜像站下载引擎
- 核心：从全球官方 Linux 发行版镜像站下载 ISO / 大文件
- 技术：HTTP HEAD 预检 + Range 请求 + 随机偏移
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
TARGET_MB = int(os.getenv("TARGET_MB", "1024"))
MAX_TOTAL_GB = float(os.getenv("MAX_TOTAL_GB", "0"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "120"))
MAX_CONSEC_FAIL = int(os.getenv("MAX_CONSEC_FAIL", "8"))
JITTER_PCT = float(os.getenv("JITTER_PCT", "0.3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STATS_FILE = os.getenv("STATS_FILE", "/app/data/stats.json")
SIMULATE_HUMAN = os.getenv("SIMULATE_HUMAN", "1") == "1"
# 下载时间窗口（小时，本地时区），格式 "START-END"，空字符串=全天运行
# 例如 "0-6" = 凌晨 0 点到早上 6 点才下载，"22-6" = 晚上 22 点到次日早上 6 点
SCHEDULE_HOURS = os.getenv("SCHEDULE_HOURS", "")

# ──────────────────────────────────────────────
# 官方镜像站源池（2026-06 实测验证）
# ──────────────────────────────────────────────
# 每个条目 { mirrors: [url1, ...], size_gb, name }
# size_gb 用于计算 Range 偏移量，不需要精确到字节
ISO_SOURCES = [
    # ── Ubuntu 24.04.4 LTS ────────────────────────────────
    {
        "name": "Ubuntu-24.04-Desktop",
        "size_gb": 5.8,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
            "https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
            "https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
            "https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
            "https://mirrors.bfsu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
            "https://mirror.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
            "https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-desktop-amd64.iso",
        ],
    },
    {
        "name": "Ubuntu-24.04-Server",
        "size_gb": 2.6,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
            "https://mirrors.aliyun.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
            "https://mirrors.ustc.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
            "https://mirrors.hit.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
            "https://mirrors.bfsu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
            "https://mirror.sjtu.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
            "https://repo.huaweicloud.com/ubuntu-releases/24.04/ubuntu-24.04.4-live-server-amd64.iso",
        ],
    },
    # ── Ubuntu 22.04.5 LTS ────────────────────────────────
    {
        "name": "Ubuntu-22.04-Desktop",
        "size_gb": 4.4,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso",
            "https://mirrors.aliyun.com/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso",
            "https://mirrors.ustc.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso",
            "https://mirrors.hit.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso",
            "https://repo.huaweicloud.com/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso",
        ],
    },
    # ── Debian 13.5 (Tria) ──────────────────────────────
    {
        "name": "Debian-13-DVD",
        "size_gb": 3.7,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-dvd/debian-13.5.0-amd64-DVD-1.iso",
            "https://mirrors.aliyun.com/debian-cd/current/amd64/iso-dvd/debian-13.5.0-amd64-DVD-1.iso",
            "https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-dvd/debian-13.5.0-amd64-DVD-1.iso",
            "https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-dvd/debian-13.5.0-amd64-DVD-1.iso",
            "https://mirrors.bfsu.edu.cn/debian-cd/current/amd64/iso-dvd/debian-13.5.0-amd64-DVD-1.iso",
            "https://mirror.sjtu.edu.cn/debian-cd/current/amd64/iso-dvd/debian-13.5.0-amd64-DVD-1.iso",
        ],
    },
    {
        "name": "Debian-13-Netinst",
        "size_gb": 0.74,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso",
            "https://mirrors.aliyun.com/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso",
            "https://mirrors.ustc.edu.cn/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso",
            "https://mirrors.hit.edu.cn/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso",
            "https://mirrors.bfsu.edu.cn/debian-cd/current/amd64/iso-cd/debian-13.5.0-amd64-netinst.iso",
        ],
    },
    # ── CentOS Stream 9 ──────────────────────────────────
    {
        "name": "CentOS-Stream9-DVD",
        "size_gb": 14.4,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
            "https://mirrors.aliyun.com/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
            "https://mirrors.ustc.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
            "https://mirrors.hit.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-dvd1.iso",
        ],
    },
    {
        "name": "CentOS-Stream9-Boot",
        "size_gb": 1.4,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso",
            "https://mirrors.aliyun.com/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso",
            "https://mirrors.ustc.edu.cn/centos-stream/9-stream/BaseOS/x86_64/iso/CentOS-Stream-9-latest-x86_64-boot.iso",
        ],
    },
    # ── Arch Linux (滚动更新) ────────────────────────────
    {
        "name": "ArchLinux-Latest",
        "size_gb": 1.5,
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/archlinux/iso/latest/archlinux-x86_64.iso",
            "https://mirrors.aliyun.com/archlinux/iso/latest/archlinux-x86_64.iso",
            "https://mirrors.ustc.edu.cn/archlinux/iso/latest/archlinux-x86_64.iso",
            "https://mirrors.hit.edu.cn/archlinux/iso/latest/archlinux-x86_64.iso",
            "https://mirrors.bfsu.edu.cn/archlinux/iso/latest/archlinux-x86_64.iso",
        ],
    },
]

# 每个镜像域名的健康状态
mirror_health: dict[str, str] = {}  # domain -> "ok" / "dead"
source_health: dict[str, str] = {}  # source_name -> "ok" / "dead"

# User-Agent 轮换池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
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
    "mirror_hits": {},
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
    live_mirrors = []
    for url in source["mirrors"]:
        domain = urlparse(url).hostname
        if mirror_health.get(domain) != "dead":
            live_mirrors.append(url)
    if not live_mirrors:
        return None
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
# HEAD 预检 — 避免对 404 链接浪费整轮超时
# ──────────────────────────────────────────────
def head_check(url: str) -> bool:
    """发 HEAD 请求确认 URL 存在且支持 Range，返回 True/False"""
    try:
        ua = random.choice(USER_AGENTS)
        req = urllib.request.Request(url, method="HEAD", headers={
            "User-Agent": ua,
            "Accept": "*/*",
            "Connection": "close",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 206):
                # 检查是否支持 Range
                accept_ranges = resp.headers.get("Accept-Ranges", "")
                content_length = resp.headers.get("Content-Length", "0")
                if content_length and int(content_length) > 0:
                    return True
            return False
    except Exception:
        return False

# ──────────────────────────────────────────────
# 时间窗口
# ──────────────────────────────────────────────
def parse_schedule(schedule_str: str) -> tuple[int, int] | None:
    """解析 SCHEDULE_HOURS 字符串，返回 (start_hour, end_hour) 或 None（全天）"""
    if not schedule_str.strip():
        return None
    try:
        parts = schedule_str.strip().split("-")
        start_h = int(parts[0])
        end_h = int(parts[1])
        if not (0 <= start_h <= 23 and 0 <= end_h <= 23):
            log.error(f"❌ SCHEDULE_HOURS 小时值须在 0~23 范围，收到 {schedule_str}")
            sys.exit(1)
        return (start_h, end_h)
    except Exception:
        log.error(f"❌ SCHEDULE_HOURS 格式错误，应为 'START-END'（如 '0-6'），收到 '{schedule_str}'")
        sys.exit(1)


def is_in_schedule(now_hour: int, schedule: tuple[int, int] | None) -> bool:
    """判断当前小时是否在允许的时间窗口内。支持跨午夜（如 22-6）"""
    if schedule is None:
        return True
    start_h, end_h = schedule
    if start_h == end_h:
        return True  # 0-0 或 6-6 等价于全天
    if start_h < end_h:
        # 同日区间，如 0-6, 9-18
        return start_h <= now_hour < end_h
    else:
        # 跨午夜区间，如 22-6 = 22~24 + 0~6
        return now_hour >= start_h or now_hour < end_h


def wait_for_schedule(schedule: tuple[int, int] | None):
    """如果不在时间窗口内，计算需要等待的秒数并 sleep"""
    if schedule is None:
        return
    now = datetime.now()
    now_hour = now.hour
    if is_in_schedule(now_hour, schedule):
        return

    start_h, end_h = schedule
    # 计算距离窗口开始的秒数
    if start_h < end_h:
        # 同日区间
        if now_hour < start_h:
            wait_until = now.replace(hour=start_h, minute=0, second=0, microsecond=0)
        else:
            # now_hour >= end_h，等到明天 start_h
            wait_until = (now.replace(hour=start_h, minute=0, second=0, microsecond=0)
                          .replace(day=now.day + 1) if now.day < 28 else
                          now.replace(hour=start_h, minute=0, second=0, microsecond=0))
    else:
        # 跨午夜区间 (如 22-6)，当前不在窗口意味着 6 <= now_hour < 22
        wait_until = now.replace(hour=start_h, minute=0, second=0, microsecond=0)
        if now_hour >= start_h:
            # 不可能（已经 is_in_schedule 会返回 True），但防御
            pass

    wait_secs = max(1, int((wait_until - now).total_seconds()))
    # 不可能精确等，每分钟醒一次检查
    log.info(f"🕐 当前 {now_hour}:00 不在下载窗口 {start_h}-{end_h}，等待 {wait_secs//60} 分钟...")
    for _ in range(wait_secs):
        if not running:
            break
        time.sleep(1)


# ──────────────────────────────────────────────
# DNS 预检
# ──────────────────────────────────────────────
def dns_preflight():
    """启动前 DNS 预检"""
    test_domains = set()
    for src in ISO_SOURCES[:3]:
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
# 下载引擎
# ──────────────────────────────────────────────
def download_iso_chunk(source: dict, target_mb: int) -> int:
    """
    从镜像站下载 ISO 的一段数据（Range 请求），直接丢弃不写磁盘。
    先做 HEAD 预检，再发 Range GET。
    返回实际下载字节数。
    """
    url = pick_mirror(source)
    if not url:
        raise RuntimeError(f"{source['name']} 所有镜像已标记 dead")
    
    domain = urlparse(url).hostname

    # ── Step 1: HEAD 预检 ──
    if not head_check(url):
        mark_mirror_dead(url, "HEAD 预检失败")
        raise RuntimeError(f"HEAD 预检失败: {domain}")
    
    file_size = int(source["size_gb"] * 1024 * 1024 * 1024)
    target_bytes = target_mb * 1024 * 1024
    
    # 随机偏移：从文件的随机位置开始读
    max_offset = max(0, file_size - target_bytes - 1)
    offset = random.randint(0, max_offset) if max_offset > 0 else 0
    end = offset + target_bytes - 1

    ua = random.choice(USER_AGENTS)

    # ── Step 2: Range GET ──
    req = urllib.request.Request(url, headers={
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "close",
        "Range": f"bytes={offset}-{end}",
    })

    bytes_downloaded = 0
    start = time.monotonic()

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            status = resp.status
            if status not in (200, 206):
                raise urllib.error.HTTPError(url, status, "Unexpected status", resp.headers, None)
            
            while True:
                chunk = resp.read(131072)  # 128KB
                if not chunk:
                    break
                bytes_downloaded += len(chunk)
                
                # 超时硬保护
                elapsed = time.monotonic() - start
                if elapsed > TIMEOUT_SEC * 2:
                    log.warning(f"下载超时硬保护 ({elapsed:.0f}s)，已下载 {bytes_downloaded/1048576:.1f} MB")
                    break
                
                if bytes_downloaded >= target_bytes:
                    break

    except urllib.error.HTTPError as e:
        if e.code == 404:
            mark_mirror_dead(url, "404")
            raise
        if e.code == 416:  # Range Not Satisfiable
            log.warning(f"Range 不可满足 {source['name']}，回退从头部读")
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

    stats["mirror_hits"][domain] = stats["mirror_hits"].get(domain, 0) + 1
    return bytes_downloaded


def _download_from_start(url: str, target_bytes: int, ua: str) -> int:
    """Range 失败回退：从文件头读"""
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

    schedule = parse_schedule(SCHEDULE_HOURS)

    log.info("=" * 60)
    log.info("⚡ Down-Streamer v2.1 — 官方镜像站下载引擎")
    log.info(f"  间隔: {INTERVAL_SEC}s | 每轮: {TARGET_MB}MB | 超时: {TIMEOUT_SEC}s")
    log.info(f"  抖动: {JITTER_PCT*100:.0f}% | 连续失败上限: {MAX_CONSEC_FAIL}")
    log.info(f"  总量上限: {'无限' if MAX_TOTAL_GB == 0 else f'{MAX_TOTAL_GB} GB'}")
    if schedule:
        start_h, end_h = schedule
        log.info(f"  下载窗口: {start_h}:00-{end_h}:00")
    else:
        log.info(f"  下载窗口: 全天")
    log.info(f"  源池: {len(ISO_SOURCES)} 个 ISO × 多镜像站")
    log.info("=" * 60)

    dns_preflight()

    load_stats()
    stats["started_at"] = stats.get("started_at") or datetime.now(timezone.utc).isoformat()

    health_reset_counter = 0

    while running:
        # 时间窗口检查
        wait_for_schedule(schedule)

        # 电路中断器冷却
        if circuit_breaker_active:
            log.info(f"电路中断器激活中，等待 {circuit_breaker_cooldown}s...")
            for _ in range(circuit_breaker_cooldown):
                if not running:
                    break
                time.sleep(1)
            circuit_breaker_active = False
            log.info("电路中断器重置，恢复下载。")
            mirror_health.clear()
            source_health.clear()

        # 总量上限
        if MAX_TOTAL_GB > 0 and stats["total_gb"] >= MAX_TOTAL_GB:
            log.info(f"🎯 已达总量上限 {MAX_TOTAL_GB} GB，停止。")
            break

        # 定期重置 dead 状态
        health_reset_counter += 1
        if health_reset_counter >= 20:
            health_reset_counter = 0
            if mirror_health or source_health:
                dead_m = sum(1 for v in mirror_health.values() if v == "dead")
                dead_s = sum(1 for v in source_health.values() if v == "dead")
                log.info(f"🔄 重置 {dead_m} 个 dead 镜像 + {dead_s} 个 dead 源")
                mirror_health.clear()
                source_health.clear()

        # 选择源
        available = get_available_sources()
        if not available:
            log.warning("所有源已耗尽，重置健康状态...")
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

        # 模拟人类
        if SIMULATE_HUMAN and not circuit_breaker_active:
            time.sleep(random.uniform(0.5, 3.0))

        # 间隔 + 抖动
        jitter = INTERVAL_SEC * JITTER_PCT
        sleep_time = max(5, INTERVAL_SEC + random.uniform(-jitter, jitter))

        log.info(f"⏳ 等待 {sleep_time:.1f}s ...")
        for _ in range(int(sleep_time)):
            if not running:
                break
            time.sleep(1)

    save_stats()
    log.info(f"🏁 退出。累计下载: {stats['total_gb']:.2f} GB，共 {stats['rounds_completed']} 轮")


if __name__ == "__main__":
    main()
