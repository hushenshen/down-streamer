#!/bin/sh
# Down-Streamer v2.0 入口脚本
# 修复 Docker Desktop for Windows 的 DNS 代理转发故障
# Docker 内部 DNS 代理 (127.0.0.11) 无法正确转发外部查询，
# 需要直接覆盖 /etc/resolv.conf 绕过它

set -e

echo "[entrypoint] 修复 DNS 配置..."

# 用真实 DNS 服务器覆盖 Docker 损坏的 resolv.conf
cat > /etc/resolv.conf <<EOF
nameserver 8.8.8.8
nameserver 1.1.1.1
nameserver 223.5.5.5
EOF

echo "[entrypoint] DNS 已配置为 8.8.8.8 / 1.1.1.1 / 223.5.5.5"

# 快速验证 DNS 是否可用（用清华镜像站验证）
if wget -q --spider --timeout=5 https://mirrors.tuna.tsinghua.edu.cn 2>/dev/null; then
    echo "[entrypoint] DNS 验证通过 ✓（清华镜像可达）"
else
    echo "[entrypoint] ⚠ DNS 验证失败，但继续运行（可能网络延迟）"
fi

echo "[entrypoint] 启动 Down-Streamer v2.0..."
exec python3 -u /app/downloader.py
