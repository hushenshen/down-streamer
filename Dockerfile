FROM python:3.12-alpine

LABEL maintainer="down-streamer"
LABEL description="Down-Streamer — 多源轮换 / 电路中断 / 智能调度"

# 安装 ca-certificates（HTTPS 证书验证）+ wget（DNS 验证用）
RUN apk add --no-cache ca-certificates tzdata wget && \
    update-ca-certificates

WORKDIR /app

# 复制脚本
COPY scripts/downloader.py /app/downloader.py
COPY scripts/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# 数据卷：统计文件持久化
VOLUME ["/app/data"]

# 健康检查：如果统计文件超过 10 分钟未更新则标记为 unhealthy
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD test $(find /app/data/stats.json -mmin -10 2>/dev/null | wc -l) -gt 0 || exit 1

# 必须以 root 运行，因为 entrypoint 需要修改 /etc/resolv.conf
# 容器内仅运行下载脚本，已通过 deploy.resources 限制资源
ENTRYPOINT ["/app/entrypoint.sh"]
