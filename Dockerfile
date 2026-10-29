FROM python:3.12-slim

WORKDIR /app

# 日志/控制台立即可见, 不写 __pycache__
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 只装这 3 个运行依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_value_added_orders.py .

# binlog 断点位点 + 运行日志落在这里; 建议挂载卷持久化:
#   docker run -v tracking-sync-logs:/app/logs ...
RUN mkdir -p /app/logs

# 首次运行: 自动全量回填目标表 -> 断点续传实时监听(前台常驻)
# 自检: docker run --rm --env-file .env <image> python sync_value_added_orders.py --check
CMD ["python", "sync_value_added_orders.py"]
