# Sử dụng Python 3.9 trên nền Debian 12 (Bookworm) bản slim cho nhẹ
FROM python:3.9-slim-bookworm

# Cài đặt Java (default-jre trên bookworm thường là OpenJDK 17 - rất tốt cho Spark) và Cron
RUN apt-get update && \
    apt-get install -y default-jre cron && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt PySpark (phiên bản mới nhất trên PyPI tương thích tốt với Java 17)
RUN pip install pyspark

WORKDIR /app

# Khởi tạo file log để hứng output từ cron
RUN touch /var/log/cron.log

# Copy script python và cấu hình cron vào container
COPY compact_parquet.py /app/compact_parquet.py
COPY crontab /etc/cron.d/compaction-cron

# Cấp quyền chuẩn cho file crontab và apply vào hệ thống cron của root
RUN chmod 0644 /etc/cron.d/compaction-cron && \
    crontab /etc/cron.d/compaction-cron

# Cập nhật đoạn RUN trong Dockerfile của bạn thành như sau:
RUN pip install --upgrade pip && \
    pip install --no-cache-dir pyspark==3.5.0 delta-spark==3.2.0

# Chạy cron ở chế độ foreground (-f) và stream log ra màn hình container
CMD cron -f && tail -f /var/log/cron.log