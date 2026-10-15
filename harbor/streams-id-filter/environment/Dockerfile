FROM apache/spark@sha256:84a4eedb1abcf36a90808d5a1310e3e910b78c85d85aaa1599e31af5f862ed59 AS java
FROM python:3.10.12-slim-bookworm@sha256:4d440b214e447deddc0a94de23a3d97d28dfafdf125a8b4bb8073381510c9ee2
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 KAFKA_BOOTSTRAP_SERVERS=kafka1:9092 REDIS_HOST=redis
RUN groupadd -g 10001 runner && useradd -m -u 10001 -g 10001 -s /bin/bash runner && mkdir -p /app && chown 10001:10001 /app
COPY runtime/ /app/
COPY run_lab.py wire_proxy.py state.py latency.py deduplication.py streams_filter.py /app/
WORKDIR /app
USER runner
