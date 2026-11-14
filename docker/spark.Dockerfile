# Spark 3.5.1 (Scala 2.12, Java 17, Python 3.10) + the Python libraries our jobs need
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir happybase==1.2.0 kafka-python-ng==2.2.3
ENV PYTHONPATH=/app
WORKDIR /app
USER spark
