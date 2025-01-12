FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_processor.py .

USER 1001

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0", \
     "--master", "spark://spark-master:7077", \
     "spark_processor.py"]