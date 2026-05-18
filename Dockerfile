FROM apache/spark:3.5.0
WORKDIR /app
USER root
# نسخ الملف
COPY spark_streaming_processor.py .
# تشغيل الكود باستخدام النسخة الرسمية
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "spark_streaming_processor.py"]