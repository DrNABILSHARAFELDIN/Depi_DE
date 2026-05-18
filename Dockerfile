FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka faker
COPY ecommerce_simulator.py .
CMD ["python", "-u", "ecommerce_simulator.py"]