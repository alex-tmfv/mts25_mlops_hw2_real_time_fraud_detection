import json
import logging
import os
import time

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_pass")


def connect_with_retry():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.autocommit = False
            logger.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("PostgreSQL connection failed: %s", exc)
            time.sleep(3)


def normalize_payload(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    raise ValueError("Unsupported payload type")


def extract_rows(records):
    rows = []
    for record in records:
        try:
            transaction_id = str(record["transaction_id"])
            score = float(record["score"])
            fraud_flag = int(record["fraud_flag"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Skipping malformed record: %s", exc)
            continue
        rows.append((transaction_id, score, fraud_flag))
    return rows


def insert_rows(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO fraud_scores (transaction_id, score, fraud_flag)
            VALUES %s
            ON CONFLICT (transaction_id)
            DO UPDATE SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag
            """,
            rows
        )
    conn.commit()


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "score-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([SCORING_TOPIC])

    conn = connect_with_retry()

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Kafka error: %s", msg.error())
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            records = normalize_payload(payload)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Invalid message payload: %s", exc)
            consumer.commit(msg, asynchronous=False)
            continue

        rows = extract_rows(records)
        if not rows:
            consumer.commit(msg, asynchronous=False)
            continue

        try:
            insert_rows(conn, rows)
            consumer.commit(msg, asynchronous=False)
        except psycopg2.Error as exc:
            logger.error("PostgreSQL insert failed: %s", exc)
            conn.rollback()
            conn.close()
            conn = connect_with_retry()


if __name__ == "__main__":
    logger.info("Starting score writer service...")
    main()
