import streamlit as st
import pandas as pd
import altair as alt
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_pass")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

def fetch_recent_frauds(conn, limit=10):
    query = """
        SELECT transaction_id, score, fraud_flag, created_at
        FROM fraud_scores
        WHERE fraud_flag = 1
        ORDER BY created_at DESC
        LIMIT %s
    """
    return pd.read_sql_query(query, conn, params=(limit,))


def fetch_recent_scores(conn, limit=100):
    query = """
        SELECT score
        FROM fraud_scores
        ORDER BY created_at DESC
        LIMIT %s
    """
    return pd.read_sql_query(query, conn, params=(limit,))


# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.divider()
st.header("📊 Результаты скоринга")

if st.button("Посмотреть результаты"):
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
    except Exception as e:
        st.error(f"Не удалось подключиться к базе: {e}")
    else:
        frauds_df = fetch_recent_frauds(conn, limit=10)
        if frauds_df.empty:
            st.info("Нет записей с fraud_flag = 1.")
        else:
            st.subheader("Последние 10 fraud-транзакций")
            st.dataframe(frauds_df, use_container_width=True)

        scores_df = fetch_recent_scores(conn, limit=100)
        if scores_df.empty:
            st.info("Нет данных для построения гистограммы.")
        else:
            st.subheader("Гистограмма скоров последних 100 транзакций")
            chart = alt.Chart(scores_df).mark_bar().encode(
                alt.X("score:Q", bin=alt.Bin(maxbins=20), title="Score"),
                alt.Y("count()", title="Count")
            )
            st.altair_chart(chart, use_container_width=True)

        conn.close()
