# Real-Time Fraud Detection (Kafka + Postgres)

Этот проект основан на сервисе с семинара №5. Я переобучил свою модель (улучшил метрику с ДЗ‑1) и добавил фичи из текущего задания: потоковый скоринг из Kafka, запись результатов в PostgreSQL и отображение в UI.

## Структура сервиса
- Kafka‑поток: чтение транзакций из `transactions`, запись результатов в `scoring`
- ML‑сервис: препроцессинг + inference на CPU
- PostgreSQL: витрина `fraud_scores` (transaction_id, score, fraud_flag, created_at)
- Consumer `score_writer`: читает `scoring` и пишет в Postgres
- UI (Streamlit): загрузка CSV + просмотр результатов

## Быстрый старт
```bash
docker-compose up --build
```

Порты:
- UI: http://localhost:8501
- Kafka UI: http://localhost:8080
- PostgreSQL: localhost:5432

## Проверка работы
1) Загрузить `test_500.csv` через UI и отправить в Kafka.  
2) В UI нажать «Посмотреть результаты»:
   - 10 последних fraud‑транзакций
   - гистограмма последних 100 скоров
3) Проверка в Postgres:
```bash
docker-compose exec postgres psql -U fraud_user -d fraud_db -c "select count(*) from fraud_scores;"
```

## Замечания
- На `test_500` у меня фроды обнаруживаются, в UI отображаются, в Postgres записываются.
