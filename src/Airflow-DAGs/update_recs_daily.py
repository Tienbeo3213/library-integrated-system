from datetime import datetime, timedelta
from pathlib import Path
import logging
import pickle

import pandas as pd
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Nếu bạn đã tạo Connection trong Airflow (Admin → Connections) với Conn ID = "libol_sql",
# bạn có thể dùng Airflow Hook thay vì hardcode chuỗi sau.
SQL_CONN_PARAMS = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "SERVER": "192.168.150.6",
    "DATABASE": "libol",
    "UID": "itc",
    "PWD": "spkt@2025",
    "timeout": 30
}

SQL_QUERY_NEW = """
SELECT b.ID AS user_id, l.Tai_lieu_ID AS item_id
FROM Lich_su_muon_sach l
JOIN Ban_doc b ON l.So_the_ID = b.ID
WHERE l.Ngay_muon >= ?  -- yesterday 00:00
  AND l.Ngay_muon <  ?; -- today 00:00
"""

PICKLE_PATH = Path("/home/lib/rs_flask/model/userBorrowDict.pkl")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_new_borrows():
    logger = logging.getLogger("airflow.task")

    # 1) Xác định khoảng thời gian: từ hôm qua 00:00 đến hôm nay 00:00
    today     = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # 2) Đọc dữ liệu mới từ SQL Server
    conn = pyodbc.connect(**SQL_CONN_PARAMS)
    df   = pd.read_sql(SQL_QUERY_NEW, conn, params=[yesterday, today])
    conn.close()

    # 3) Load history (pickle) — chuyển sang dùng set cho mỗi user để check nhanh
    if PICKLE_PATH.exists():
        with PICKLE_PATH.open("rb") as f:
            user_history: dict[int, set[int]] = pickle.load(f)
    else:
        user_history = {}

    # 4) Tìm những borrow mới
    new_records: list[tuple[int,int]] = []
    for _, row in df.iterrows():
        uid  = int(row["user_id"])
        item = int(row["item_id"])
        seen = user_history.setdefault(uid, set())
        if item not in seen:
            seen.add(item)
            new_records.append((uid, item))

    # 5) Chỉ ghi file lại nếu có borrow mới
    if new_records:
        PICKLE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with PICKLE_PATH.open("wb") as f:
            pickle.dump(user_history, f)

        logger.info(f"✅ Added {len(new_records)} new borrows:")
        for uid, item in new_records:
            logger.info(f"   - user {uid} borrowed item {item}")
    else:
        logger.info("🔄 No new borrows found.")

with DAG(
    dag_id="daily_update_borrow_and_retrain",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",    # chạy 00:00 UTC mỗi ngày
    start_date=days_ago(1),
    catchup=False,
    tags=["recommender", "daily"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_borrows",
        python_callable=extract_new_borrows,
    )

    extract_task
