from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime


# Hàm kết nối và xử lý dữ liệu
def fetch_data_and_process():
    # Tạo kết nối
    conn_dwh_library = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=172.22.24.232;' # Địa chỉ IP của SQL Server
        'DATABASE=DWH_Lib;' # Tên cơ sở dữ liệu
        'UID=sa;'              # Tên đăng nhập
        'PWD=spkt@2025;')

    today = datetime.now()
    date_key = int(today.strftime('%Y%m%d'))
    # Đọc data từ SQL Server
    query_phieumuon = f"""SELECT PMS.ID_phieu_muon, PMS.ID_ban_doc, PMS.ID_xep_gia, PMS.Ngay_muon
                        FROM oltp.Phieu_muon_sach PMS
                            JOIN olap.DIM_Ban_doc BD ON PMS.ID_ban_doc = BD.ID_ban_doc
                            JOIN olap.DIM_Xep_gia XG ON PMS.ID_xep_gia = XG.ID_xep_gia
                            WHERE PMS.Ngay_muon = {date_key}"""

    df_phieumuon = pd.read_sql(query_phieumuon, conn_dwh_library) 

    if df_phieumuon.empty:
        return
    
    # Xử lý data
    So_luot_dung = df_phieumuon.groupby(['ID_ban_doc','ID_xep_gia', 'Ngay_muon'])['ID_phieu_muon'].count().reset_index()
    So_luot_dung = So_luot_dung.rename(columns={'ID_phieu_muon': 'So_luot_dung'})

    # load vào datawarehouse
    cursor_dwh = conn_dwh_library.cursor()
    insert_query = """
                    INSERT INTO olap.FACT_Thu_vien (ID_ban_doc, ID_xep_gia, ID_date, So_luot_dung) 
                    VALUES (?, ?, ?, ?)
                    """
    for index, row in So_luot_dung.iterrows():
        values = (row['ID_ban_doc'], 
                row['ID_xep_gia'],
                row['Ngay_muon'],
                row['So_luot_dung'])
        cursor_dwh.execute(insert_query, values)
    conn_dwh_library.commit()

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'etl_fact_thu_vien_daily',
    default_args=default_args,
    description='Load data vào bảng Thống Kê Thư Viện',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 10, 22, 00),
    catchup=False,
    tags=['etl_daily'],
) as dag:

    # Task thực thi
    task_fetch_and_process = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_data_and_process,
    )
