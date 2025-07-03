from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyodbc
import pandas as pd
import numpy as np


# Hàm kết nối và xử lý dữ liệu
def fetch_data_and_process():
    # Tạo kết nối
    conn_dwh_library = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.150.6;' # Địa chỉ IP của SQL Server
        'DATABASE=DWH_Lib;' # Tên cơ sở dữ liệu
        'UID=itc;'              # Tên đăng nhập
        'PWD=spkt@2025;')

    # Đọc data từ SQL Server
    query_phieumuon = """SELECT TL.ID_tai_lieu, XG.ID_xep_gia, Ma_tai_lieu, Ngay_giao_dich 
                        FROM olap.DIM_Tai_lieu TL
                        JOIN olap.DIM_Xep_gia XG ON TL.ID_tai_lieu = XG.ID_tai_lieu"""
    df_phieumuon = pd.read_sql(query_phieumuon, conn_dwh_library)
    
    # Xử lý data
    so_ban_sach = df_phieumuon.groupby(['ID_tai_lieu', 'Ma_tai_lieu', 'Ngay_giao_dich'])['ID_xep_gia'].count().reset_index()
    so_ban_sach = so_ban_sach.rename(columns={'ID_xep_gia': 'So_ban_sach'})

    # load vào datawarehouse
    cursor_dwh = conn_dwh_library.cursor()
    insert_query = """
                    INSERT INTO olap.FACT_Tai_lieu (ID_tai_lieu,
                                                    Ma_xep_gia, 
                                                    ID_date, 
                                                    So_ban_sach)
                    VALUES (?, ?, ?, ?)"""
    for index, row in so_ban_sach.iterrows():
    # Trích xuất giá trị từ các cột
        values = (row['ID_tai_lieu'],
                row['Ma_tai_lieu'],
                row['Ngay_giao_dich'],
                row['So_ban_sach'])  # Nếu cột này có tên đúng
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
    'etl_fact_tai_lieu',
    default_args=default_args,
    description='Load data vào bảng Thống Kê Tài Liệu',
    schedule_interval='@once',
    start_date=datetime(2025, 3, 27, 14, 40),
    catchup=False,
    tags=['etl'],
) as dag:

    # Task thực thi
    task_fetch_and_process = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_data_and_process,
    )
