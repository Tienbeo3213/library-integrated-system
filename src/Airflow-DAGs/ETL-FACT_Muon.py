from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyodbc
import pandas as pd
import numpy as np


# Hàm kết nối và xử lý dữ liệu
def fetch_data_and_process():
    # Tạo kết nối
    conn_172 = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=172.22.24.232;'  # Địa chỉ IP của SQL Server
        'DATABASE=DWH_Lib;'         # Tên cơ sở dữ liệu
        'UID=sa;'                # Tên đăng nhập
        'PWD=spkt@2025;'
    )   

    # Đọc data từ SQL Server
    query_phieumuon = """SELECT PMS.ID_phieu_muon, 
				                    PMS.ID_ban_doc, 
				                    PMS.ID_tai_lieu, 
				                    Ma_xep_gia, 
				                    Ngay_muon,
				                    Ngay_tra,
				                    So_luot_gia_han,
				                    So_ngay_qua_han
                    FROM oltp.Phieu_muon_sach PMS
                        JOIN olap.DIM_Ban_doc BD ON BD.ID_ban_doc = PMS.ID_ban_doc
                        JOIN olap.DIM_Xep_gia XG ON XG.ID_xep_gia = PMS.ID_xep_gia"""
    df_phieumuon = pd.read_sql(query_phieumuon, conn_172)
    
    # So_luot_muon
    so_luot_muon = df_phieumuon.groupby(['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'])['ID_phieu_muon'].count().reset_index()
    so_luot_muon = so_luot_muon.rename(columns={'ID_phieu_muon': 'So_luot_muon'})
    
    # So_luot_da_hoan_tra: Ngay_tra != 0
    df_phieumuon_da_tra = df_phieumuon[df_phieumuon['Ngay_tra'] != 0]
    so_luot_da_tra = df_phieumuon_da_tra.groupby(['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'])['ID_phieu_muon'].count().reset_index()
    so_luot_da_tra = so_luot_da_tra.rename(columns={'ID_phieu_muon': 'So_luot_da_hoan_tra'})

    # So_luot_khong_hoan_tra: Ngay_tra == 0
    df_phieumuon_khong_tra = df_phieumuon[df_phieumuon['Ngay_tra'] == 0]
    so_luot_khong_tra = df_phieumuon_khong_tra.groupby(['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'])['ID_phieu_muon'].count().reset_index()
    so_luot_khong_tra = so_luot_khong_tra.rename(columns={'ID_phieu_muon': 'So_luot_khong_hoan_tra'})

    # So_luot_qua_han: So_luot_gia_han not null
    df_phieumuon_qua_han = df_phieumuon[df_phieumuon['So_luot_gia_han'].notnull()]
    so_luot_qua_han = df_phieumuon_qua_han.groupby(['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'])['ID_phieu_muon'].count().reset_index()
    so_luot_qua_han = so_luot_qua_han.rename(columns={'ID_phieu_muon': 'So_luot_qua_han'})

    # Gộp tất cả lại
    df_final = so_luot_muon \
        .merge(so_luot_da_tra, on=['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'], how='left') \
        .merge(so_luot_khong_tra, on=['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'], how='left') \
        .merge(so_luot_qua_han, on=['ID_ban_doc', 'ID_tai_lieu', 'Ma_xep_gia', 'Ngay_muon'], how='left')

    # Fill các NaN bằng 0
    df_final = df_final.fillna(0).astype({
        'So_luot_da_hoan_tra': int,
        'So_luot_khong_hoan_tra': int,
        'So_luot_qua_han': int
    })

    # Load vào datawarehouse
    cursor_dwh = conn_172.cursor()

    insert_query = """
        INSERT INTO olap.FACT_Muon (
            ID_ban_doc,
            ID_tai_lieu, 
            Ma_xep_gia, 
            ID_date, 
            So_luot_muon,
            So_luot_da_hoan_tra,
            So_luot_khong_hoan_tra,
            So_luot_qua_han
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for index, row in df_final.iterrows():
        values = (
            row['ID_ban_doc'],
            row['ID_tai_lieu'],
            row['Ma_xep_gia'],
            row['Ngay_muon'],  # Đây là ID_date
            int(row['So_luot_muon']),
            int(row.get('So_luot_da_hoan_tra', 0)),
            int(row.get('So_luot_khong_hoan_tra', 0)),
            int(row.get('So_luot_qua_han', 0))
        )
        cursor_dwh.execute(insert_query, values)

    conn_172.commit()
    conn_172.close()

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'etl_fact_muon',
    default_args=default_args,
    description='Load data vào bảng Thống Kê Mượn',
    schedule_interval='@once',
    start_date=datetime(2025, 5, 30, 13, 30),
    catchup=False,
    tags=['etl'],
) as dag:

    # Task thực thi
    task_fetch_and_process = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_data_and_process,
    )
