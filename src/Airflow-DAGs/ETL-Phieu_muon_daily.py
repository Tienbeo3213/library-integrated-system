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
    conn_libol = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.150.6;'  # Địa chỉ IP của SQL Server
        'DATABASE=libol;'         # Tên cơ sở dữ liệu
        'UID=itc;'                # Tên đăng nhập
        'PWD=spkt@2025;'
    )
    conn_dwh_library = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=172.22.24.232;' # Địa chỉ IP của SQL Server
        'DATABASE=DWH_Lib;' # Tên cơ sở dữ liệu
        'UID=sa;'              # Tên đăng nhập
        'PWD=spkt@2025;'
    )

    today = datetime.now()
    
    # Query bảng An_pham_cho_muon
    query_Anphamchomuon = f"""
        SELECT Tai_lieu_ID,
            Ma_xep_gia, 
            So_the_ID,
            Ngay_muon,
            Ngay_tra,
            So_luot_gia_han,
            Note
        FROM An_pham_cho_muon
        WHERE CONVERT(DATE, Ngay_muon) = '{today}'
    """
    df_apcm = pd.read_sql(query_Anphamchomuon, conn_libol)

    # Query bảng Lich_su_muon_sach
    query_Lichsumuonsach = f"""
        SELECT Tai_lieu_ID,
            Ma_xep_gia, 
            So_the_ID,
            Ngay_muon,
            Ngay_tra,
            So_ngay_qua_han,
            Tien_phat
        FROM Lich_su_muon_sach
        WHERE CONVERT(DATE, Ngay_muon) = '{today}'
    """
    df_lscm = pd.read_sql(query_Lichsumuonsach, conn_libol)

    if df_apcm.empty and df_lscm.empty:
        return

    # Xử lý data
    ## Xử lý cột còn thiếu cho 2 bảng
    df_apcm['So_ngay_qua_han'] = None
    df_apcm['Tien_phat'] = None
    df_apcm['Ngay_tra'] = None     # chỉnh sửa cho ngày trả là None hết vì chưa trả sách
    df_lscm['So_luot_gia_han'] = None
    df_lscm['Note'] = None

    ## Gộp 2 bảng lại
    df_phieumuon = pd.concat([df_lscm, df_apcm], ignore_index=True)
    df_phieumuon = df_phieumuon.sort_values(by='Ngay_muon', ascending=True).reset_index(drop=True) # sắp xếp lại cho dễ nhìn
    query_MaxID = "SELECT MAX(ID_phieu_muon) AS MaxID FROM oltp.Phieu_muon_sach" # Lấy giá trị MaxID từ bảng FACT_Phieu_muon_sach
    df_MaxID = pd.read_sql(query_MaxID, conn_dwh_library)
    max_id = int(df_MaxID['MaxID'].iloc[0]) if not df_MaxID.empty else 0 # Giá trị khởi tạo ID mới, bắt đầu từ MaxID + 1
    start_id = max_id + 1
    df_phieumuon.insert(0, 'ID', range(start_id, start_id + len(df_phieumuon))) # Thêm cột ID mới đếm từ MaxID + 1
    df_phieumuon.rename(columns={'Tai_lieu_ID': 'ID_tai_lieu'}, inplace=True)

    ## Xử lý NaN và ""
    df_phieumuon = df_phieumuon.replace('', None)
    df_phieumuon = df_phieumuon.replace(np.nan, None)

    ## Xử lý kiểu Date
    query_date = "SELECT Date_key FROM olap.DIM_Date"
    df_date = pd.read_sql(query_date, conn_dwh_library)
    date_ids = set(df_date['Date_key'])
    # chuyển date về dang int 
    # kiểm tra nhưng ngày đó có tồn tại trong date_key của bảng DIM_date hay không ?
    df_phieumuon['Ngay_muon'] = pd.to_datetime(df_phieumuon['Ngay_muon'], errors='coerce')
    df_phieumuon['Ngay_tra'] = pd.to_datetime(df_phieumuon['Ngay_tra'], errors='coerce')
    df_phieumuon['Ngay_muon'] = df_phieumuon['Ngay_muon'].apply(lambda x: int(x.strftime('%Y%m%d')) if pd.notna(x) and int(x.strftime('%Y%m%d')) in date_ids else 0)
    df_phieumuon['Ngay_tra'] = df_phieumuon['Ngay_tra'].apply(lambda x: int(x.strftime('%Y%m%d')) if pd.notna(x) and int(x.strftime('%Y%m%d')) in date_ids else 0)

    ## Xử lý ID_tai_lieu
    query_Tailieu = "SELECT ID_tai_lieu FROM olap.DIM_Tai_lieu"
    df_tailieu = pd.read_sql(query_Tailieu, conn_dwh_library)
    tailieu_ids = set(df_tailieu['ID_tai_lieu'])
    # chuyển date về dang int 
    # kiểm tra nhưng ngày đó có tồn tại trong ID_tai_lieu của bảng DIM_Tai_lieu hay không ?
    df_phieumuon['ID_tai_lieu'] = df_phieumuon['ID_tai_lieu'].apply(lambda x: x if pd.notna(x) and x in tailieu_ids else 0)

    ## Xử lý ID_xep_gia
    query_Xepgia = "SELECT ID_xep_gia, ID_tai_lieu, Ma_xep_gia FROM olap.DIM_Xep_gia"
    df_xepgia = pd.read_sql(query_Xepgia, conn_dwh_library)
    # Gán ID_xep_gia từ df_xep_gia vào df_phieu_muon_sach
    df_phieumuon['ID_xep_gia'] = None
    df_phieumuon['ID_xep_gia'] = df_phieumuon.apply(lambda row: df_xepgia.loc[
                                                    (df_xepgia['ID_tai_lieu'] == row['ID_tai_lieu']) & 
                                                    (df_xepgia['Ma_xep_gia'] == row['Ma_xep_gia']), 
                                                    'ID_xep_gia'
                                                    ].iloc[0] if not df_xepgia[
                                                        (df_xepgia['ID_tai_lieu'] == row['ID_tai_lieu']) & 
                                                        (df_xepgia['Ma_xep_gia'] == row['Ma_xep_gia'])
                                                        ].empty else 0,
                                                        axis=1)

    ## Xử lý ID_ban_doc
    ### Đọc từ libol để đổi ID sang So_the
    query_Ban_doc = "SELECT ID, dbo.DecodeUTF8String(So_the) AS So_the FROM Ban_doc"
    df_bandoc = pd.read_sql(query_Ban_doc, conn_libol)

    df_phieumuon['ID_ban_doc'] = None
    df_phieumuon['ID_ban_doc'] = df_phieumuon['So_the_ID'].apply(
                                                                lambda x: df_bandoc.loc[df_bandoc['ID'] == x, 
                                                                                        'So_the'].iloc[0] 
                                                                if not df_bandoc[df_bandoc['ID'] == x].empty else 0)

    ### Kiểm tra lại so db dwh_lib
    query_Bandoc = "SELECT ID_ban_doc FROM olap.DIM_Ban_doc"
    df_bandoc = pd.read_sql(query_Bandoc, conn_dwh_library)
    bandoc_ids = set(df_bandoc['ID_ban_doc'])
    # chuyển date về dang int 
    # kiểm tra nhưng ngày đó có tồn tại trong ID_tai_lieu của bảng DIM_Tai_lieu hay không ?
    df_phieumuon['ID_ban_doc'] = df_phieumuon['ID_ban_doc'].apply(lambda x: x if pd.notna(x) and x in bandoc_ids else 0)


    # Load data
    cursor_dwh = conn_dwh_library.cursor()
    insert_query = """
                    INSERT INTO oltp.Phieu_muon_sach (
                        ID_phieu_muon, 
                        ID_tai_lieu, ID_xep_gia,
                        ID_ban_doc,
                        Ngay_muon, Ngay_tra,
                        So_luot_gia_han, So_ngay_qua_han,
                        Tien_phat, Ghi_chu
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """
    data_to_insert = [
        (
            row['ID'], 
            row['ID_tai_lieu'], row['ID_xep_gia'], 
            row['ID_ban_doc'], 
            row['Ngay_muon'], row['Ngay_tra'],
            row['So_luot_gia_han'], row['So_ngay_qua_han'],
            row['Tien_phat'], row['Note']
        )
        for index, row in df_phieumuon.iterrows()
    ]
    cursor_dwh.executemany(insert_query, data_to_insert)    # Sử dụng executemany để chèn dữ liệu cùng lúc
    conn_dwh_library.commit() # Commit thay đổi
    cursor_dwh.close() # Đóng cursor và kết nối
    conn_dwh_library.close()


# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'oltp_pms_daily',
    default_args=default_args,
    description='etl phiếu mượn sách hằng ngày',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 10, 21, 0),
    catchup=False,
    tags=['etl'],
) as dag:

    # Task thực thi
    task_fetch_and_process = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_data_and_process,
    )
