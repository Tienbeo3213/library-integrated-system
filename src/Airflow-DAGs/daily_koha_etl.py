import sys
import logging
from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator

# --- Paths & bins ---
APP_PATH    = '/home/lib/rs_flask'
CONFIG_PATH = '/home/lib/airflow/dags'
PYTHON_BIN  = f"{APP_PATH}/venv/bin/python"

# --- Logging for DAG file ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def build_operator(fn, task_id, **op_kwargs):
    return ExternalPythonOperator(
        task_id=task_id,
        python=PYTHON_BIN,
        use_dill=False,
        python_callable=fn,
        op_kwargs=op_kwargs,
    )

with DAG(
    dag_id='daily_koha_incremental_etl_knn',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 1 * * *',     # run daily at 01:00
    catchup=False,
    default_args={'owner':'airflow', 'retries':1, 'retry_delay':timedelta(minutes=5)},
    tags=['koha','etl','knn'],
) as dag:

    def extract_new_books(app_path: str, config_path: str, start_date: str, end_date: str):
        import sys, logging, pandas as pd
        from sqlalchemy import create_engine
        sys.path.insert(0, app_path)
        sys.path.insert(0, config_path)
        import config

        logger = logging.getLogger('extract_new_books')
        logger.info(f"🟢 [extract] Lấy sách từ {start_date} đến {end_date}")

        engine = create_engine(config.DB_CONN_STR)
        sql = f"""
            SELECT
                biblionumber AS item_id,
                title,
                author,
                timestamp
            FROM biblio
            WHERE timestamp >= '{start_date} 00:00:00'
              AND timestamp <  '{end_date} 00:00:00'
            ORDER BY timestamp
        """
        df = pd.read_sql_query(sql, engine)
        df['timestamp'] = df['timestamp'].astype(str)
        logger.info(f"🔹 [extract] Đã lấy {len(df)} bản ghi")
        return df.to_dict('records')

    def transform_books(app_path: str, config_path: str, records):
        import sys, logging, pandas as pd, ast, json
        sys.path.insert(0, app_path)
        sys.path.insert(0, config_path)
        import config
        from app import preprocess_text

        logger = logging.getLogger('transform_books')
        logger.info('🟢 [transform] Bắt đầu tiền xử lý')

        # parse nếu string
        if isinstance(records, str):
            try:
                records = ast.literal_eval(records)
            except:
                records = json.loads(records)
        if not records:
            logger.warning('⚠️ Không có bản ghi nào')
            return []

        df = pd.DataFrame(records)
        # 1) Author NULL -> Unknown
        df['author'] = df['author'].fillna('Unknown').astype(str)
        # 2) Bỏ author toàn số
        mask_auth_num = df['author'].str.strip().str.isdigit()
        df = df[~mask_auth_num]
        # 3) Bỏ title toàn số
        df['title'] = df['title'].fillna('').astype(str)
        mask_title_num = df['title'].str.strip().str.isdigit()
        df = df[~mask_title_num]
        # 4) Bỏ title quá dài
        mask_too_long = df['title'].str.len() > config.MAX_TITLE_LEN
        df = df[~mask_too_long]
        # 5) Drop trùng
        df = df.drop_duplicates(subset=['title','author'])
        # 6) Tiền xử lý text
        df['ptitle']  = df['title'].apply(preprocess_text)
        df['pauthor'] = df['author'].apply(preprocess_text)

        logger.info(f"🔹 [transform] Xử lý xong {len(df)} bản ghi còn lại")
        return df.to_dict('records')

    def load_embeddings(app_path: str, config_path: str, transformed):
        import sys, logging, pandas as pd, numpy as np, pickle, ast, json
        sys.path.insert(0, app_path)
        sys.path.insert(0, config_path)
        import config
        from app import embedding_vietnamese, recommender

        logger = logging.getLogger('load_embeddings')
        logger.info('🟢 [load] Tính embedding')

        if isinstance(transformed, str):
            try:
                transformed = ast.literal_eval(transformed)
            except:
                transformed = json.loads(transformed)
        if not transformed:
            logger.warning('⚠️ Không có dữ liệu để embed')
            return 'no_data'

        df = pd.DataFrame(transformed)
        contents = (df['ptitle'] + ' ' + df['pauthor']).tolist()
        new_embs = embedding_vietnamese(contents)

        mat = np.load(config.EMBEDDING_MATRIX_PATH)
        with open(config.MAPPING_FILE_PATH, 'rb') as f:
            m = pickle.load(f)
        idx2id = m['index_to_itemid']; id2idx = m['itemid_to_index']

        start = mat.shape[0]
        mat_new = np.vstack([mat, new_embs])
        for i, iid in enumerate(df['item_id'].astype(int), start=start):
            idx2id[i] = iid
            id2idx[iid] = i

        # lưu vào prod
        np.save(config.EMBEDDING_MATRIX_PATH, mat_new)
        with open(config.MAPPING_FILE_PATH, 'wb') as f:
            pickle.dump({'index_to_itemid': idx2id, 'itemid_to_index': id2idx}, f)
        logger.info(f'✔️ Đã thêm {len(new_embs)} embeddings vào prod')

        # cập nhật runtime
        recommender.embedding_matrix = mat_new
        recommender.knn_model.fit(mat_new)
        return 'ok'

    t1 = build_operator(
        extract_new_books, 'extract_new_books',
        app_path=APP_PATH, config_path=CONFIG_PATH,
        start_date="{{ macros.ds_add(ds, -1) }}",
        end_date="{{ ds }}",
    )

    t2 = build_operator(
        transform_books, 'transform_books',
        app_path=APP_PATH, config_path=CONFIG_PATH,
        records="{{ ti.xcom_pull(task_ids='extract_new_books') }}",
    )

    t3 = build_operator(
        load_embeddings, 'load_embeddings',
        app_path=APP_PATH, config_path=CONFIG_PATH,
        transformed="{{ ti.xcom_pull(task_ids='transform_books') }}",
    )

    t1 >> t2 >> t3
