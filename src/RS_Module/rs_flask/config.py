# config.py
# Đường dẫn tới dữ liệu sách và lịch sử mượn
BOOKS_PATH = '/home/lib/rs_flask/data/clean_data_CB.csv'
USER_RATING_PATH = '/home/lib/rs_flask/model/userBorrowDict.pkl'

# Đường dẫn lưu trữ file embedding và mapping
EMBEDDING_MATRIX_PATH = '/home/lib/rs_flask/model/embedding_matrix.npy'
MAPPING_FILE_PATH = '/home/lib/rs_flask/model/mapping.pkl'

# Cấu hình SBERT: sử dụng model đã được train cho tiếng Việt
SBERT_MODEL_NAME = 'dangvantuan/vietnamese-embedding'

DB_CONN_STR = 'mysql+pymysql://koha_reader:SpKt%402024@172.22.24.223:3306/koha_library?charset=utf8mb4'
MAX_TITLE_LEN = 512
