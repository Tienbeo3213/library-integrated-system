# app.py

import os
import re
import pickle
import logging
import pandas as pd
import numpy as np
import torch
from flask import Flask, request, jsonify
from flask_cors import CORS
from pyvi import ViTokenizer
from pyvi.ViTokenizer import tokenize
from sentence_transformers import SentenceTransformer
from sklearn.neighbors import NearestNeighbors

from config import BOOKS_PATH, USER_RATING_PATH, EMBEDDING_MATRIX_PATH, MAPPING_FILE_PATH, SBERT_MODEL_NAME

# --- Cấu hình logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# --- Các hằng số và hàm tiền xử lý ---
vietnamese_stopwords = {
    "và", "là", "của", "có", "cho", "với", "một",
    "để", "các", "những", "được", "này", "trong", "từ", "bằng"
}

def preprocess_text(text):
    if not isinstance(text, str):
        text = ""
    text = text.lower().strip()
    text = re.sub(
        r"[^a-z0-9\sàáạảãâầấậẩẫăằắặẳẵêềếệểễ"
        r"íìịỉĩóòọỏõôồốộổỗơờớợởỡ"
        r"úùụủũưừứựửữýỳỵỷỹđ]",
        " ", text
    )
    text = re.sub(r"\s+", " ", text).strip()
    tokens = ViTokenizer.tokenize(text).split()
    filtered_tokens = [w for w in tokens if w not in vietnamese_stopwords]
    return " ".join(filtered_tokens)

# --- Lớp quản lý dữ liệu sách ---
class BookData:
    def __init__(self, booksPath):
        self.booksPath = booksPath
        self.df_books = None
        self.itemID_to_processed_title = {}
        self.itemID_to_processed_author = {}
        self.itemID_to_title = {}
        self.itemID_to_author = {}
        self.itemID_to_year = {}

    def loadBooks(self):
        df_books = pd.read_csv(self.booksPath, encoding='utf-8')
        df_books.drop_duplicates(subset=['item_id'], inplace=True)
        # Fill missing values cho title, author, year, item_id
        for col in ['title', 'author']:
            if col not in df_books.columns:
                df_books[col] = ""
            df_books[col] = df_books[col].fillna("")
        if 'year' not in df_books.columns:
            df_books['year'] = 0
        df_books['year'] = df_books['year'].fillna(0).astype(int)
        df_books['item_id'] = df_books['item_id'].fillna(0).astype(int)
        # Tiền xử lý text
        df_books['processed_title'] = df_books['title'].apply(preprocess_text)
        df_books['processed_author'] = df_books['author'].apply(preprocess_text)
        # Tạo các dictionary tra cứu
        for _, row in df_books.iterrows():
            item_id = row['item_id']
            self.itemID_to_processed_title[item_id] = row['processed_title']
            self.itemID_to_processed_author[item_id] = row['processed_author']
            self.itemID_to_title[item_id] = row['title']
            self.itemID_to_author[item_id] = row['author']
            self.itemID_to_year[item_id] = row['year']
        self.df_books = df_books.reset_index(drop=True)
        logger.info(f"Loaded {len(self.df_books)} books from {self.booksPath}")

    def getOriginalTitle(self, item_id):
        return self.itemID_to_title.get(item_id, "Unknown")

# --- Cấu hình thiết bị và tải mô hình SBERT ---
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
logger.info(f"Using device: {device}")
sbert_model = SentenceTransformer(SBERT_MODEL_NAME, device=device)

def embedding_vietnamese(texts):
    """Tokenize văn bản bằng ViTokenizer và encode bằng SBERT."""
    tokenized_texts = [tokenize(text) for text in texts]
    embeddings = sbert_model.encode(tokenized_texts, convert_to_numpy=True, normalize_embeddings=True)
    return embeddings

# --- Lớp gợi ý dựa trên nội dung sử dụng SBERT ---
class ContentBasedRecommenderSBERT:
    def __init__(self, bookData, n_neighbors=30):
        self.bookData = bookData
        self.n_neighbors = n_neighbors
        self.itemid_to_index = {}
        self.index_to_itemid = {}
        self.embedding_matrix = None
        self.knn_model = None

    def build_model(self):
        df_books = self.bookData.df_books.copy()
        contents = []
        # Xây dựng mapping item_id <-> index
        for idx, row in df_books.iterrows():
            item_id = row['item_id']
            full_content = f"{row['processed_title']} {row['processed_author']}".strip()
            contents.append(full_content)
            self.index_to_itemid[idx] = item_id
            self.itemid_to_index[item_id] = idx

        # Kiểm tra nếu file embedding và mapping đã tồn tại
        if os.path.exists(EMBEDDING_MATRIX_PATH) and os.path.exists(MAPPING_FILE_PATH):
            logger.info("Loading embedding matrix and mapping from file...")
            self.embedding_matrix = np.load(EMBEDDING_MATRIX_PATH)
            with open(MAPPING_FILE_PATH, 'rb') as f:
                mapping = pickle.load(f)
                self.index_to_itemid = mapping["index_to_itemid"]
                self.itemid_to_index = mapping["itemid_to_index"]
        else:
            logger.info("Calculating embeddings for all books...")
            self.embedding_matrix = embedding_vietnamese(contents)
            logger.info("Embedding completed, shape = %s", self.embedding_matrix.shape)
            # Lưu embedding và mapping ra file
            np.save(EMBEDDING_MATRIX_PATH, self.embedding_matrix)
            mapping = {"index_to_itemid": self.index_to_itemid,
                       "itemid_to_index": self.itemid_to_index}
            with open(MAPPING_FILE_PATH, 'wb') as f:
                pickle.dump(mapping, f)

        # Khởi tạo và fit KNN với metric cosine
        self.knn_model = NearestNeighbors(n_neighbors=self.n_neighbors, metric='cosine')
        self.knn_model.fit(self.embedding_matrix)

    def build_user_profile(self, user_id, userBorrowDict):
        if user_id not in userBorrowDict:
            return None
        item_ids = userBorrowDict[user_id]
        indices = [self.itemid_to_index[it] for it in item_ids if it in self.itemid_to_index]
        if not indices:
            return None
        user_vecs = self.embedding_matrix[indices, :]
        profile_vec = np.mean(user_vecs, axis=0)
        return profile_vec.reshape(1, -1)

    def recommend(self, user_id, userBorrowDict, topN=10):
        profile_vec = self.build_user_profile(user_id, userBorrowDict)
        if profile_vec is None:
            logger.info(f"User {user_id} has no borrowing history -> fallback.")
            return []
        neighbors_count = topN + len(userBorrowDict[user_id]) + 10
        distances, indices = self.knn_model.kneighbors(profile_vec, n_neighbors=neighbors_count)
        distances = distances[0]
        indices = indices[0]
        results = []
        for idx, dist in zip(indices, distances):
            rec_item = self.index_to_itemid[idx]
            if rec_item in userBorrowDict[user_id]:
                continue
            score = round(1 - dist, 4)
            results.append((user_id, rec_item, score))
        results.sort(key=lambda x: x[2], reverse=True)
        return results[:topN]

# --- Hàm load hoặc compute userBorrowDict ---
def load_user_borrow_dict(csv_path, dict_path=USER_RATING_PATH.replace('data/', 'model/').replace('.csv', '.pkl')):
    if os.path.exists(dict_path):
        with open(dict_path, 'rb') as f:
            userBorrowDict = pickle.load(f)
        logger.info("Loaded userBorrowDict from file.")
    else:
        df_borrow = pd.read_csv(csv_path)
        userBorrowDict = {}
        for row in df_borrow.itertuples():
            uid = row.user_id
            it = row.item_id
            userBorrowDict.setdefault(uid, []).append(it)
        with open(dict_path, 'wb') as f:
            pickle.dump(userBorrowDict, f)
        logger.info("Computed and saved userBorrowDict.")
    return userBorrowDict

# --- Khởi tạo dữ liệu và mô hình ---
book_data = BookData(BOOKS_PATH)
book_data.loadBooks()

# Xây dựng mô hình gợi ý
recommender = ContentBasedRecommenderSBERT(bookData=book_data, n_neighbors=30)
recommender.build_model()

userBorrowDict = load_user_borrow_dict(USER_RATING_PATH)

# --- Khởi tạo Flask server ---
app = Flask(__name__)
CORS(app)

@app.route('/get_recommendations', methods=['POST'])
def get_recommendations():
    data = request.get_json()
    user_id = data.get("user_id")
    topN = data.get("topN", 10)
    if user_id is None:
        return jsonify({"error": "Missing user_id"}), 400
    recs = recommender.recommend(user_id, userBorrowDict, topN=topN)
    response = []
    for uid, item_id, score in recs:
        response.append({
            "user_id": uid,
            "item_id": item_id,
            "title": book_data.getOriginalTitle(item_id),
            "score": float(score)
        })
    return jsonify(response)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=False)
