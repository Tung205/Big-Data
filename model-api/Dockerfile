# Sử dụng Python 3.10 bản slim để tối ưu dung lượng
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cài đặt thư viện hệ thống cần thiết cho XGBoost và FAISS
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements vào trước để tận dụng layer cache
COPY requirements.txt .

# Ép cài đặt Torch CPU bản 2.3.1 cố định
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install torch==2.3.1 --index-url https://download.pytorch.org/whl/cpu

# Cài đặt các thư viện còn lại dựa trên extra-index để bảo đảm liên kết chuẩn
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt --extra-index-url https://download.pytorch.org/whl/cpu

# Copy toàn bộ mã nguồn và thư mục models vào container
COPY . .

# Mở port 8000 cho FastAPI
EXPOSE 8000

# Lệnh khởi động server khi container chạy
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]