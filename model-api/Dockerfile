# Sử dụng Python 3.10 bản slim để tối ưu dung lượng (nhẹ nhưng đủ thư viện)
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cài đặt thư viện hệ thống cần thiết cho XGBoost và FAISS
# libgomp1 là thư viện bắt buộc để XGBoost chạy được trên môi trường Linux/Docker
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements vào trước để tận dụng cache của Docker
COPY requirements.txt .

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn và thư mục models vào container
COPY . .

# Mở port 8000 cho FastAPI
EXPOSE 8000

# Lệnh khởi động server khi container chạy
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
