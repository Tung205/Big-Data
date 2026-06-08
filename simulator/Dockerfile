# Sử dụng python slim siêu nhẹ làm base
FROM python:3.10-slim

WORKDIR /usr/share/nginx/html

# Sao chép trang HTML giao diện vào container
COPY index.html .

# Mở cổng 8080 cho giao diện
EXPOSE 8080

# Chạy máy chủ web mini tích hợp sẵn của Python để serve file index.html
CMD ["python", "-m", "http.server", "8080"]