# Sử dụng môi trường Python 3.9 bản tinh gọn (giảm kích thước Image)
FROM python:3.9-slim

# Thiết lập thư mục làm việc mặc định bên trong container
WORKDIR /app

# Copy riêng file requirements.txt vào trước
# Mẹo DevOps: Làm việc này trước giúp Docker tận dụng bộ nhớ đệm (cache). 
# Nếu em chỉ sửa code main.py mà không thêm thư viện mới, Docker sẽ bỏ qua bước cài đặt rất mất thời gian này.
COPY requirements.txt .

# Cài đặt các thư viện cần thiết, dùng --no-cache-dir để không lưu rác sau khi cài
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn (main.py, config.py) từ máy tính vào container
# Lưu ý: file adc.json và thư mục data/ sẽ KHÔNG bị copy vào đây vì chúng nằm ở thư mục gốc ngoài rìa.
COPY . .

# Lệnh mặc định sẽ thực thi khi container khởi động
CMD ["python", "main.py"]
