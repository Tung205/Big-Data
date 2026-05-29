FROM flink:1.17.1-scala_2.12

# Cài đặt Python 3 và Pip vào bên trong lõi của Flink TaskManager
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

# Cài đặt các thư viện AI (XGBoost)
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt