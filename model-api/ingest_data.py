import os
from langchain_core.documents import Document
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings

def build_faiss_index():
    print("Đang khởi tạo danh sách các case study gian lận và nhận diện nhầm...")
    
    # Tập hợp các quy tắc tinh chỉnh dựa trên phân phối dữ liệu thực tế
    knowledge_base = [
        # ==========================================
        # GIAO DỊCH FRAUD (GIAN LẬN THẬT - TRUE POSITIVES)
        # ==========================================
        Document(
            page_content="Case FRAUD-01 (Hacker càn quét thẻ ban đêm): GIAN LẬN. Đặc điểm: Diễn ra vào ban đêm (22h-3h) KẾT HỢP với thời gian giữa các giao dịch cực kỳ ngắn (time_since_last_trans < 60 giây) và tần suất cao. Nguyên nhân: Khác với Case SAFE-01 (mua sắm bình thường), đây là dấu hiệu bot tự động bắn request liên tục để rút cạn tiền trong thẻ khi chủ thẻ đang ngủ.",
            metadata={"type": "true_fraud", "status": "FRAUD"}
        ),
        Document(
            page_content="Case FRAUD-02 (Bất thường không gian và nhân khẩu học): GIAN LẬN. Đặc điểm: Người lớn tuổi (age > 50) nhưng lại thực hiện giao dịch online danh mục rủi ro (11, 4) vào lúc nửa đêm, KẾT HỢP với khoảng cách địa lý đột biến. Nguyên nhân: Thẻ của người lớn tuổi bị lộ thông tin và bị kẻ gian ở nơi khác sử dụng.",
            metadata={"type": "true_fraud", "status": "FRAUD"}
        ),
        Document(
            page_content="Case FRAUD-03 (Rút tiền/Mua đồ thanh khoản cao liên tục): GIAN LẬN. Đặc điểm: Số tiền lớn lặp đi lặp lại nhiều lần trong ngày (velocity_1d_count lớn, velocity_1d_amt_sum vọt lên mức hàng ngàn đô), thuộc nhóm danh mục 4 hoặc 11. Thời gian giữa các giao dịch ngắn bất thường.",
            metadata={"type": "true_fraud", "status": "FRAUD"}
        )
    ]

    print("Đang tải mô hình nhúng (Embedding Model) - sentence-transformers/all-MiniLM-L6-v2...")
    print("Quá trình này có thể mất vài chục giây trong lần chạy đầu tiên để tải model về máy.")
    
    # Sử dụng mô hình embedding chuẩn như đã cấu hình trong app.py
    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

    print("Đang mã hóa văn bản thành Vector và đưa vào FAISS Index...")
    vector_db = FAISS.from_documents(knowledge_base, embeddings)

    # Lưu xuống ổ cứng
    save_path = "models/faiss_index"
    os.makedirs(save_path, exist_ok=True)
    vector_db.save_local(save_path)
    
    print("\n==========================================")
    print(f"THÀNH CÔNG! Đã lưu bộ não RAG cho em tại thư mục: {os.path.abspath(save_path)}")
    print("==========================================")

if __name__ == "__main__":
    build_faiss_index()
