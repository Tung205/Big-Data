import os
import xgboost as xgb
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings, HuggingFacePipeline
from langchain_core.prompts import PromptTemplate
from transformers import pipeline
from langchain_google_genai import ChatGoogleGenerativeAI, HarmCategory, HarmBlockThreshold
app = FastAPI(title="Hybrid Fraud Detection: XGBoost + RAG + LLM")

# --- 1. Load XGBoost Model ---
XGB_MODEL_PATH = "models/xgb_fraud_model.json"
xgb_model = xgb.XGBClassifier()
xgb_model.load_model(XGB_MODEL_PATH)

# --- 2. Cấu hình RAG ---
embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

if os.path.exists("models/faiss_index"):
    vector_db = FAISS.load_local(
        "models/faiss_index", 
        embeddings, 
        allow_dangerous_deserialization=True
    )
    print("Đã tải FAISS Index thành công từ ổ cứng.")
else:
    vector_db = None

# --- 3. Cấu hình LLM ---
# Sử dụng Qwen 1.5B Instruct - Rất nhẹ, thông minh, chạy local CPU mượt
# --- 3. Cấu hình LLM ---
# Khai báo API Key của em ở đây (Nhớ điền key em vừa copy vào)
os.environ["GOOGLE_API_KEY"] = "thich_thi_tu_dien_vao_nhe"

# Khởi tạo mô hình Gemini
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite", # Dùng bản Flash để lấy tốc độ siêu tốc
    temperature=0.1,          # Để 0.1 giúp mô hình tư duy logic, không sáng tạo linh tinh
    safety_settings={
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    }
)


# --- SỬA LẠI PROMPT TEMPLATE TRONG app.py ---
# --- SỬA LẠI PROMPT TEMPLATE TRONG app.py ---
template = """Bạn là chuyên gia phân tích gian lận. Hệ thống đã đánh dấu giao dịch này là GIAN LẬN. Hãy đối chiếu với luật RAG và suy luận để đưa ra quyết định cuối cùng.

=== VÍ DỤ MẪU BẮT BUỘC TUÂN THỦ ===
THÔNG SỐ GIAO DỊCH: Số tiền (amt): 1200.0 (rất cao), Giờ giao dịch (hour): 10h (ban ngày).
TÀI LIỆU (RAG): Case SAFE-01: Chi tiêu lớn đột xuất vào ban ngày là an toàn.
PHÂN TÍCH: Giao dịch có số tiền rất cao nhưng diễn ra vào ban ngày, khớp với Case SAFE-01. Không có dấu hiệu của gian lận ban đêm.
KẾT LUẬN CHÍNH THỨC: SAFE
====================================

=== GIAO DỊCH CẦN XỬ LÝ ===
THÔNG SỐ GIAO DỊCH:
{transaction_context}

TÀI LIỆU ĐỐI CHIẾU (RAG):
{rag_context}

Nhiệm vụ của bạn: Hãy phân tích giao dịch trên dựa theo tài liệu đối chiếu. 
YÊU CẦU ĐỊNH DẠNG BẮT BUỘC:
1. Bắt đầu bằng chữ "PHÂN TÍCH: " và viết lời giải thích của bạn.
2. Bắt buộc phải xuống dòng và kết thúc bằng câu "KẾT LUẬN CHÍNH THỨC: SAFE" hoặc "KẾT LUẬN CHÍNH THỨC: FRAUD"

"""

prompt = PromptTemplate(
    input_variables=["transaction_context", "rag_context"], 
    template=template
)

# --- 4. Định nghĩa Input khớp 100% với Kaggle Features ---
class Transaction(BaseModel):
    amt: float
    city_pop: int
    hour: int
    day: int
    month: int
    day_of_week: int
    age: int
    distance_km: float
    time_since_last_trans_sec: float
    velocity_1d_count: float
    velocity_1d_amt_sum: float
    gender_encoded: int
    category_encoded: int

# --- 5. Logic Xử lý (API) ---
@app.post("/predict")
async def detect_fraud(txn: Transaction):
    # Bước 1: Đưa dữ liệu vào DataFrame để XGBoost dự đoán
    txn_dict = txn.model_dump()
    df_input = pd.DataFrame([txn_dict])    
    # Model trả về 0 (SAFE) hoặc 1 (FRAUD)
    prediction = int(xgb_model.predict(df_input)[0])
    
    # Bước 2: Theo biểu đồ, nếu XGBoost đánh giá là an toàn (0) -> Xuất SAFE luôn
    if prediction == 0:
        return {
            "final_status": "SAFE",
            "reason": "Mô hình XGBoost đánh giá giao dịch hợp lệ, không có dấu hiệu bất thường."
        }
    
    # Bước 3: Nếu XGBoost dự đoán là 1 (FRAUD) -> Kích hoạt luồng RAG + LLM để kiểm tra lại
    # Serialize to natural language (Từ biểu đồ)
    amt_response = "bình thường" 
    time_since_last_trans_sec_response = "bình thường"
    velocity_1d_count_response = "bình thường"
    velocity_1d_amt_sum_response = "bình thường"
    distance_km_response = "bình thường"
    hour_response = "bình thường"
    age_response = "bình thường"
    category_encoded_response = "bình thường"
    if txn.amt > 1000:
        amt_response = "rất cao"
    elif txn.amt > 500:
        amt_response = "cao"
    elif txn.amt < 50:
        amt_response = "rất thấp"

    if txn.time_since_last_trans_sec < 60:
        time_since_last_trans_sec_response = "cực kỳ ngắn"
    elif txn.time_since_last_trans_sec < 1000:
        time_since_last_trans_sec_response = "ngắn"
    elif txn.time_since_last_trans_sec > 12000:
        time_since_last_trans_sec_response = "rất dài"

    if txn.velocity_1d_count >= 6:
        velocity_1d_count_response = "cao" 
    elif txn.velocity_1d_count > 0:
        velocity_1d_count_response = "thấp"
    
    if txn.velocity_1d_amt_sum > 2000:
        velocity_1d_amt_sum_response = "cao"
    elif txn.velocity_1d_amt_sum > 0:
        velocity_1d_amt_sum_response = "bình thường"
    
    if txn.distance_km > 100:
        distance_km_response = "cao"
    elif txn.distance_km < 80:
        distance_km_response = "bình thường"
    
    if 22 <= txn.hour or txn.hour <= 3:
        hour_response = "ban đêm"
    elif 6 <= txn.hour <= 18:
        hour_response = "ban ngày"
    
    if txn.age > 60:
        age_response = "cao"
    elif txn.age < 35:
        age_response = "trẻ"
    
    if txn.category_encoded in [8, 10]:
        category_encoded_response = "nhóm danh mục an toàn"
    elif txn.category_encoded in [11, 4]:
        category_encoded_response = "nhóm danh mục rủi ro"
    
    txn_context = (
        f"Số tiền (amt): {txn.amt} ({amt_response}), Khoảng cách (distance_km): {txn.distance_km:.2f} km ({distance_km_response}), "
        f"Tuổi (age): {txn.age} ({age_response}), Tần suất 1 ngày (velocity_1d_count): {txn.velocity_1d_count} lần, "
        f"Tổng tiền 1 ngày (velocity_1d_amt_sum): {txn.velocity_1d_amt_sum} ({velocity_1d_amt_sum_response}), Giờ giao dịch (hour): {txn.hour}h ({hour_response}), "
        f"Thời gian cách giao dịch trước (time_since_last_trans_sec): {txn.time_since_last_trans_sec} giây ({time_since_last_trans_sec_response})."
        f"Danh mục (category_encoded): {txn.category_encoded} ({category_encoded_response})"
    )
    
    # Truy xuất RAG
    if vector_db:
        docs = vector_db.similarity_search(txn_context, k=2)
        rag_context = "\n".join([doc.page_content for doc in docs])
    else:
        rag_context = "Không có dữ liệu lịch sử đối chiếu."

    # Chạy LLM
    import re # Đừng quên import thư viện regex ở đầu file app.py nếu chưa có

# ... (các phần code phía trên giữ nguyên) ...

    # Chạy LLM
    final_prompt = prompt.format(
        transaction_context=txn_context,
        rag_context=rag_context
    )
    
    # llm_response = llm.invoke(final_prompt).strip()
    # Thay bằng 2 dòng mới này:
    llm_output = llm.invoke(final_prompt)
    llm_response = llm_output.content.strip() # Lấy phần nội dung chữ rồi mới strip
    
    print(f"\n--- [GIAO DỊCH] LLM SUY LUẬN ---")
    print(llm_response)
    print("---------------------------------\n")

    # === THÊM 2 DÒNG NÀY ĐỂ BẮT BỆNH ===
    print(f"\n--- [GIAO DỊCH] METADATA TỪ GOOGLE ---")
    print(llm_output.response_metadata)
    # ===================================
    
    # Mặc định là FRAUD vì XGBoost đã cảnh báo
    final_status = "FRAUD" 
    
    # Chuyển toàn bộ thành chữ hoa để dễ quét
    upper_response = llm_response.upper()
    
    # Quét phần đuôi (100 ký tự cuối) vì LLM thường chốt kết luận ở cuối
    tail_response = upper_response[-100:]
    
    # Sử dụng Regex để tìm các biến thể của chữ KẾT LUẬN CHÍNH THỨC: SAFE
    # re.search sẽ tìm cụm từ "SAFE" nằm ngay sau chữ "KẾT LUẬN" hoặc "CHÍNH THỨC"
    if re.search(r'(KẾT LUẬN|CHÍNH THỨC|LUẬN CHÍNH THỨC).*?:?\s*SAFE', tail_response):
        final_status = "SAFE"
    # Nếu nó chỉ in cộc lốc chữ SAFE ở cuối dòng
    elif tail_response.strip().endswith("SAFE") or tail_response.strip().endswith("SAFE."):
         final_status = "SAFE"
    
    return {
        "final_status": final_status,
        "reason": llm_response,
        "xgboost_initial_prediction": "FRAUD"
    }
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)