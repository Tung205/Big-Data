import os
import markdown
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI, HarmCategory, HarmBlockThreshold

app = FastAPI(title="Real-time Fraud Root Cause Analysis (RCA) Engine")

# --- CẤU HÌNH CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# --- 1. KHỞI TẠO RAG ---
embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
if os.path.exists("models/faiss_index"):
    vector_db = FAISS.load_local("models/faiss_index", embeddings, allow_dangerous_deserialization=True)
    print("✅ Đã kết nối cơ sở dữ liệu RAG.")
else:
    print("⚠️ Không tìm thấy faiss_index! Vui lòng tạo tài liệu RAG.")
    vector_db = None

# --- 2. CẤU HÌNH GEMINI ---
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash-lite", 
    temperature=0.1, 
    safety_settings={
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    }
)

template = """Bạn là một Chuyên gia Thẩm định Rủi ro. 
Hệ thống giám sát vừa kích hoạt cảnh báo GIAN LẬN. Hãy đối chiếu thông số với Tài liệu Kịch bản (RAG) và kết luận.

=== THÔNG SỐ GIAO DỊCH ===
{transaction_context}

=== TÀI LIỆU KỊCH BẢN (RAG) ===
{rag_context}

Phản hồi bằng Markdown theo cấu trúc (tiếng Việt):
#### 1. KỊCH BẢN PHÙ HỢP NHẤT
[Tên kịch bản]
#### 2. BẰNG CHỨNG SỐ LIỆU
[Phân tích]
#### 3. NGUYÊN NHÂN GỐC RỄ
[Phân tích động cơ]

Chốt bằng câu: KẾT LUẬN ĐIỀU TRA: XÁC THỰC GIAN LẬN
"""
prompt = PromptTemplate(input_variables=["transaction_context", "rag_context"], template=template)

# --- 3. ENDPOINT CHO GRAFANA (SỬ DỤNG GET & HTML RESPONSE) ---
@app.get("/", response_class=HTMLResponse)
async def explain_fraud_ui(
    cc_num: str = Query("Không rõ"),
    amt: float = Query(0.0),
    hour: int = Query(0),
    age: int = Query(0),
    distance_km: float = Query(0.0),
    velocity_1d_count: float = Query(0.0),
    velocity_1d_amt_sum: float = Query(0.0),
    category_encoded: str = Query("Không rõ")
):
    # Nếu chưa có thẻ nào được click, hiển thị màn hình chờ
    if cc_num == "Không rõ" or cc_num == "":
        return build_html_response("Hãy click vào một giao dịch bên bảng log để bắt đầu phân tích RCA.", status="CHỜ DỮ LIỆU", color="#4a90e2")

    # Xử lý logic ngữ nghĩa
    amt_desc = "cao" if amt > 500 else ("rất cao" if amt > 1000 else "bình thường")
    hour_desc = "đêm muộn / rạng sáng" if 22 <= hour or hour <= 4 else "ban ngày"
    dist_desc = "rất xa" if distance_km > 100 else ("xa" if distance_km > 50 else "bình thường")
    
    txn_context = (
        f"- Số thẻ: {cc_num}\n"
        f"- Số tiền: {amt} USD ({amt_desc})\n"
        f"- Khung giờ: {hour}h ({hour_desc})\n"
        f"- Tuổi: {age} tuổi\n"
        f"- Khoảng cách: {distance_km:.2f} km ({dist_desc})\n"
        f"- Tần suất (24h): {velocity_1d_count} lần\n"
        f"- Tổng tiền (24h): {velocity_1d_amt_sum} USD\n"
        f"- Mã Danh mục: {category_encoded}"
    )

    try:
        if vector_db:
            docs = vector_db.similarity_search(txn_context, k=2)
            rag_context = "\n\n".join([doc.page_content for doc in docs])
        else:
            rag_context = "Hệ thống chưa có dữ liệu đối chiếu RAG."

        final_prompt = prompt.format(transaction_context=txn_context, rag_context=rag_context)
        llm_output = llm.invoke(final_prompt)
        
        # Chuyển Markdown sang HTML
        html_content = markdown.markdown(llm_output.content.strip())
        return build_html_response(html_content, status="FRAUD_CONFIRMED", color="#ff5a60")
        
    except Exception as e:
        error_msg = f"<p>Lỗi kết nối tới mô hình AI: {str(e)}</p>"
        return build_html_response(error_msg, status="HỆ THỐNG LỖI", color="#ffae42")

def build_html_response(content: str, status: str, color: str):
    """Hàm bọc giao diện CSS siêu chuẩn để nhúng vào Iframe Grafana"""
    return f"""
    <html>
        <head>
            <style>
                body {{
                    background-color: transparent; 
                    color: #c8d1e0; 
                    font-family: 'Inter', sans-serif; 
                    padding: 10px;
                    margin: 0;
                }}
                h4 {{ color: #ffffff; border-bottom: 1px solid #2d3139; padding-bottom: 8px; margin-top: 20px; font-size: 14px;}}
                p, ul, li {{ font-size: 13px; line-height: 1.6; }}
                .status-box {{
                    background-color: rgba(255, 90, 96, 0.1); 
                    color: {color}; 
                    padding: 8px 12px; 
                    border-radius: 4px; 
                    font-weight: bold; 
                    border: 1px solid {color}; 
                    display: inline-block;
                    font-size: 12px;
                    margin-bottom: 10px;
                }}
                strong {{ color: #e0e0e0; }}
            </style>
        </head>
        <body>
            <div class="status-box">🚨 STATUS: {status}</div>
            <div class="content">
                {content}
            </div>
        </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)