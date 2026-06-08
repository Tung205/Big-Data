import os
os.environ["TRANSFORMERS_NO_TF"] = "1"  # Ép thư viện transformers bỏ qua TensorFlow/Keras
from langchain_core.documents import Document
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings

def build_faiss_index():
    print("Đang khởi tạo danh sách các case study gian lận và nhận diện nhầm...")
    
    # Tập hợp các quy tắc tinh chỉnh dựa trên phân phối dữ liệu thực tế
    knowledge_base = [
        # --- 5 CASE GỐC ---
        Document(
            page_content="Case FRAUD-01 (Hacker càn quét tài khoản - Velocity Attack): GIAN LẬN. Đặc điểm: Thời gian cách giao dịch trước cực ngắn (time_since_last_trans_sec < 60 giây), tần suất quẹt thẻ trong ngày dày đặc (velocity_1d_count lớn). Nguyên nhân: Kẻ gian sử dụng bot tự động (automated scripts) để thực hiện liên tiếp các lệnh thanh toán nhằm rút cạn hạn mức thẻ trước khi chủ thẻ kịp phát hiện và khóa thẻ.",
            metadata={"type": "RCA", "pattern": "Velocity Bot Attack"}
        ),
        Document(
            page_content="Case FRAUD-02 (Tấn công tài khoản người cao tuổi - Account Takeover): GIAN LẬN. Đặc điểm: Chủ thẻ lớn tuổi (age > 50 hoặc age > 60), phát sinh giao dịch trực tuyến tại các danh mục rủi ro cao (category_encoded 11 hoặc 12 như mua sắm trực tuyến, công nghệ) vào khung giờ đêm muộn (hour từ 22h - 4h sáng). Nguyên nhân: Thông tin thẻ của người lớn tuổi (vốn ít am hiểu công nghệ) bị lộ qua các trang web giả mạo (Phishing) hoặc mã độc. Kẻ gian dùng thông tin này để mua sắm vật phẩm thanh khoản cao trực tuyến.",
            metadata={"type": "RCA", "pattern": "Elderly Account Takeover"}
        ),
        Document(
            page_content="Case FRAUD-03 (Giao dịch giá trị lớn nửa đêm - High-Value Midnight Fraud): GIAN LẬN. Đặc điểm: Số tiền giao dịch đơn lẻ rất lớn (amt > 500 hoặc amt > 1000 USD), xảy ra vào lúc đêm muộn (hour từ 22h đến 3h sáng), tập trung ở danh mục dịch vụ trực tuyến xa xỉ. Nguyên nhân: Kẻ gian lợi dụng lúc chủ thẻ đang ngủ sâu (không thể kiểm tra điện thoại hay nhập mã OTP) để thực hiện các giao dịch lớn mua vé máy bay, đồ xa xỉ nhằm tẩu tán tài sản nhanh chóng.",
            metadata={"type": "RCA", "pattern": "High-Value Midnight Siphoning"}
        ),
        Document(
            page_content="Case FRAUD-04 (Thử thẻ / Kích hoạt thẻ đánh cắp - Card Testing): GIAN LẬN. Đặc điểm: Số tiền giao dịch đơn lẻ nhỏ hoặc cực nhỏ (amt < 50 USD), thường xảy ra ở các danh mục mua sắm tự động, dịch vụ trực tuyến nhưng đi kèm tần suất giao dịch trong ngày (velocity_1d_count) hoặc tổng chi tiêu ngày tăng cao bất thường. Nguyên nhân: Kẻ gian vừa hack hoặc mua được thông tin thẻ, tiến hành giao dịch nhỏ thử nghiệm xem thẻ còn hoạt động (Card Testing) hay không trước khi thực hiện chuỗi giao dịch lớn.",
            metadata={"type": "RCA", "pattern": "Card Testing Fraud"}
        ),
        Document(
            page_content="Case FRAUD-05 (Bất thường không gian địa lý - Geo-Location Anomaly): GIAN LẬN. Đặc điểm: Khoảng cách từ vị trí giao dịch đến nơi cư trú của chủ thẻ rất lớn (distance_km > 100 km), thời gian di chuyển không hợp lý so với vận tốc vật lý thông thường. Nguyên nhân: Thẻ vật lý của nạn nhân đã bị đánh cắp hoặc bị sao chép thông tin băng từ (Skimming) tại cây ATM và đang bị kẻ gian sử dụng ở một tỉnh/thành phố hoàn toàn khác.",
            metadata={"type": "RCA", "pattern": "Geo-Location Anomaly"}
        ),
        
        # --- 7 CASE NÂNG CAO MỚI ---
        Document(
            page_content="Case FRAUD-06 (Tài khoản rác / Rửa tiền tần suất cao - Mule Account / Money Laundering): GIAN LẬN. Đặc điểm: Khách hàng trẻ tuổi (age < 30), tần suất giao dịch trong ngày cực lớn (velocity_1d_count > 8 lần) và tổng số tiền tích lũy ngày vọt lên rất cao (velocity_1d_amt_sum hàng ngàn USD), mặc dù số tiền mỗi giao dịch (amt) có thể chỉ ở mức trung bình. Nguyên nhân: Tài khoản này có dấu hiệu là tài khoản cho thuê/mượn (Mule Account) phục vụ cho mục đích gom tiền bất hợp pháp hoặc rửa tiền thông qua các ví điện tử, cổng thanh toán trực tuyến.",
            metadata={"type": "RCA", "pattern": "Mule Account Activity"}
        ),
        Document(
            page_content="Case FRAUD-07 (Chi tiêu đột biến sau chu kỳ đóng băng - Splurge After Inactivity): GIAN LẬN. Đặc điểm: Thời gian cách giao dịch trước cực kỳ dài (time_since_last_trans_sec lớn, thể hiện thẻ đã lâu không dùng), nhưng bất ngờ phát sinh chuỗi giao dịch liên tiếp với số tiền lớn (amt > 500 USD), tần suất dày (velocity_1d_count tăng nhanh) ở nhóm danh mục rủi ro (category_encoded 11, 4). Nguyên nhân: Thẻ này đã bị hacker thu thập thông tin từ lâu nhưng 'ngủ đông', chờ thời điểm thích hợp mới kích hoạt và thực hiện càn quét quy mô lớn.",
            metadata={"type": "RCA", "pattern": "Splurge After Inactivity"}
        ),
        Document(
            page_content="Case FRAUD-08 (Gian lận vùng nông thôn / Đô thị nhỏ - Small Town Anomaly): GIAN LẬN. Đặc điểm: Quy mô dân số nơi cư trú cực kỳ thấp (city_pop nhỏ, vùng nông thôn), nhưng lại phát sinh giao dịch đơn lẻ có giá trị khổng lồ (amt > 1000 USD) hoặc tổng chi tiêu ngày (velocity_1d_amt_sum) vượt quá xa mức thu nhập trung bình của khu vực, thường diễn ra vào ban đêm. Nguyên nhân: Thông tin thẻ của người dân ở vùng quê bị kẻ gian chiếm đoạt (ví dụ qua các chiêu trò lừa đảo trúng thưởng) và sử dụng để quẹt mua hàng xa xỉ trên mạng.",
            metadata={"type": "RCA", "pattern": "Small Town Income Anomaly"}
        ),
        Document(
            page_content="Case FRAUD-09 (Gian lận danh mục ẩn danh / Thanh khoản ngay - High-Liquidity Anonymous Fraud): GIAN LẬN. Đặc điểm: Giao dịch phát sinh tại các danh mục rủi ro đặc biệt (category_encoded 4 hoặc 11 - đại diện cho mua thẻ quà tặng, nạp tiền tiền ảo, hoặc đồ điện tử giá trị cao) với số tiền sát ngưỡng hạn mức (amt sát hoặc trên 1000 USD). Nguyên nhân: Kẻ gian tập trung mua các mặt hàng không cần địa chỉ giao hàng vật lý, dễ dàng chuyển hóa thành tiền mặt hoặc không thể truy vết (vô hình hóa dòng tiền) ngay sau khi chiếm đoạt thẻ.",
            metadata={"type": "RCA", "pattern": "High-Liquidity Asset Siphoning"}
        ),
        Document(
            page_content="Case FRAUD-10 (Tấn công thay đổi vị trí đột ngột - Fast Traveling Fraud): GIAN LẬN. Đặc điểm: Khoảng cách địa lý của giao dịch cực xa (distance_km > 120 km) kết hợp thời gian cách giao dịch trước ngắn (time_since_last_trans_sec thấp). Về mặt vật lý, chủ thẻ không thể di chuyển một quãng đường xa như vậy trong một khoảng thời gian ngắn như thế. Nguyên nhân: Đây là bằng chứng rõ ràng của việc lộ dữ liệu thẻ. Kẻ gian ở một tỉnh/thành phố khác đã nhân bản thông tin thẻ thành công và tiến hành giao dịch song song với chủ thẻ thật.",
            metadata={"type": "RCA", "pattern": "Impossible Travel Anomaly"}
        ),
        Document(
            page_content="Case FRAUD-11 (Gian lận kỳ nghỉ / Giả mạo dịch vụ du lịch - Holiday/Travel Phishing Splash): GIAN LẬN. Đặc điểm: Giao dịch thuộc danh mục di chuyển/du lịch (category_encoded thường là travel hoặc nhóm dịch vụ lưu trú), số tiền rất cao (amt > 800 USD), xuất hiện bất thường không nằm trong thói quen chi tiêu lịch sử. Nguyên nhân: Nạn nhân sập bẫy các trang web combo du lịch giá rẻ giả mạo (Phishing Travel Site) và bị kẻ gian ép quẹt thẻ thanh toán trọn gói vào các tài khoản ma.",
            metadata={"type": "RCA", "pattern": "Travel Category Phishing"}
        ),
        Document(
            page_content="Case FRAUD-12 (Tấn công vét cạn tài khoản rạng sáng - Dawn Exhaustion Attack): GIAN LẬN. Đặc điểm: Giao dịch xảy ra vào khung giờ nhạy cảm nhất từ 4h - 6h sáng (hour rạng sáng), số tiền giao dịch tăng tiến (giao dịch sau lớn hơn giao dịch trước) đi kèm khoảng cách địa lý xa. Nguyên nhân: Kẻ tấn công tính toán thời điểm chủ thẻ tắt hoàn toàn thiết bị hoặc trong trạng thái ngủ say nhất để thực hiện các lệnh rút tiền hoặc mua sắm liên tục, mục tiêu là vét cạn hạn mức trước khi bình minh lên.",
            metadata={"type": "RCA", "pattern": "Dawn Exhaustion Attack"}
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
