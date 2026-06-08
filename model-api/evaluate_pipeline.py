import pandas as pd
import requests
import time

def evaluate_hybrid_pipeline():
    print("1. Đang đọc dữ liệu kiểm thử...")
    # Đọc 2 file CSV em đã tạo từ bước trước
    try:
        # Nếu muốn test nhanh toàn bộ, em xóa .head(10) đi nhé.
        # Ở đây thầy để lại .head(10) theo ý em để test 20 giao dịch trước.
        df_tp = pd.read_csv("sample_50_real_frauds.csv").sample(n=5)
    except FileNotFoundError:
        print("Lỗi: Không tìm thấy file CSV. Hãy đảm bảo file nằm cùng thư mục.")
        return

    # Gộp lại thành tập test
    df_test = df_tp
    total_rows = len(df_test) # Lấy tổng số dòng động để in ra chính xác
    print(f"Tổng số giao dịch cần kiểm thử: {total_rows}")

    # URL của API FastAPI em đang chạy local
    API_URL = "http://localhost:8000/predict"
    
    results = []
    
    print("\n2. Bắt đầu gửi dữ liệu qua Hybrid RAG Pipeline...\n")
    # Lặp qua từng dòng dữ liệu
    for index, row in df_test.iterrows():
        # Ánh xạ dữ liệu vào format chuẩn của class Transaction (Pydantic)
        payload = {
            "amt": float(row['amt']),
            "city_pop": int(row['city_pop']),
            "hour": int(row['hour']),
            "day": int(row['day']),
            "month": int(row['month']),
            "day_of_week": int(row['day_of_week']),
            "age": int(row['age']),
            "distance_km": float(row['distance_km']),
            "time_since_last_trans_sec": float(row['time_since_last_trans_sec']),
            "velocity_1d_count": float(row['velocity_1d_count']),
            "velocity_1d_amt_sum": float(row['velocity_1d_amt_sum']),
            "gender_encoded": int(row['gender_encoded']),
            "category_encoded": int(row['category_encoded'])
        }

        # 0 là An toàn (SAFE), 1 là Gian lận thật (FRAUD)
        ground_truth = int(row['is_fraud']) 
        actual_label = "FRAUD" if ground_truth == 1 else "SAFE"

        # Đo thời gian xử lý của RAG + LLM
        start_time = time.time()
        
        try:
            # Gửi HTTP POST request tới app.py
            response = requests.post(API_URL, json=payload)
            response.raise_for_status() # Báo lỗi nếu API sập
            
            res_data = response.json()
            end_time = time.time()
            latency = round(end_time - start_time, 2)
            llm_reason = res_data.get("reason", "")
            
            # Lưu lại kết quả để xíu nữa phân tích
            results.append({
                "transaction_id": index + 1,
                "ground_truth": actual_label,
                "xgboost_prediction": res_data.get("xgboost_initial_prediction", "SAFE"),
                "llm_final_status": res_data["final_status"],
                "llm_reasoning": llm_reason,
                "latency_sec": latency
            })
            
            # In log ra màn hình rõ ràng hơn
            print(f"[{index+1}/{total_rows}] Thực tế: {actual_label} | LLM chốt: {res_data['final_status']} | Độ trễ: {latency}s")
            # Cắt lấy 100 ký tự đầu của lý do để xem lướt cho nhanh
            snippet = llm_reason
            print(f"   -> Lý do: {snippet}\n")
            time.sleep(5)
            
        except requests.exceptions.RequestException as e:
            print(f"Lỗi kết nối tại giao dịch {index+1}: {e}\n")
        
        
            
    # ==========================================
    # 3. Tổng hợp báo cáo đánh giá
    # ==========================================
    print("3. Đang xuất báo cáo đánh giá...")
    df_results = pd.DataFrame(results)
    
    # Lọc ra nhóm False Positive ban đầu (thực tế là SAFE, nhưng XGBoost đoán là FRAUD)
    fp_group = df_results[df_results['ground_truth'] == "SAFE"]
    saved_by_llm = fp_group[fp_group['llm_final_status'] == "SAFE"]
    
    print("--------------------------------------------------")
    print("KẾT QUẢ HIỆU CHỈNH CỦA RAG + LLM:")
    print(f"Tổng số ca False Positive (XGBoost báo oan): {len(fp_group)}")
    print(f"Số ca LLM lật ngược thành công (cứu được khách hàng): {len(saved_by_llm)}")
    if len(fp_group) > 0:
        print(f"Tỷ lệ sửa lỗi (Correction Rate): {(len(saved_by_llm) / len(fp_group)) * 100:.1f}%")
    print("--------------------------------------------------")

    df_results.to_csv("hybrid_pipeline_evaluation_report.csv", index=False)
    print("Đã lưu chi tiết toàn bộ giải thích của LLM vào file: hybrid_pipeline_evaluation_report.csv")

if __name__ == "__main__":
    evaluate_hybrid_pipeline()