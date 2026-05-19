import google.generativeai as genai

# Điền key của em vào đây
genai.configure(api_key="thich_thi_tu_dien_vao_nhe")

print("Danh sách các model em có thể dùng:")
for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print(m.name)