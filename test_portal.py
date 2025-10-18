import streamlit as st
import random
from datetime import datetime

# --- CONFIGURATION ---
NUM_CONCEPTUAL = 3  # Number of conceptual questions to select
NUM_APPLIED_T1 = 3  # Number of Tier 1 (Basic) questions
NUM_APPLIED_T2 = 6  # Number of Tier 2 (Join) questions
NUM_APPLIED_T3 = 3  # Number of Tier 3 (Advanced) questions
# Total questions = 3 + 3 + 6 + 3 = 15 questions

# --- QUESTION POOL (VIETNAMESE) ---
CONCEPTUAL_QUESTIONS = [
    """Vai trò chính của HDFS NameNode là gì?
    a) Lưu trữ các khối dữ liệu thực tế trên đĩa cục bộ của nó.
    b) Quản lý siêu dữ liệu (metadata) và không gian tên (namespace) của hệ thống tệp (ví dụ như cấu trúc tệp/thư mục).
    c) Thực thi các công việc Spark và tác vụ do người dùng gửi trên cụm.
    d) Chia các tệp lớn thành các khối nhỏ hơn trước khi chúng được ghi.""",
    
    """Lệnh nào được sử dụng để sao chép một tệp có tên `sales.csv` vào thư mục `/data` trong HDFS?
    a) hdfs dfs -mv sales.csv /data
    b) hdfs dfs -get sales.csv /data
    c) hdfs dfs -put sales.csv /data
    d) hdfs dfs -cat sales.csv > /data/sales.csv""",

    """Vai trò chính của HDFS DataNode là gì?
    a) Quản lý cây hệ thống tệp và tất cả siêu dữ liệu (metadata).
    b) Lưu trữ và truy xuất các khối dữ liệu theo chỉ dẫn của NameNode và báo cáo trạng thái của nó.
    c) Chấp nhận các yêu cầu công việc trực tiếp từ client và lập lịch cho chúng.
    d) Cân bằng tải dữ liệu bằng cách di chuyển các khối giữa các DataNode khác.""",

    """Bạn sẽ sử dụng lệnh nào để sao chép thư mục `/data/results` từ HDFS về thư mục hiện tại trên máy cục bộ của bạn?
    a) hdfs dfs -put /data/results .
    b) hdfs dfs -get /data/results .
    c) hdfs dfs -download /data/results .
    d) hdfs dfs -read /data/results > local_results""",

    """Khi bạn thực thi `spark.read.parquet("hdfs://...")`, điều gì xảy ra đầu tiên?
    a) Spark ngay lập tức tải toàn bộ tệp Parquet vào bộ nhớ của driver.
    b) Spark liên hệ với HDFS NameNode để lấy siêu dữ liệu và vị trí của các khối dữ liệu.
    c) Spark chỉ thị cho các DataNode gửi tất cả dữ liệu đến một worker node duy nhất.
    d) Spark tạo một bản sao tạm thời của tệp Parquet ở định dạng hiệu quả hơn.""",

    """Vai trò độc quyền của Spark Master trong một cụm độc lập (standalone cluster) là gì?
    a) Nó phân tích mã của người dùng để tạo ra một Đồ thị không tuần hoàn có hướng (DAG) của các tác vụ.
    b) Nó giữ một phân vùng của RDD trong bộ nhớ của nó.
    c) Nó hoạt động như một trình quản lý tài nguyên, nhận yêu cầu từ các chương trình driver và phân bổ tài nguyên của worker (lõi CPU, bộ nhớ) cho chúng.
    d) Nó chịu trách nhiệm ghi kết quả cuối cùng của một công việc vào một hệ thống lưu trữ bền vững như HDFS.""",

    """Câu nào sau đây mô tả đúng nhất về một Spark Worker Node?
    a) Một máy trung tâm kiểm soát và điều phối toàn bộ cụm.
    b) Một tiến trình nhẹ chỉ giữ siêu dữ liệu về vị trí của dữ liệu.
    c) Một máy chạy một hoặc nhiều tiến trình 'executor', chịu trách nhiệm thực sự chạy các tác vụ tính toán.
    d) Máy mà từ đó người dùng gửi ứng dụng Spark của họ.""",

    """Khi một ứng dụng Spark được khởi chạy, các thành phần thường giao tiếp với nhau như thế nào?
    a) Tất cả các worker node giao tiếp trực tiếp với nhau để điều phối các tác vụ.
    b) Chương trình driver giao tiếp trực tiếp với mỗi worker node để giao nhiệm vụ.
    c) Master node gửi mã ứng dụng đến chương trình driver để thực thi.
    d) Chương trình driver kết nối với master, master phân bổ tài nguyên trên các worker, và sau đó driver gửi các tác vụ đến các executor trên các worker đó.""",

    """Nếu một Spark Worker node bị lỗi giữa chừng một phép tính, hành vi dự kiến trong một thiết lập có khả năng chịu lỗi (fault-tolerant) là gì?
    a) Toàn bộ ứng dụng Spark bị lỗi và phải được khởi động lại thủ công.
    b) Spark Master node sẽ tắt toàn bộ cụm để ngăn ngừa hỏng dữ liệu.
    c) Spark Master sẽ phát hiện lỗi và chỉ thị một worker khác khởi chạy lại các tác vụ đã bị mất.
    d) Chương trình driver sẽ cố gắng hoàn thành các tác vụ bị lỗi trên máy cục bộ của chính nó."""
]

APPLIED_TIER_1 = [
    "(`customers.csv`) Có bao nhiêu khách hàng đã đăng ký tại thành phố '{CITY}'?",
    "(`customers.csv`) Có bao nhiêu khách hàng là thành viên cao cấp (`is_premium_member` là true)?",
    "(`products.csv`) Giá `base_price_vnd` trung bình cho tất cả các sản phẩm từ thương hiệu '{BRAND}' là bao nhiêu?",
    "(`orders.parquet`) Tổng số lượng mặt hàng đã bán cho `product_id` = {K_PROD} là bao nhiêu?",
    "(`orders.parquet`) Có bao nhiêu đơn hàng có trạng thái 'CANCELLED'?",
    "(`web_events.json`) Có tổng cộng bao nhiêu sự kiện web từ người dùng ẩn danh (nơi `customer_id` là null)?",
    "(`web_events.json`) Có bao nhiêu sự kiện web có `event_type` là 'search'?"
]

APPLIED_TIER_2 = [
    "Tổng doanh thu (`quantity` * `final_price_per_unit_vnd`) từ tất cả các đơn hàng 'COMPLETED' được đặt bởi khách hàng từ '{CITY}' là bao nhiêu?",
    "Có bao nhiêu đơn hàng 'COMPLETED' được đặt bởi các thành viên cao cấp đã đăng ký trong năm 2024?",
    "Tổng số lượng mặt hàng (`quantity`) đã bán từ thương hiệu '{BRAND}' là bao nhiêu?",
    "Tìm tổng doanh thu từ danh mục cấp cao nhất '{CATEGORY_L1}'.",
    "Có tổng cộng bao nhiêu sự kiện web được tạo ra bởi các khách hàng đã đăng ký trong năm 2023?",
    "Vào ngày nào (`YYYY-MM-DD`), thành phố '{CITY}' có số lượng đơn hàng 'COMPLETED' cao nhất?",
    "Có bao nhiêu khách hàng riêng biệt đã mua một sản phẩm từ thương hiệu '{BRAND}'?"
]

APPLIED_TIER_3 = [
    "Tổng doanh thu từ các đơn hàng 'COMPLETED' cho các sản phẩm trong danh mục 'Electronics' được đặt bởi các thành viên cao cấp từ '{CITY}' là bao nhiêu?",
    "Có bao nhiêu sự kiện `add_to_cart` dành cho `product_id` = {K_PROD}?",
    "Đối với mỗi `category_l1`, hãy tìm sản phẩm có `base_price_vnd` cao nhất. Giá của sản phẩm đó trong danh mục '{CATEGORY_L1}' là bao nhiêu?",
    "Có bao nhiêu đơn hàng 'COMPLETED' được đặt trong tháng {MONTH} của năm 2025?",
    "Tìm tất cả các khách hàng có tổng chi tiêu cho các đơn hàng 'COMPLETED' lớn hơn 50,000,000 VND. Có bao nhiêu khách hàng như vậy đến từ '{CITY}'?",
    "Tìm các phiên người dùng (user sessions) chứa cả sự kiện 'search' và sự kiện 'add_to_cart'. Có bao nhiêu `session_id` duy nhất như vậy?",
    "Đối với `customer_id` = {K_CUST}, ngày (`YYYY-MM-DD`) của đơn hàng 'COMPLETED' thứ hai của họ là ngày nào?",
    "Có bao nhiêu thành viên cao cấp từ '{CITY}' đã thêm một sản phẩm từ thương hiệu '{BRAND}' vào giỏ hàng của họ?"
]


def generate_questions(student_id_str):
    """Generates a unique and reproducible set of questions for a student."""
    try:
        s = int(student_id_str)
    except ValueError:
        return "Lỗi: Vui lòng nhập Mã số sinh viên hợp lệ (chỉ bao gồm số).", ""

    # --- Seed the random number generator for reproducibility ---
    # This is CRUCIAL: the same ID will always get the same "random" questions.
    random.seed(s)

    # --- Select questions from each pool ---
    conceptual = random.sample(CONCEPTUAL_QUESTIONS, NUM_CONCEPTUAL)
    tier1 = random.sample(APPLIED_TIER_1, NUM_APPLIED_T1)
    tier2 = random.sample(APPLIED_TIER_2, NUM_APPLIED_T2)
    tier3 = random.sample(APPLIED_TIER_3, NUM_APPLIED_T3)

    # --- Generate dynamic values from Student ID ---
    params = {
        'K_CUST': (s % 200) + 1,
        'K_PROD': (s % 100) + 1,
        'CITY': ['Ho Chi Minh City', 'Hanoi', 'Da Nang', 'Can Tho', 'Hai Phong'][s % 5],
        'BRAND': [f'Brand{chr(65+i)}' for i in range(10)][s % 10],
        'CATEGORY_L1': ['Electronics', 'Home Appliances', 'Fashion', 'Books'][s % 4],
        'MONTH': (s % 12) + 1
    }

    # --- Format the questions with the dynamic values ---
    all_questions = conceptual + tier1 + tier2 + tier3
    formatted_questions = []
    for i, q_template in enumerate(all_questions, 1):
        # Use .format_map() to safely substitute only the keys present in the template
        formatted_q = q_template.format_map(params)
        formatted_questions.append(f"{i}. {formatted_q}")

    header = f"Bộ câu hỏi cho Mã số sinh viên: {s}\nĐược tạo vào lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    header += "---------------------------------------\n"
    
    return header, "\n".join(formatted_questions)

# --- Streamlit UI ---
st.set_page_config(page_title="Cổng kiểm tra Thực Hành Big Data", layout="wide")

st.title("Cổng kiểm tra Thực Hành Big Data")
st.markdown("Chào mừng đến với cổng kiểm tra Thực Hành BigData! Vui lòng xem lại mô tả bộ dữ liệu và quy tắc nộp bài trước khi tạo câu hỏi của bạn.")

# --- EXPANDER FOR DATASET DESCRIPTION ---
with st.expander("📚 Nhấn vào đây để xem Mô tả Bộ dữ liệu"):
    st.markdown("""
        Bạn sẽ làm việc với một bộ dữ liệu lớn từ một nền tảng thương mại điện tử  có tên là **"ByteBazaar"**.

        | Tên tệp | Định dạng | Kích thước ước tính | Mô tả |
        | :--- | :--- | :--- | :--- |
        | `customers.csv` | CSV | ~10-20 MB | Chứa thông tin về các khách hàng đã đăng ký. |
        | `products.csv` | CSV | ~1-2 MB | Danh mục sản phẩm, với các danh mục và giá cả. |
        | `orders.parquet` | Parquet | ~1.5 GB | Dữ liệu giao dịch cho tất cả các đơn hàng đã đặt. |
        | `web_events.json` | JSONL | ~2.0 GB | Nhật ký sự kiện web thô từ hoạt động của người dùng trên nền tảng. |

        ---
        #### Chi tiết Lược đồ (Schema)

        1. `customers.csv`

        Chứa thông tin tài khoản cho mỗi khách hàng.
        | Tên cột              | Kiểu dữ liệu | Mô tả                                                     |
        |-----------------------|--------------|-----------------------------------------------------------|
        | customer_id           | Integer      | Mã định danh duy nhất cho mỗi khách hàng. (Khóa chính)
        | signup_date           | String       | Ngày khách hàng đăng ký (YYYY-MM-DD).
        | city                  | String       | Thành phố của khách hàng (ví dụ: 'Ho Chi Minh City', 'Hanoi').
        | last_active_platform  | String       | Nền tảng cuối cùng được sử dụng ('ios', 'android', 'web').
        | is_premium_member     | Boolean      | `true` nếu khách hàng có đăng ký gói cao cấp.

        2. `products.csv`

        Danh sách tổng hợp tất cả các sản phẩm được bán trên ByteBazaar.
        | Tên cột               | Kiểu dữ liệu | Mô tả                                                     |
        |-----------------------|--------------|-----------------------------------------------------------|
        | product_id            | Integer      | Mã định danh duy nhất cho mỗi sản phẩm. (Khóa chính)
        | product_name          | String       | Tên của sản phẩm.
        | category_l1           | String       | Danh mục sản phẩm cấp cao nhất (ví dụ: 'Electronics').
        | category_l2           | String       | Danh mục phụ (ví dụ: 'Laptops').
        | brand                 | String       | Thương hiệu sản phẩm (ví dụ: 'BrandA').
        | base_price_vnd        | Long         | Giá tiêu chuẩn của sản phẩm bằng Việt Nam Đồng.

        3. `orders.parquet`

        Đây là một tệp Parquet lớn chứa tất cả các đơn hàng đã hoàn thành, đã hủy và đã trả lại.
        | Tên cột                  | Kiểu dữ liệu | Mô tả                                                     |
        |--------------------------|--------------|-----------------------------------------------------------|
        | order_id                 | String       | Mã định danh duy nhất cho đơn hàng.
        | customer_id              | Integer      | Liên kết đến `customers.csv`. (Khóa ngoại)
        | product_id               | Integer      | Liên kết đến `products.csv`. (Khóa ngoại)
        | order_timestamp          | String       | Dấu thời gian của đơn hàng ở định dạng ISO 8601 (YYYY-MM-DDTHH:mm:ssZ).
        | quantity                 | Integer      | Số lượng đơn vị đã mua.
        | final_price_per_unit_vnd | Long         | Giá thực tế đã trả cho mỗi đơn vị sau khi giảm giá.
        | order_status             | String       | 'COMPLETED', 'CANCELLED', hoặc 'RETURNED'.

        4. `web_events.json`

        Đây là tệp lớn nhất và phức tạp nhất, chứa nhật ký sự kiện thô. Tệp ở định dạng JSONL, trong đó mỗi dòng là một đối tượng JSON hoàn chỉnh.
        | Tên cột               | Kiểu dữ liệu | Mô tả                                                     |
        |-----------------------|--------------|-----------------------------------------------------------|
        | event_timestamp       | Long         | Dấu thời gian Unix epoch tính bằng mili giây.
        | session_id            | String       | ID duy nhất cho phiên của người dùng.
        | customer_id           | Integer      | ID của khách hàng. Trường này có thể là `null` đối với người dùng ẩn danh.
        | event_type            | String       | Hành động người dùng đã thực hiện ('page_view', 'add_to_cart', 'search').
        | event_properties      | Struct/JSON  | Một nested JSON object với các chi tiết phụ thuộc vào `event_type`.

        Ví dụ về cấu trúc `event_properties`:

            Nếu event_type là 'page_view': `{"url": "...", "referrer": "..."}`

            Nếu event_type là 'add_to_cart': `{"product_id": 12345, "quantity": 1}`

            Nếu event_type là 'search': `{"search_query": "...", "results_count": 42}`
        ---
        #### Gợi ý 💡
        * **Nhiều định dạng tệp:** Đọc dữ liệu chính xác từ CSV, Parquet và JSONL.
        * **Làm sạch dữ liệu:** Xử lý các giá trị `null` tiềm ẩn, đặc biệt là trong `web_events.json`.
        * **Chuyển đổi Dấu thời gian:** Lưu ý các định dạng dấu thời gian khác nhau trong `orders.parquet` (ISO 8601) và `web_events.json` (Unix mili giây).
    """)

# --- EXPANDER FOR SUBMISSION INSTRUCTIONS ---
with st.expander("❗ QUAN TRỌNG: Nhấn vào đây để đọc Hướng dẫn Nộp bài"):
    st.markdown("""
        Để được chấm điểm, bạn phải nộp một tệp nén `.zip` duy nhất lên Google Classroom.
        Tệp **phải được đặt tên theo mã số sinh viên của bạn** (ví dụ: `20012345.zip`).
        
        **Các bài nộp có tên tệp zip không chính xác hoặc thiếu/sai tên tệp bên trong sẽ nhận điểm 0.**

        Tệp `.zip` của bạn **phải** chứa 3 tệp sau bên trong:

        1.  **`questions.txt`**
            * Toàn bộ văn bản không chỉnh sửa của các câu hỏi được tạo cho bạn bởi cổng thông tin này.

        2.  **`answers.txt`**
            * Tệp này **chỉ được chứa câu trả lời cuối cùng của bạn**, mỗi câu trả lời trên một dòng.
            * Đối với câu hỏi trắc nghiệm, hãy viết chữ cái của lựa chọn (ví dụ: `A`).
            * Đối với câu hỏi ứng dụng, hãy viết kết quả bằng số hoặc văn bản.
            * **Ví dụ định dạng:**
                ```
                1. B
                2. C
                3. 17542
                ...
                ```

        3.  **`<MSSV_Cua_Ban>.py`**
            * Script PySpark hoàn chỉnh, có thể chạy được của bạn (ví dụ: `20012345.py`).
            * Script này sẽ được chạy để xác minh rằng nó tạo ra các câu trả lời bạn đã nộp.

    """)

st.divider()

st.write("Vui lòng nhập Mã số sinh viên (MSSV) của bạn để tạo bộ câu hỏi duy nhất. Sao chép toàn bộ câu hỏi và lưu nó dưới dạng `questions.txt` để nộp bài.")

student_id = st.text_input("Nhập Mã số sinh viên của bạn:", placeholder="ví dụ: 20012345")

if st.button("Tạo câu hỏi của tôi"):
    if student_id:
        with st.spinner("Đang tạo bộ câu hỏi duy nhất của bạn..."):
            header, questions_text = generate_questions(student_id)
            if "Lỗi" in header:
                st.error(header)
            else:
                st.success("Câu hỏi của bạn đã được tạo thành công!")
                full_text = header + questions_text
                st.text_area("Câu hỏi của bạn (Sao chép toàn bộ văn bản từ hộp này):", full_text, height=600)
    else:
        st.warning("Vui lòng nhập Mã số sinh viên.")