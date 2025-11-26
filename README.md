# So Sánh Redis vs MySQL

## Tổng Quan
Dự án này trình bày và so sánh hiệu suất cũng như hành vi của Redis và MySQL qua nhiều thao tác cơ sở dữ liệu khác nhau trên dữ liệu hình ảnh y tế. Hệ thống xử lý metadata DICOM và ghi chú lâm sàng để thể hiện sự khác biệt về:

- Hiệu suất tìm kiếm toàn văn (full-text search)
- Tốc độ tra cứu key-value
- Thao tác nguyên tử (atomic operations)
- Xử lý giao dịch (transactions)
- Quản lý lỗi
- Chiến lược kiểm soát đồng thời (concurrency control)

## Tính Năng

### Xử Lý Dữ Liệu
- Import metadata hình ảnh y tế từ file JSON
- Tải ghi chú lâm sàng từ file CSV
- Lưu trữ dữ liệu bệnh nhân, bản ghi hình ảnh và metadata DICOM trong cả Redis và MySQL

### So Sánh Hiệu Suất
1. **Tìm Kiếm Toàn Văn**: Tìm kiếm thuật ngữ y tế trong ghi chú lâm sàng
2. **Tra Cứu Key**: Truy xuất trực tiếp bản ghi bệnh nhân theo ID
3. **Bộ Đếm Nguyên Tử**: Thao tác tăng số với tần suất cao
4. **Giao Dịch Thành Công**: Minh họa giao dịch ACID
5. **Xử Lý Lỗi Giao Dịch**: So sánh hành vi rollback
6. **Kiểm Soát Đồng Thời**: Khóa bi quan vs khóa lạc quan
7. **Đồng Thời Thực Tế**: Kịch bản đa client thực tế

## Yêu Cầu Hệ Thống

- **Node.js** >= 16
- **MySQL** 8+
- **Redis** 6 hoặc 7
- **npm** package manager

## Cấu Trúc Dự Án

```
/project
├── index.js                          # File chính của ứng dụng
├── package.json                      # Các dependencies của Node.js
├── README.md                         # File này
├── data/
│   ├── metadata/                     # Các file JSON metadata (bắt buộc)
│   │   ├── 1.json
│   │   ├── 2.json
│   │   └── ...
│   └── text_data.csv                 # CSV ghi chú lâm sàng (bắt buộc)
└── node_modules/                     # Các dependencies đã cài đặt
```

## Yêu Cầu Dữ Liệu

### 1. File JSON Metadata

Đặt các file JSON metadata DICOM đã parse vào thư mục `./data/metadata/`. Mỗi file JSON đại diện cho một bệnh nhân và chứa một mảng metadata của các hình ảnh.

## Hướng Dẫn Cài Đặt

### 1. Cài Đặt Dependencies

```bash
npm install
```

**Các package cần thiết:**
- `mysql2` - Driver MySQL
- `redis` - Client Redis
- `xlsx` - Xử lý file Excel
- `csv-parser` - Parse CSV

### 2. Cấu Hình MySQL

Tạo database:
```sql
CREATE DATABASE dbms;
```

Cập nhật thông tin đăng nhập MySQL trong `index.js` (khoảng dòng 560):
```javascript
const db = await mysql.createConnection({
  host: "127.0.0.1",
  user: "root",
  password: "MẬT_KHẨU_CỦA_BẠN",  // ⚠️ Thay đổi ở đây
  database: "dbms",
  port: 3306
});
```

### 3. Chuẩn Bị File Dữ Liệu

Đảm bảo các file sau tồn tại:
- `./data/metadata/*.json` - Các file metadata bệnh nhân
- `./data/text_data.csv` - Ghi chú lâm sàng

### 4. Chạy Ứng Dụng

```bash
node index.js
```

## Cách Sử Dụng

Khi chạy, menu tương tác sẽ xuất hiện:

```
============================================================
SO SÁNH DỮ LIỆU Y TẾ: Redis vs MySQL
============================================================
1. Tìm Kiếm Toàn Văn (Ghi Chú Lâm Sàng)
2. Tra Cứu Key (Bệnh Nhân Theo ID)
3. Bộ Đếm Nguyên Tử
4. Giao Dịch Thành Công
5. Xử Lý Lỗi Giao Dịch
6. Kiểm Soát Đồng Thời (Demo Đơn Giản)
7. Đồng Thời MySQL Thực Tế (Khóa Bi Quan)
8. Đồng Thời Redis Thực Tế (Khóa Lạc Quan)
0. Thoát
============================================================
```