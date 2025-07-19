# Tuần 1 – Nền tảng Cơ bản: Ngôn ngữ, Cơ sở dữ liệu, Linux

## 1. Python

### 1.1. Kiểu dữ liệu cơ bản
Python là một ngôn ngữ lập trình mạnh mẽ, dễ học và được sử dụng rộng rãi trong xử lý dữ liệu. Các kiểu dữ liệu cơ bản trong Python bao gồm:

| **Kiểu dữ liệu** | **Mô tả**                          | **Ví dụ**                           |
|-------------------|------------------------------------|-------------------------------------|
| `int`            | Số nguyên                          | `x = 10`                           |
| `float`          | Số thực (có phần thập phân)        | `pi = 3.14`                        |
| `str`            | Chuỗi ký tự                        | `s = "Python"`                     |
| `bool`           | Giá trị logic (True/False)         | `flag = True`                      |
| `list`           | Danh sách, có thể thay đổi         | `a = [1, 2, 3]`                    |
| `tuple`          | Bộ giá trị, không thể thay đổi     | `t = (1, 2)`                       |
| `set`            | Tập hợp không chứa phần tử trùng   | `s = {1, 2, 3}`                    |
| `dict`           | Từ điển ánh xạ key-value           | `d = {"name": "Alice", "age": 25}` |

### 1.2. Toán tử
Python hỗ trợ nhiều loại toán tử để thao tác trên dữ liệu:

- **Toán tử toán học**:
  - `+` (cộng), `-` (trừ), `*` (nhân), `/` (chia lấy nguyên), `//` (chia lấy nguyên), `%` (chia lấy dư), `**` (lũy thừa)
  - Ví dụ: `5 + 3 = 8`, `10 // 3 = 3`, `2 ** 3 = 8`

- **Toán tử so sánh**:
  - `==` (bằng), `!=` (khác), `>`, `<`, `>=`, `<=`
  - Ví dụ: `5 > 3` trả về `True`

- **Toán tử logic**:
  - `and`, `or`, `not`
  - Ví dụ: `(5 > 3) and (2 < 4)` trả về `True`

### 1.3. Cấu trúc điều khiển
#### Câu lệnh điều kiện
- Sử dụng `if`, `elif`, `else` để kiểm soát luồng chương trình:
```python
x = 10
if x > 0:
    print("Số dương")
elif x == 0:
    print("Số 0")
else:
    print("Số âm")
```

#### Vòng lặp
- **Vòng lặp `for`**:
```python
for i in range(5):  # Duyệt từ 0 đến 4
    print(i)
```
- **Vòng lặp `while`**:
```python
count = 0
while count < 5:
    print(count)
    count += 1
```

### 1.4. Hàm
Hàm giúp tái sử dụng mã và tổ chức chương trình:
```python
def tinh_tong(a, b):
    return a + b

result = tinh_tong(3, 5)  # Kết quả: 8
```
- **Tham số mặc định**:
```python
def chao(name="Guest"):
    print(f"Xin chào, {name}")

chao()         # In: Xin chào, Guest
chao("Alice")  # In: Xin chào, Alice
```

### 1.5. List, Tuple, Set, Dict
| **Đặc điểm**              | **List**                     | **Tuple**                    | **Set**                      | **Dict**                             |
|---------------------------|------------------------------|------------------------------|------------------------------|--------------------------------------|
| **Ký hiệu khởi tạo**      | `[ ]`                        | `( )`                        | `{ }`                        | `{key: value}`                       |
| **Có thứ tự**             | Có                           | Có                           | Có                           | Có                                   |
| **Thay đổi được**         | Có                           | Không                        | Có                           | Có                                   |
| **Cho phép trùng lặp**    | Có                           | Có                           | Không                        | Key: Không / Value: Có               |
| **Truy cập qua chỉ số**   | Có                           | Có                           | Không                        | Có (qua key)                         |
| **Dùng khi nào**          | Danh sách có thể thay đổi    | Dữ liệu cố định              | Tập hợp giá trị duy nhất     | Ánh xạ key-value                     |
| **Ví dụ**                 | `[1, 2, 3]`                  | `(1, 2, 3)`                  | `{1, 2, 3}`                  | `{"name": "Alice", "age": 25}`       |

- **Ví dụ thao tác**:
```python
# List
my_list = [1, 2, 3]
my_list.append(4)  # Thêm phần tử
print(my_list)     # [1, 2, 3, 4]

# Tuple
my_tuple = (1, 2, 3)
print(my_tuple[0])  # 1

# Set
my_set = {1, 2, 2, 3}
print(my_set)       # {1, 2, 3}

# Dict
my_dict = {"name": "Alice", "age": 25}
print(my_dict["name"])  # Alice
```

### 1.6. Đọc/ghi file
#### Đọc/ghi file cơ bản
```python
# Đọc file
with open("data.txt", "r") as file:
    content = file.read()
    print(content)

# Ghi file
with open("output.txt", "w") as file:
    file.write("Hello, Python!")
```

#### Đọc/ghi file bằng Pandas
```python
import pandas as pd

# Đọc file CSV
df = pd.read_csv("data.csv")
print(df.head())

# Ghi file CSV
df.to_csv("output.csv", index=False)
```

---

## 2. SQL Cơ bản

### 2.1. Các nhóm lệnh chính trong SQL
#### Ngôn ngữ định nghĩa dữ liệu (DDL)
| **Lệnh**   | **Mô tả**                                      |
|------------|------------------------------------------------|
| `CREATE`   | Tạo bảng hoặc cơ sở dữ liệu                    |
| `ALTER`    | Sửa đổi cấu trúc bảng                          |
| `DROP`     | Xóa bảng hoặc cơ sở dữ liệu                    |
| `TRUNCATE` | Xóa toàn bộ dữ liệu trong bảng (nhanh hơn DELETE) |

#### Ngôn ngữ thao tác dữ liệu (DML)
| **Lệnh**   | **Mô tả**                                      |
|------------|------------------------------------------------|
| `SELECT`   | Truy vấn dữ liệu                               |
| `INSERT`   | Thêm dữ liệu mới                               |
| `UPDATE`   | Cập nhật dữ liệu                               |
| `DELETE`   | Xóa dữ liệu                                    |

### 2.2. Cấu trúc bảng
- **Bảng (Table)**: Tập hợp các hàng và cột.
- **Hàng (Row/Record)**: Một bản ghi dữ liệu.
- **Cột (Column/Field)**: Kiểu dữ liệu như `VARCHAR`, `INT`, `DATE`, `BOOLEAN`.

**Ví dụ tạo bảng**:
```sql
CREATE TABLE nhan_vien (
    id INT PRIMARY KEY,
    ten VARCHAR(50),
    phong_ban VARCHAR(50),
    luong FLOAT
);
```

### 2.3. GROUP BY – Nhóm dữ liệu
Sử dụng để nhóm dữ liệu và áp dụng hàm tổng hợp:
```sql
-- Đếm số nhân viên theo phòng ban
SELECT phong_ban, COUNT(*) AS so_nhan_vien
FROM nhan_vien
GROUP BY phong_ban;

-- Lương trung bình theo phòng ban
SELECT phong_ban, AVG(luong) AS luong_tb
FROM nhan_vien
GROUP BY phong_ban;
```
- Hàm tổng hợp: `COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()`

### 2.4. HAVING – Lọc sau khi nhóm
```sql
-- Phòng ban có > 5 nhân viên
SELECT phong_ban, COUNT(*) AS so_nv
FROM nhan_vien
GROUP BY phong_ban
HAVING COUNT(*) > 5;
```

### 2.5. ORDER BY – Sắp xếp
```sql
-- Sắp xếp tăng dần
SELECT * FROM nhan_vien ORDER BY luong ASC;

-- Sắp xếp giảm dần
SELECT * FROM nhan_vien ORDER BY luong DESC;

-- Sắp xếp nhiều cột
SELECT * FROM nhan_vien 
ORDER BY phong_ban ASC, luong DESC;
```

### 2.6. JOIN – Kết hợp nhiều bảng
- `INNER JOIN`: Lấy giao nhau của 2 bảng.
- `LEFT JOIN`: Lấy toàn bộ bảng trái và dữ liệu giao nhau.
- `RIGHT JOIN`: Lấy toàn bộ bảng phải và dữ liệu giao nhau.
- `FULL OUTER JOIN`: Lấy tất cả dữ liệu từ cả hai bảng.

**Ví dụ**:
```sql
SELECT nhan_vien.ten, phong_ban.ten_phong
FROM nhan_vien
INNER JOIN phong_ban ON nhan_vien.phong_ban = phong_ban.id;
```

### 2.7. Vấn đề hiệu năng thường gặp khi truy vấn
- **Dữ liệu lớn**: Hàng triệu dòng làm chậm truy vấn.
- **`SELECT *`**: Lấy tất cả cột gây lãng phí tài nguyên.
- **Thiếu index**: Tìm kiếm tuần tự chậm.
- **JOIN không rõ ràng**: Thiếu điều kiện hoặc index trên cột nối.

### 2.8. Kỹ thuật tối ưu hóa truy vấn
#### 2.8.1. Sử dụng chỉ mục (Index)
- Tạo index trên cột thường xuyên được truy vấn hoặc dùng trong `WHERE`, `JOIN`:
```sql
CREATE INDEX idx_phong_ban ON nhan_vien(phong_ban);
```

#### 2.8.2. Viết lại truy vấn
- Thay subquery bằng `JOIN` hoặc sử dụng hàm tổng hợp:
```sql
-- Thay vì:
SELECT * FROM nhan_vien WHERE id IN (SELECT id FROM phong_ban WHERE ten_phong = 'IT');
-- Dùng JOIN:
SELECT nhan_vien.* FROM nhan_vien
JOIN phong_ban ON nhan_vien.phong_ban = phong_ban.id
WHERE phong_ban.ten_phong = 'IT';
```

#### 2.8.3. Tối ưu hóa JOIN
- Sử dụng index trên cột nối, giảm số bảng nối.
- Chọn `INNER JOIN` thay vì `OUTER JOIN` khi có thể.

#### 2.8.4. Giới hạn dữ liệu trả về
- Chỉ chọn cột cần thiết:
```sql
SELECT ten, luong FROM nhan_vien;  -- Tốt hơn SELECT *
```

#### 2.8.5. Sử dụng subquery cẩn thận
- Tránh subquery trong `WHERE` hoặc `SELECT` nếu có thể thay bằng `JOIN`.

#### 2.8.6. Stored Procedure và View
- Lưu trữ truy vấn tối ưu hóa:
```sql
CREATE VIEW view_nhan_vien AS
SELECT ten, phong_ban FROM nhan_vien;
```

#### 2.8.7. Phân tích kế hoạch thực thi
- Sử dụng `EXPLAIN` để kiểm tra cách truy vấn được thực thi:
```sql
EXPLAIN SELECT * FROM nhan_vien WHERE phong_ban = 'IT';
```

#### 2.8.8. Tránh hàm trên cột trong WHERE
- Thay vì:
```sql
SELECT * FROM nhan_vien WHERE UPPER(ten) = 'ALICE';
```
- Sử dụng:
```sql
SELECT * FROM nhan_vien WHERE ten = 'Alice';
```

#### 2.8.9. UNION ALL thay vì UNION
- `UNION ALL` không loại bỏ trùng lặp, nhanh hơn:
```sql
SELECT ten FROM nhan_vien UNION ALL SELECT ten FROM khach_hang;
```

#### 2.8.10. EXISTS thay vì COUNT
- Thay vì:
```sql
SELECT * FROM nhan_vien WHERE (SELECT COUNT(*) FROM phong_ban WHERE id = nhan_vien.phong_ban) > 0;
```
- Sử dụng:
```sql
SELECT * FROM nhan_vien WHERE EXISTS (SELECT 1 FROM phong_ban WHERE id = nhan_vien.phong_ban);
```

---

## 3. Linux Command Line

### 3.1. Quản lý File và Thư mục
| **Câu lệnh**       | **Mô tả**                                      |
|--------------------|------------------------------------------------|
| `pwd`             | Hiển thị thư mục hiện tại                      |
| `ls`              | Liệt kê nội dung thư mục                       |
| `ls -l`           | Hiển thị chi tiết dạng danh sách               |
| `ls -lh`          | Hiển thị chi tiết, dung lượng dễ đọc           |
| `cd <dir>`        | Di chuyển tới thư mục                          |
| `cd ..`           | Quay về thư mục cha                            |
| `mkdir <dir>`     | Tạo thư mục mới                                |
| `mkdir -p`        | Tạo thư mục kể cả khi thư mục cha chưa tồn tại  |
| `touch <file>`    | Tạo file rỗng                                  |
| `cp <src> <dest>` | Sao chép file/thư mục                          |
| `mv <src> <dest>` | Di chuyển hoặc đổi tên file/thư mục            |
| `rm <file>`       | Xóa file                                       |
| `rm -r <dir>`     | Xóa thư mục và toàn bộ nội dung                |
| `du -sh *`        | Xem dung lượng file/thư mục                    |
| `find . -name "*.csv"` | Tìm tất cả file .csv                      |

**Ví dụ**:
```bash
ls -lh  # Liệt kê chi tiết các file/thư mục với kích thước dễ đọc
```

### 3.2. Xem và chỉnh sửa nội dung
| **Câu lệnh**      | **Mô tả**                                      |
|--------------------|------------------------------------------------|
| `cat <file>`      | Hiển thị toàn bộ nội dung file                 |
| `less <file>`     | Xem nội dung file theo trang                   |
| `head -n 10 <file>` | Hiển thị 10 dòng đầu tiên của file           |
| `tail -f <file>`  | Theo dõi nội dung file thời gian thực          |
| `nano <file>`     | Mở trình chỉnh sửa văn bản nano                |

**Ví dụ**:
```bash
head -n 10 /data/final_converted_data.csv  # Xem 10 dòng đầu của file CSV
```

### 3.3. Quyền truy cập và người dùng
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `chmod <mode> <file>`           | Thay đổi quyền truy cập (e.g., `chmod 755 file`) |
| `chown <user>:<group> <file>`   | Thay đổi chủ sở hữu file                       |
| `whoami`                        | Hiển thị người dùng hiện tại                   |
| `sudo <command>`                | Thực thi với quyền admin                       |
| `su`                            | Chuyển sang người dùng khác                    |

### 3.4. Nén và giải nén
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `tar -cvf archive.tar folder`   | Nén folder thành file tar                      |
| `tar -xvf archive.tar`          | Giải nén file tar                              |
| `gzip file`                     | Nén file thành .gz                             |
| `gunzip file.gz`                | Giải nén file .gz                              |

### 3.5. Xử lý File (CSV, JSON, Log, Parquet, ...)
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `cut -d',' -f1,3 file.csv`      | Cắt và hiển thị cột 1, 3 của file CSV          |
| `awk -F',' '{print $1, $2}' file.csv` | Trích xuất cột 1, 2 từ CSV               |
| `sed 's/foo/bar/g' file.txt`    | Thay thế chuỗi trong file                      |
| `sort file.txt`                 | Sắp xếp nội dung file                          |
| `uniq file.txt`                 | Loại bỏ dòng trùng lặp                         |
| `wc -l file.txt`                | Đếm số dòng trong file                         |
| `split -l 100000 data.csv`      | Tách file lớn thành file nhỏ (100k dòng/file)   |
| `jq . file.json`                | Xử lý JSON                                     |
| `yq . file.yaml`                | Xử lý YAML                                     |

### 3.6. Quản lý Process và Tài nguyên
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `top`, `htop`                   | Theo dõi tiến trình hệ thống                   |
| `ps aux | grep python`          | Tìm tiến trình liên quan đến Python            |
| `kill -9 <PID>`                 | Dừng tiến trình theo PID                       |
| `free -h`                       | Xem RAM đang sử dụng                           |
| `df -h`                         | Xem dung lượng ổ đĩa                           |
| `ulimit -n`                     | Xem/tăng số lượng file descriptor              |

### 3.7. Làm việc với dữ liệu từ Command-line
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `curl <url>`                    | Gọi API hoặc tải file từ web                   |
| `wget <url>`                    | Tải file từ web                                |
| `ftp`, `sftp`, `scp`, `rsync`   | Truyền file từ xa                              |
| `ssh user@host`                 | Kết nối đến server                             |
| `grep "pattern" file`           | Tìm chuỗi trong file                           |
| `zcat file.gz`                  | Xem file nén .gz không cần giải nén            |

### 3.8. Kết nối và tương tác với Database
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `psql -U user -d dbname`        | Kết nối PostgreSQL                             |
| `mysql -u user -p`              | Kết nối MySQL                                  |
| `clickhouse-client`               | Kết nối ClickHouse                             |
| `sqlite3 mydata.db`             | Kết nối SQLite CLI                             |
| `csvsql`                        | Thao tác CSV như SQL (cài bằng pip)            |

**Ví dụ**:
```bash
mysql -h host.docker.internal -u root -p
```

### 3.9. Xử lý song song
| **Câu lệnh**                     | **Mô tả**                                      |
|----------------------------------|------------------------------------------------|
| `xargs -P <n>`                 | Chạy song song n lệnh                          |
| `parallel`                      | GNU Parallel – xử lý hàng loạt file            |
| `while read line; do ...; done < file.txt` | Duyệt từng dòng để xử lý             |

**Ví dụ**:
```bash
cat file.txt | xargs -P 4 -I {} sh -c "echo Processing {}; sleep 1"
```
# Tuần 2 – Kiến trúc Dữ liệu: OLTP, OLAP, CAP, ETL

## 1. OLTP vs OLAP

### 1.1. Tổng quan
- **OLTP (Online Transaction Processing)**: Hệ thống xử lý giao dịch trực tuyến, được thiết kế để quản lý các giao dịch kinh doanh hàng ngày như thêm, sửa, xóa dữ liệu trong thời gian thực.
- **OLAP (Online Analytical Processing)**: Hệ thống phân tích trực tuyến, tập trung vào xử lý truy vấn phức tạp trên dữ liệu lớn, thường là dữ liệu lịch sử, để hỗ trợ ra quyết định và phân tích.

### 1.2. So sánh chi tiết OLTP và OLAP

| **Đặc điểm**                | **OLTP**                                                                 | **OLAP**                                                                 |
|-----------------------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------|
| **Mục tiêu chính**          | Xử lý số lượng lớn các giao dịch nhỏ, thường xuyên (đọc/ghi) trong thời gian thực | Thực hiện truy vấn phân tích phức tạp trên tập dữ liệu lớn, lịch sử |
| **Tính chất dữ liệu**       | Dữ liệu hiện tại, chi tiết, thường xuyên thay đổi                         | Dữ liệu tổng hợp, lịch sử, ít thay đổi                                   |
| **Loại truy vấn**           | `INSERT`, `UPDATE`, `DELETE`, truy vấn đơn giản                          | `SELECT` với `JOIN`, `GROUP BY`, `drill-down`, `pivot`                   |
| **Tốc độ phản hồi**         | Yêu cầu tốc độ cao, mili giây để phục vụ nhiều người dùng đồng thời       | Chấp nhận chậm hơn (giây đến phút) cho truy vấn phức tạp                 |
| **Mô hình dữ liệu**         | Chuẩn hóa (3NF) để tránh dư thừa dữ liệu                                  | Phi chuẩn hóa (Star schema, Snowflake schema) để tối ưu phân tích        |
| **Người dùng chính**        | Nhân viên nghiệp vụ (kế toán, bán hàng, nhập kho)                         | Nhà quản lý, nhà phân tích dữ liệu, cấp điều hành                        |
| **Ví dụ ứng dụng**          | Hệ thống ngân hàng, website thương mại điện tử, quản lý tồn kho           | Hệ thống phân tích doanh số, BI dashboard, hỗ trợ ra quyết định          |
| **Cơ sở dữ liệu phù hợp**   | MySQL, PostgreSQL, Oracle, SQL Server                                     | BigQuery, Snowflake, Redshift                                            |
| **Dung lượng lưu trữ**      | Tương đối nhỏ, chỉ lưu dữ liệu hiện tại                                  | Lớn, lưu trữ dữ liệu lịch sử và tổng hợp                                 |
| **Cập nhật và sao lưu**     | Cập nhật thời gian thực, sao lưu thường xuyên                            | Cập nhật định kỳ (hàng giờ/ngày), sao lưu ít hơn                         |

**Ví dụ minh họa**:
- **OLTP**: Ghi lại giao dịch mua hàng trên Shopee, cập nhật số dư tài khoản ngân hàng ngay lập tức.
- **OLAP**: Phân tích xu hướng mua sắm trong 6 tháng qua, tạo báo cáo doanh thu theo khu vực.

### 1.3. Mô hình dữ liệu
- **OLTP**: Sử dụng mô hình chuẩn hóa (3NF - Third Normal Form) để giảm dư thừa dữ liệu và đảm bảo tính nhất quán.
  ```sql
  CREATE TABLE phong_ban (
      id INT PRIMARY KEY,
      ten_phong VARCHAR(50)
  );
  CREATE TABLE nhan_vien (
      id INT PRIMARY KEY,
      ten VARCHAR(50),
      phong_ban_id INT,
      FOREIGN KEY (phong_ban_id) REFERENCES phong_ban(id)
  );
  ```

- **OLAP**: Sử dụng mô hình phi chuẩn hóa như Star Schema hoặc Snowflake Schema để tối ưu hóa truy vấn phân tích.
  - **Star Schema**: Một bảng sự kiện trung tâm (Fact Table) kết nối với các bảng chiều (Dimension Tables).
  - **Snowflake Schema**: Tương tự Star Schema nhưng các bảng chiều được chuẩn hóa thêm.
  ```sql
  -- Fact Table (Doanh thu)
  CREATE TABLE fact_doanh_thu (
      id INT PRIMARY KEY,
      ngay DATE,
      san_pham_id INT,
      khach_hang_id INT,
      so_luong INT,
      doanh_thu DECIMAL(10,2)
  );
  -- Dimension Table (Sản phẩm)
  CREATE TABLE dim_san_pham (
      san_pham_id INT PRIMARY KEY,
      ten_san_pham VARCHAR(50),
      danh_muc VARCHAR(50)
  );
  ```

## 2. ETL vs ELT

### 2.1. Tổng quan
- **ETL (Extract, Transform, Load)**: Trích xuất dữ liệu từ nguồn, biến đổi (làm sạch, chuẩn hóa), sau đó tải vào Data Warehouse.
- **ELT (Extract, Load, Transform)**: Trích xuất và tải dữ liệu thô vào Data Warehouse trước, sau đó thực hiện biến đổi trong hệ thống đích.

### 2.2. So sánh chi tiết ETL và ELT

| **Đặc điểm**                | **ETL**                                                                 | **ELT**                                                                 |
|-----------------------------|-------------------------------------------------------------------------|-------------------------------------------------------------------------|
| **Quy trình xử lý**         | Trích xuất → Biến đổi → Tải vào                                         | Trích xuất → Tải vào → Biến đổi                                         |
| **Nơi xử lý biến đổi**      | Ngoài Data Warehouse (server ETL trung gian)                            | Trong Data Warehouse (tận dụng sức mạnh tính toán của hệ thống đích)    |
| **Phù hợp với**             | Hệ thống truyền thống, dữ liệu nhỏ, khả năng xử lý giới hạn              | Hệ thống cloud-based, dữ liệu lớn, khả năng xử lý mạnh                   |
| **Ưu điểm**                 | Kiểm soát chất lượng dữ liệu trước khi lưu, giảm tải cho Data Warehouse  | Tận dụng hiệu năng tính toán của Data Warehouse, linh hoạt hơn           |
| **Nhược điểm**              | Yêu cầu hạ tầng riêng, khó mở rộng với Big Data                         | Có thể làm chậm Data Warehouse nếu không tối ưu, lưu trữ dữ liệu thô    |
| **Công nghệ phổ biến**      | Talend, Informatica, Apache NiFi                                        | Snowflake, BigQuery, Redshift                                           |
| **Hiệu suất với Big Data**  | Giới hạn do xử lý biến đổi ngoài hệ thống                               | Tốt hơn, tận dụng sức mạnh của cloud                                    |
| **Thời gian thực**          | Kém, thường xử lý theo lô                                               | Tốt hơn khi kết hợp với streaming                                       |
| **Bảo mật dữ liệu thô**     | Không lưu trữ lâu dài                                                   | Có thể lưu trữ để truy vết                                              |

**Ví dụ minh họa**:
- **ETL**: Trích xuất dữ liệu từ MySQL, làm sạch và chuẩn hóa trên server Talend, sau đó tải vào Snowflake.
- **ELT**: Trích xuất dữ liệu thô từ Kafka, tải trực tiếp vào BigQuery, sau đó dùng SQL để biến đổi trong BigQuery.

### 2.3. Quy trình minh họa
- **ETL**:
  ```plaintext
  1. Extract: Lấy dữ liệu từ API, CSV, hoặc database.
  2. Transform: Làm sạch, chuẩn hóa, tính toán (e.g., tổng hợp doanh thu).
  3. Load: Tải dữ liệu đã biến đổi vào Data Warehouse.
  ```
- **ELT**:
  ```plaintext
  1. Extract: Lấy dữ liệu thô từ nguồn.
  2. Load: Tải trực tiếp vào Data Warehouse (e.g., Snowflake).
  3. Transform: Sử dụng SQL hoặc công cụ trong Data Warehouse để biến đổi.
  ```

## 3. CAP Theorem

### 3.1. Tổng quan
CAP là viết tắt của **Consistency**, **Availability**, và **Partition Tolerance**, mô tả ba thuộc tính của hệ thống phân tán:

- **Consistency (Tính nhất quán)**: Mọi node trong hệ thống có cùng dữ liệu tại một thời điểm. Khi ghi dữ liệu, các truy vấn sau sẽ nhận giá trị mới nhất hoặc lỗi.
- **Availability (Tính khả dụng)**: Mọi yêu cầu (đọc/ghi) đều nhận được phản hồi, dù có thể là dữ liệu cũ.
- **Partition Tolerance (Chịu lỗi phân vùng)**: Hệ thống vẫn hoạt động dù có sự cố mạng chia tách các node.

**Định lý CAP**: Một hệ thống phân tán chỉ có thể đảm bảo tối đa 2 trong 3 thuộc tính trên.

### 3.2. Các loại hệ thống theo CAP

| **Loại hệ thống** | **Ưu tiên**                           | **Ví dụ**                       |
|-------------------|---------------------------------------|---------------------------------|
| **CA**            | Consistency + Availability             | RDBMS đơn máy (MySQL, PostgreSQL) |
| **CP**            | Consistency + Partition Tolerance      | HBase, MongoDB (cấu hình mạnh)  |
| **AP**            | Availability + Partition Tolerance     | Cassandra, DynamoDB             |

**Ví dụ minh họa**:
- **CA**: Một hệ thống ngân hàng ưu tiên nhất quán dữ liệu (số dư tài khoản) và sẵn sàng, nhưng nếu mạng bị chia cắt, hệ thống có thể từ chối phản hồi.
- **CP**: HBase đảm bảo dữ liệu nhất quán và chịu lỗi phân vùng, nhưng có thể từ chối phản hồi nếu không thể đồng bộ.
- **AP**: Cassandra luôn phản hồi, nhưng dữ liệu có thể tạm thời không đồng bộ giữa các node.

### 3.3. Ứng dụng thực tế
- **Hệ thống ngân hàng**: Thường ưu tiên CA để đảm bảo số dư chính xác.
- **Hệ thống thương mại điện tử**: Có thể ưu tiên AP để đảm bảo trải nghiệm người dùng, chấp nhận dữ liệu không đồng bộ tạm thời (e.g., giỏ hàng hiển thị chưa cập nhật ngay).

## 4. ACID vs BASE

### 4.1. ACID (Atomicity, Consistency, Isolation, Durability)
ACID là tập hợp các thuộc tính đảm bảo giao dịch an toàn trong cơ sở dữ liệu quan hệ (RDBMS):

| **Thành phần**    | **Giải thích**                                                                 |
|--------------------|-------------------------------------------------------------------------------|
| **Atomicity**      | Giao dịch là nguyên khối: hoặc tất cả thành công, hoặc tất cả thất bại.        |
| **Consistency**    | Giao dịch giữ dữ liệu hợp lệ, tuân thủ các ràng buộc (khóa ngoại, định dạng).   |
| **Isolation**      | Các giao dịch đồng thời không ảnh hưởng lẫn nhau, đảm bảo kết quả như tuần tự. |
| **Durability**     | Dữ liệu sau khi commit được lưu vĩnh viễn, không mất dù hệ thống lỗi.          |

**Ví dụ**:
```sql
BEGIN TRANSACTION;
UPDATE tai_khoan SET so_du = so_du - 100 WHERE id = 1;
UPDATE tai_khoan SET so_du = so_du + 100 WHERE id = 2;
COMMIT;
```
- Nếu một câu lệnh thất bại, toàn bộ giao dịch sẽ được rollback.

### 4.2. BASE (Basically Available, Soft State, Eventually Consistent)
BASE là mô hình thiết kế cho hệ thống NoSQL, ưu tiên khả năng mở rộng và tính khả dụng:

| **Thành phần**            | **Giải thích**                                                                 |
|---------------------------|-------------------------------------------------------------------------------|
| **Basically Available**    | Hệ thống luôn phản hồi, dù dữ liệu có thể chưa đồng bộ hoàn toàn.              |
| **Soft State**            | Trạng thái dữ liệu có thể thay đổi mà không cần đầu vào mới (do đồng bộ dần).  |
| **Eventually Consistent** | Dữ liệu sẽ đồng bộ giữa các node sau một khoảng thời gian.                     |

**Ví dụ**:
- Trong Cassandra, khi cập nhật tên người dùng, một node có thể phản hồi tên mới ngay lập tức, nhưng các node khác cần thời gian để đồng bộ.

### 4.3. So sánh ACID và BASE
| **Đặc điểm**              | **ACID**                              | **BASE**                              |
|---------------------------|---------------------------------------|---------------------------------------|
| **Tính nhất quán**        | Mạnh (ngay lập tức)                   | Cuối cùng (eventually consistent)     |
| **Khả năng mở rộng**      | Giới hạn                              | Cao, phù hợp với hệ thống phân tán    |
| **Phức tạp**              | Cao hơn (do kiểm soát giao dịch)      | Thấp hơn                              |
| **Ví dụ hệ thống**        | MySQL, PostgreSQL, Oracle             | Cassandra, DynamoDB, Couchbase        |

**Ứng dụng thực tế**:
- **ACID**: Hệ thống ngân hàng, nơi tính chính xác và nhất quán là tối quan trọng.
- **BASE**: Ứng dụng mạng xã hội, nơi tính khả dụng quan trọng hơn nhất quán tức thời (e.g., hiển thị số lượt thích có thể chậm vài giây).

## 5. Ví dụ thực tế và ứng dụng

### 5.1. Ứng dụng OLTP
- **Hệ thống bán hàng**:
  ```sql
  INSERT INTO don_hang (khach_hang_id, san_pham_id, so_luong, ngay_dat)
  VALUES (1, 101, 5, '2025-07-19');
  ```
- **Hệ thống ngân hàng**:
  ```sql
  UPDATE tai_khoan SET so_du = so_du - 100 WHERE id = 1;
  ```

### 5.2. Ứng dụng OLAP
- **Phân tích doanh thu**:
  ```sql
  SELECT dim_san_pham.danh_muc, SUM(fact_doanh_thu.doanh_thu) AS tong_doanh_thu
  FROM fact_doanh_thu
  JOIN dim_san_pham ON fact_doanh_thu.san_pham_id = dim_san_pham.san_pham_id
  GROUP BY dim_san_pham.danh_muc
  ORDER BY tong_doanh_thu DESC;
  ```

### 5.3. Ứng dụng ETL/ELT
- **ETL với Apache NiFi**:
  - Trích xuất dữ liệu từ API REST, làm sạch dữ liệu (loại bỏ giá trị null), tải vào Snowflake.
- **ELT với Snowflake**:
  - Tải dữ liệu thô từ Kafka vào Snowflake, sau đó sử dụng SQL để làm sạch và tổng hợp.

### 5.4. Ứng dụng CAP
- **Hệ thống AP (Cassandra)**: Đảm bảo người dùng luôn thấy nội dung trang web, dù dữ liệu có thể chưa đồng bộ hoàn toàn giữa các node.
- **Hệ thống CP (HBase)**: Đảm bảo dữ liệu giao dịch ngân hàng luôn nhất quán, có thể từ chối phản hồi nếu node không đồng bộ.

# Tuần 3 – Big Data & Batch Processing

## 1. Apache Spark

### 1.1. Apache Spark là gì?
Apache Spark là một nền tảng tính toán phân tán mã nguồn mở, được thiết kế để xử lý dữ liệu lớn theo hai phương thức chính: **batch processing** (xử lý hàng loạt) và **streaming** (xử lý luồng). Spark nổi bật nhờ khả năng xử lý dữ liệu trong bộ nhớ (in-memory), giúp tăng tốc độ xử lý lên đến 100 lần so với Hadoop MapReduce trong một số trường hợp. Nó được sử dụng rộng rãi trong các ứng dụng như phân tích dữ liệu, học máy, xử lý đồ thị, và xử lý luồng thời gian thực.

<div align="center">

  <img src="https://tse2.mm.bing.net/th/id/OIP.CRlTR3rsel1ykr3Jb6CVQgHaD7?rs=1&pid=ImgDetMain&o=7&rm=3" alt="Apache Spark Overview" width="60%">

  <p><em>Hình 1: Logo Apache Spark, một hình ảnh biểu tượng màu đen với chữ "Spark" được cách điệu, đại diện cho nền tảng xử lý dữ liệu lớn mạnh mẽ.</em></p>

</div>

**Đặc điểm chính**:
- **In-Memory Computing**: Lưu trữ và xử lý dữ liệu trong RAM, chỉ ghi ra đĩa khi cần thiết, giảm đáng kể thời gian xử lý.
- **Lazy Evaluation**: Các phép biến đổi (transformations) được lưu trữ dưới dạng kế hoạch và chỉ thực thi khi có hành động (action), giúp tối ưu hóa hiệu suất.
- **Multi-language Support**: Hỗ trợ nhiều ngôn ngữ lập trình như Python (PySpark), Scala, Java, và R, với các API như RDD, DataFrame, và SQL.
- **Khả năng mở rộng**: Hoạt động trên cụm máy tính với hàng ngàn node, xử lý khối lượng dữ liệu từ gigabyte đến petabyte.
- **Tích hợp đa dạng**: Tích hợp với các hệ thống lưu trữ như HDFS, S3, Cassandra, và các công cụ như Hive, Kafka.

### 1.2. Các thành phần chính của Apache Spark
Apache Spark cung cấp một hệ sinh thái mạnh mẽ với các module chính, mỗi module phục vụ một mục đích cụ thể trong xử lý dữ liệu:

<div align="center">

  <img src="https://techvccloud.mediacdn.vn/280518386289090560/2021/7/26/apache-spark-3-16272746922751044587352.jpg" alt="Spark Components" width="70%">

  <p><em>Hình 2: Một sơ đồ minh họa các thành phần của Apache Spark, bao gồm Spark Core (lõi), Spark SQL (xử lý SQL), Spark Streaming (xử lý luồng), MLlib (học máy), và GraphX (xử lý đồ thị), được sắp xếp theo tầng lớp để thể hiện mối quan hệ giữa chúng.</em></p>

</div>


1. **Spark Core**: Lõi của Spark, cung cấp các chức năng cơ bản như quản lý bộ nhớ, lập lịch task, xử lý lỗi, và tương tác với các hệ thống lưu trữ (HDFS, S3, v.v.).
2. **Spark SQL**: Hỗ trợ xử lý dữ liệu có cấu trúc và bán cấu trúc thông qua SQL, DataFrame, và Dataset API. Tích hợp với các nguồn dữ liệu như Hive, JSON, Parquet.
3. **Spark Streaming**: Cho phép xử lý dữ liệu thời gian thực bằng mô hình micro-batch, phù hợp với các ứng dụng như giám sát hệ thống hoặc phân tích luồng dữ liệu.
4. **MLlib**: Thư viện học máy tích hợp, cung cấp các thuật toán như phân loại, hồi quy, cụm hóa, và học sâu.
5. **GraphX**: Thư viện xử lý đồ thị, hỗ trợ các ứng dụng như phân tích mạng xã hội, đồ thị quan hệ, hoặc tìm kiếm đường đi ngắn nhất.

### 1.3. Kiến trúc vật lý của Spark
Spark hoạt động theo mô hình **Master-Slave**, với các thành phần chính phối hợp để thực hiện tính toán phân tán:

<div align="center">

  <img src="https://spark.apache.org/docs/latest/img/cluster-overview.png" alt="Spark Physical Architecture" width="70%">

  <p><em>Hình 3: Một sơ đồ kiến trúc vật lý của Spark, cho thấy Driver Program trên Master Node (trung tâm điều phối), Cluster Manager (quản lý tài nguyên), và các Executor trên Worker Nodes (thực thi task), được kết nối qua các đường nét thể hiện luồng dữ liệu và lệnh.</em></p>

</div>


- **Driver Program** (chạy trên Master Node):
  - Là chương trình chính, chịu trách nhiệm điều phối toàn bộ ứng dụng Spark.
  - Tạo **SparkContext**, giao tiếp với Cluster Manager để phân phối công việc và quản lý tài nguyên.
  - Lập lịch và giám sát các task trong cụm, thu thập kết quả từ các Executor.
- **Cluster Manager**:
  - Quản lý tài nguyên (CPU, RAM) trên các node trong cụm.
  - Phân phối **Executor** đến các Worker Node để thực hiện các task.
  - Hỗ trợ nhiều loại Cluster Manager như YARN, Mesos, hoặc Spark Standalone.
- **Executor** (chạy trên Worker Nodes):
  - Là các tiến trình chạy trên Worker Node, thực thi các task được giao từ Driver.
  - Mỗi Executor xử lý một hoặc nhiều phân vùng (partition) của dữ liệu.
  - Lưu trữ dữ liệu trong bộ nhớ hoặc đĩa, thực hiện tính toán song song, và trả kết quả về Driver.

**Quy trình hoạt động**:
1. Driver Program phân vùng dữ liệu và gửi các phân vùng đến các Executor.
2. Mỗi Executor thực hiện tính toán trên phân vùng dữ liệu được giao, tận dụng tính song song.
3. Executor trả kết quả về Driver Program, hoặc lưu kết quả vào hệ thống lưu trữ theo yêu cầu.

### 1.4. Kiến trúc logic
Kiến trúc logic của Spark mô tả cách mã người dùng được xử lý và tối ưu hóa trước khi thực thi:

```plaintext
+---------------------------+
|        User Layer         |
|  (RDD / DataFrame / SQL)  |
+---------------------------+
              |
              v
+---------------------------+
|    Catalyst Optimizer     |
| (Logical Plan + Optimization) |
+---------------------------+
              |
              v
+---------------------------+
|       Physical Plan       |
| (Optimized Execution Plan)|
+---------------------------+
              |
              v
+---------------------------+
|        DAG Scheduler      |
| (Build DAG, split Stages) |
+---------------------------+
              |
              v
+---------------------------+
|       Task Scheduler      |
| (Distribute tasks to      |
|  Executors efficiently)   |
+---------------------------+
```
*Hình 4: Một sơ đồ kiến trúc logic của Spark, thể hiện các lớp từ User Layer (mã người dùng), qua Catalyst Optimizer (tối ưu hóa logic), Physical Plan (kế hoạch vật lý), DAG Scheduler (lập lịch đồ thị), đến Task Scheduler (phân phối task), với các mũi tên cho thấy luồng xử lý.*

1. **User Layer**:
   - Người dùng viết mã bằng RDD API, DataFrame API, hoặc SQL API thông qua các ngôn ngữ như Python, Scala, Java, hoặc R.
   - Ví dụ: Lọc dữ liệu trong DataFrame hoặc thực hiện truy vấn SQL.
2. **Catalyst Optimizer**:
   - Chuyển mã người dùng thành **Logical Plan**, biểu diễn các phép toán dưới dạng cây logic.
   - Tối ưu hóa Logical Plan bằng cách sắp xếp lại các phép toán, loại bỏ thao tác dư thừa, hoặc áp dụng dự đoán điều kiện (predicate pushdown).
3. **Physical Plan**:
   - Catalyst Optimizer sinh ra nhiều Physical Plan từ Logical Plan.
   - Chọn plan tối ưu nhất dựa trên chi phí tính toán, sử dụng thống kê về dữ liệu và tài nguyên.
4. **DAG Scheduler**:
   - Biến Physical Plan thành **Directed Acyclic Graph (DAG)**.
   - Chia DAG thành các **Stage**, mỗi Stage là tập hợp các task có thể thực thi song song.
5. **Task Scheduler**:
   - Phân phối các task từ mỗi Stage đến các Executor để xử lý song song.
   - Đảm bảo dữ liệu được xử lý cục bộ (data locality) để giảm chi phí truyền dữ liệu qua mạng.

### 1.5. RDD (Resilient Distributed Dataset)
- **Định nghĩa**: RDD là tập dữ liệu phân tán, bất biến, chịu lỗi, được chia thành các phân vùng (partition) để xử lý song song trên nhiều node trong cụm. RDD là nền tảng cốt lõi của Spark, cung cấp khả năng xử lý dữ liệu phân tán mạnh mẽ.

```plaintext
              +----------------------------+
              |        RDD Lineage         |
              | (Transformations: map, etc.)|
              +-------------+--------------+
                            |
                            v
                  +------------------+
                  |     RDD A        |
                  | [Partitioner: Hash] |
                  +--------+---------+
                           |
     +---------------------+----------------------+
     |                     |                      |
     v                     v                      v
+-----------+        +-----------+         +-----------+
| Partition |        | Partition |         | Partition |
|   A1      |        |   A2      |         |   A3      |
+-----------+        +-----------+         +-----------+
     |                   |                      |
     v                   v                      v
 Worker Node 1     Worker Node 2          Worker Node 3
 (Preferred Loc.)  (Preferred Loc.)       (Preferred Loc.)

      ⋮                   ⋮                      ⋮

             ===> Compute Function (e.g., map)

                            |
                            v
                  +------------------+
                  |     RDD B        |
                  | [Derived from A] |
                  +--------+---------+
                           |
           (Lineage tracked for fault recovery)
```
*Hình 5: Một sơ đồ minh họa cấu trúc RDD, thể hiện các phân vùng (partitions) được phân bố trên các node, cùng với thông tin dòng dõi (lineage) để tái tạo dữ liệu khi cần.*

**Đặc điểm chính của RDD**:
1. **Phân vùng (Partitions)**: Dữ liệu được chia thành các khối nhỏ (partition), mỗi partition được xử lý trên một node riêng biệt.
2. **Phụ thuộc (Dependencies)**: Lưu trữ thông tin dòng dõi (lineage), tức là lịch sử các phép biến đổi từ RDD cha, để tái tạo dữ liệu nếu mất.
3. **Tính toán (Compute Function)**: Áp dụng các hàm **Transformation** (như `map`, `filter`) để tạo RDD mới từ RDD hiện tại.
4. **Vị trí ưu tiên (Preferred Locations)**: Gợi ý nơi lưu trữ partition (ví dụ: trên node nào) để tối ưu hóa xử lý cục bộ, giảm chi phí truyền dữ liệu.
5. **Trình phân vùng (Partitioner)**: Xác định cách chia dữ liệu thành các partition, sử dụng các chiến lược như hash partitioning hoặc range partitioning.

**Tính chất quan trọng**:
- **Immutable (Bất biến)**: RDD không thể sửa đổi; mỗi phép Transformation tạo ra một RDD mới.
- **Lazy Evaluation**: Các Transformation chỉ được ghi lại trong DAG và không thực thi cho đến khi một Action được gọi.
- **Fault Tolerance**: Dòng dõi (lineage) cho phép Spark tái tạo dữ liệu bị mất mà không cần checkpoint, đảm bảo khả năng chịu lỗi.

**Quy trình làm việc với RDD**:
1. **Tạo RDD**: Từ nguồn dữ liệu như file (HDFS, S3), database, hoặc dữ liệu trong bộ nhớ (ví dụ: danh sách Python).
2. **Áp dụng Transformation**: Thực hiện các phép biến đổi như `map`, `filter`, `groupBy` để tạo RDD mới.
3. **Thực hiện Action**: Gọi các hành động như `collect`, `count`, `saveAsTextFile` để lấy kết quả hoặc lưu dữ liệu.

**Ví dụ**:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDDExample").getOrCreate()
# Tạo RDD từ danh sách
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
# Transformation: Lọc số chẵn
even_rdd = rdd.filter(lambda x: x % 2 == 0)
# Action: Thu thập kết quả
result = even_rdd.collect()
print(result)  # Output: [2, 4]
```

### 1.6. DataFrame và Dataset
- **DataFrame**:
  - Biểu diễn dữ liệu dưới dạng bảng với các cột được đặt tên, tương tự bảng trong cơ sở dữ liệu SQL.
  - Tận dụng Catalyst Optimizer để tối ưu hóa truy vấn, giúp truy vấn nhanh hơn so với RDD.
  - Phù hợp với Python và R, nơi kiểm soát kiểu dữ liệu không nghiêm ngặt.
- **Dataset**:
  - Kết hợp ưu điểm của RDD (an toàn kiểu) và DataFrame (tối ưu hóa truy vấn).
  - Chủ yếu dùng trong Scala và Java, cung cấp kiểm tra kiểu tại thời điểm biên dịch.
  - Hỗ trợ xử lý dữ liệu phức tạp với cấu trúc mạnh mẽ hơn.

<p align="center">
  <img src="https://sdmntpreastus2.oaiusercontent.com/files/00000000-d4f8-61f6-8198-225ad8f6af28/raw?se=2025-07-19T13%3A45%3A22Z&sp=r&sv=2024-08-04&sr=b&scid=5faa18db-b0e2-5925-81a1-67bec8166704&skoid=5c72dd08-68ae-4091-b4e1-40ccec0693ae&sktid=a48cca56-e6da-484e-a814-9c849652bcb3&skt=2025-07-19T02%3A22%3A38Z&ske=2025-07-20T02%3A22%3A38Z&sks=b&skv=2024-08-04&sig=kxMdXiObLNc49G1HIu5RHCjbZEXAwPtAdvJMwOaxRbw%3D" width="500"/>
  <br>
  <em>Hình 6: Một sơ đồ so sánh DataFrame và RDD, thể hiện DataFrame với cấu trúc bảng có cột và hàng, trong khi RDD là tập hợp các đối tượng phân tán, với chú thích về hiệu suất và tính linh hoạt.</em>
</p>


**Ví dụ DataFrame**:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()
# Tạo DataFrame từ danh sách
data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()
# +-----+---+
# | name|age|
# +-----+---+
# |Alice| 25|
# |  Bob| 30|
# +-----+---+
```

**So sánh DataFrame và Dataset**:
- **DataFrame**: Dễ sử dụng, phù hợp cho dữ liệu có cấu trúc, không yêu cầu kiểm tra kiểu nghiêm ngặt.
- **Dataset**: An toàn kiểu, phù hợp cho các ứng dụng cần kiểm tra lỗi tại thời điểm biên dịch, nhưng ít phổ biến trong Python.

### 1.7. Lazy Evaluation
- **Định nghĩa**: Spark sử dụng cơ chế **Lazy Evaluation**, trong đó các **Transformation** (như `filter`, `map`) không được thực thi ngay lập tức mà được lưu trữ dưới dạng **Directed Acyclic Graph (DAG)**.
- **Cách hoạt động**:
  - Khi người dùng gọi Transformation, Spark chỉ ghi lại thao tác vào DAG mà không thực hiện tính toán.
  - Khi một **Action** (như `collect`, `show`, `write`) được gọi, Spark sẽ thực thi toàn bộ chuỗi Transformation trong DAG theo thứ tự tối ưu.
- **Lợi ích**:
  - **Tối ưu hóa hiệu suất**: Catalyst Optimizer có thể sắp xếp lại các thao tác, loại bỏ thao tác dư thừa, hoặc đẩy điều kiện lọc xuống nguồn dữ liệu (predicate pushdown).
  - **Giảm chi phí I/O**: Giảm số lần đọc/ghi dữ liệu bằng cách thực thi các thao tác một cách thông minh.
- **Ví dụ**:
  - Trong ví dụ RDD ở trên, `rdd.filter(lambda x: x % 2 == 0)` không thực thi ngay, chỉ khi `collect()` được gọi thì Spark mới thực hiện lọc và thu thập kết quả.

## 2. Hadoop HDFS & Columnar Storage Formats (ORC, Parquet)

### 2.1. Hadoop HDFS
- **Định nghĩa**: Hadoop Distributed File System (HDFS) là hệ thống file phân tán, được thiết kế để lưu trữ dữ liệu lớn trên nhiều node với khả năng chịu lỗi cao và hiệu suất đọc/ghi tuần tự tốt. HDFS là một phần cốt lõi của hệ sinh thái Hadoop, thường được sử dụng cùng Spark hoặc MapReduce.

![HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/images/hdfsarchitecture.png)
*Hình 7: Một sơ đồ kiến trúc HDFS, thể hiện NameNode (trung tâm quản lý metadata) kết nối với nhiều DataNode (lưu trữ dữ liệu), với các đường nét thể hiện luồng dữ liệu và lệnh giữa các thành phần.*

**Đặc điểm**:
- Lưu trữ dữ liệu từ gigabyte đến petabyte, phù hợp với Big Data.
- Chịu lỗi thông qua sao chép dữ liệu (replication) trên nhiều node, mặc định là 3 bản sao.
- Tối ưu cho đọc/ghi tuần tự, không phù hợp cho đọc/ghi ngẫu nhiên.
- Hỗ trợ mở rộng dễ dàng bằng cách thêm node vào cụm.

**Kiến trúc HDFS**:
- **NameNode (Master)**:
  - Quản lý **namespace** và metadata của hệ thống file, bao gồm thông tin về vị trí các block dữ liệu.
  - Lưu trữ **Filesystem Tree** (cấu trúc thư mục) và **edit log** (nhật ký thao tác) trên đĩa.
  - Xử lý các thao tác như mở, đóng, đổi tên file/thư mục.
  - Giao tiếp với client để cung cấp thông tin về vị trí block.
- **DataNode (Worker)**:
  - Lưu trữ dữ liệu thực tế dưới dạng các block (mặc định 64MB hoặc 128MB, có thể cấu hình).
  - Xử lý yêu cầu đọc/ghi từ client.
  - Thực hiện tạo, xóa, sao chép block theo lệnh từ NameNode.
  - Định kỳ báo cáo trạng thái block về NameNode.

**Quy trình hoạt động**:
1. Client gửi yêu cầu đọc/ghi đến NameNode.
2. NameNode trả về metadata, bao gồm vị trí các block trên DataNode.
3. Client giao tiếp trực tiếp với DataNode để đọc/ghi dữ liệu.
4. DataNode báo cáo trạng thái block về NameNode để đảm bảo tính nhất quán.

![HDFS Blocks](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/images/hdfsdatanodes.png)
*Hình 8: Một sơ đồ minh họa cấu trúc block trong HDFS, thể hiện cách một file được chia thành các block (màu khác nhau) và sao chép trên nhiều DataNode để đảm bảo chịu lỗi.*

### 2.2. ORC (Optimized Row Columnar)
- **Định nghĩa**: ORC là định dạng lưu trữ dạng cột, được tối ưu hóa cho việc nén và xử lý song song trong hệ sinh thái Hadoop, đặc biệt với Apache Hive. ORC được thiết kế để cải thiện hiệu suất truy vấn và tiết kiệm không gian lưu trữ.

<p align="center">
  <img src="https://tse3.mm.bing.net/th/id/OIP.yP3oKuvpO9o3ylgkYFaFkgHaHa?rs=1&pid=ImgDetMain&o=7&rm=3" width="400">
  <br>
  <em>Hình 9: Một sơ đồ cấu trúc file ORC, thể hiện các stripe (dải dữ liệu) chứa index (chỉ mục), row data (dữ liệu hàng), và footer (thông tin thống kê), với các mũi tên chỉ luồng tổ chức dữ liệu.</em>
</p>


**Đặc điểm**:
- Dữ liệu được tổ chức thành các **stripe** (kích thước mặc định 250MB), mỗi stripe bao gồm:
  - **Index**: Lưu trữ thông tin chỉ mục để hỗ trợ truy vấn nhanh và bỏ qua dữ liệu không cần thiết.
  - **Row Data**: Dữ liệu thực tế được lưu trữ theo cột, tối ưu hóa cho nén và truy vấn.
  - **Footer**: Chứa thống kê (min, max, count) và metadata cho mỗi cột.
- Hỗ trợ giao dịch **ACID** trong Apache Hive, cho phép cập nhật và xóa dữ liệu.
- Tối ưu cho các tác vụ ghi dữ liệu chuyên sâu và thay đổi schema thường xuyên.
- Hỗ trợ nén dữ liệu hiệu quả (ví dụ: Zlib, Snappy) và các kỹ thuật như predicate pushdown.

**Lợi ích**:
- Giảm không gian lưu trữ nhờ nén cột.
- Tăng tốc độ truy vấn bằng cách chỉ đọc các cột cần thiết.
- Hỗ trợ các thống kê cột (column statistics) để tối ưu hóa truy vấn.

### 2.3. Parquet
- **Định nghĩa**: Parquet là định dạng lưu trữ dạng cột, tối ưu hóa cho truy vấn dữ liệu lớn và xử lý dữ liệu phức tạp. Parquet được sử dụng rộng rãi trong Spark, Hive, Impala, và các công cụ khác trong hệ sinh thái Hadoop.

![Parquet Structure](https://www.upsolver.com/wp-content/uploads/2020/05/Screen-Shot-2020-05-26-at-17.53.13.png)


**Đặc điểm**:
- Tệp Parquet gồm:
  - **Row Groups**: Nhóm các hàng được lưu trữ cùng nhau, mỗi nhóm chứa dữ liệu cột (column chunks).
  - **Header**: Đánh dấu bắt đầu của file.
  - **Footer**: Lưu metadata về cấu trúc file và thống kê cột (min, max, count).
- Hỗ trợ nén cột và mã hóa hiệu quả (ví dụ: Snappy, Gzip).
- Tối ưu cho dữ liệu lồng nhau (nested data structures), như JSON hoặc Avro.
- Hỗ trợ **schema evolution**, cho phép thay đổi schema mà không phá hủy dữ liệu cũ.

**Tính năng nổi bật**:
- Tối ưu cho các tác vụ truy vấn, đặc biệt khi chỉ cần đọc một số cột nhất định.
- Hỗ trợ các kỹ thuật tối ưu hóa truy vấn như predicate pushdown và projection pushdown.
- Có khả năng mở rộng để hỗ trợ các cơ chế mã hóa trong tương lai.

### 2.4. So sánh ORC và Parquet
| **Đặc điểm**              | **ORC**                                  | **Parquet**                              |
|---------------------------|------------------------------------------|------------------------------------------|
| **Hỗ trợ giao dịch**      | Hỗ trợ ACID trong Hive                   | Không hỗ trợ ACID                        |
| **Tối ưu cho**            | Ghi dữ liệu, thay đổi schema             | Truy vấn dữ liệu, cấu trúc lồng nhau     |
| **Nén và mã hóa**         | Tốt, tập trung vào Hive                  | Tốt, hỗ trợ nhiều hệ thống               |
| **Hỗ trợ hệ sinh thái**   | Chủ yếu trong Hive                       | Rộng rãi (Spark, Hive, Impala, etc.)     |
| **Hiệu suất**             | Tốt hơn cho Hive                         | Tốt hơn cho Spark                        |

**Lựa chọn sử dụng**:
- **ORC**: Phù hợp cho các hệ thống tập trung vào Hive, cần giao dịch ACID hoặc ghi dữ liệu chuyên sâu.
- **Parquet**: Lý tưởng cho các ứng dụng Spark, cần truy vấn nhanh trên dữ liệu lồng nhau hoặc phân tích phức tạp.

## 3. Hadoop Ecosystem

### 3.1. Hadoop Framework
Hadoop là một framework mã nguồn mở, được viết bằng Java, dùng để xử lý dữ liệu lớn (Big Data) phân tán trên các cụm máy tính. Nó cho phép lưu trữ và xử lý dữ liệu từ gigabyte đến petabyte, sử dụng mô hình MapReduce để xử lý song song trên nhiều máy tính.

![Hadoop Architecture](https://hadoop.apache.org/images/hadoop-logo.jpg)  
*Hình 11: Logo Hadoop, một hình ảnh gồm chữ "Hadoop" màu xanh lá cây với nền trắng, đại diện cho hệ sinh thái xử lý dữ liệu lớn.*


**Kiến trúc của Hadoop gồm 3 lớp chính**:
1. **HDFS (Hadoop Distributed File System)**: Hệ thống lưu trữ phân tán, cung cấp khả năng lưu trữ dữ liệu lớn với độ tin cậy cao.
2. **MapReduce**: Framework xử lý dữ liệu song song, chia nhỏ công việc thành các task Map và Reduce.
3. **YARN (Yet-Another-Resource-Negotiator)**: Quản lý tài nguyên và lập lịch cho các ứng dụng phân tán.

### 3.2. MapReduce
MapReduce là một framework xử lý song song, cho phép xử lý khối lượng dữ liệu lớn trên các cụm máy tính với khả năng chịu lỗi cao.

![MapReduce Process](https://storage.googleapis.com/algodailyrandomassets/curriculum/systems-design/map-reduce/example.png)
*Hình 12: Một sơ đồ quy trình MapReduce, thể hiện giai đoạn Map (phân tích dữ liệu đầu vào thành cặp key/value) và Reduce (tổng hợp kết quả), với các task được phân bố trên nhiều node.*

**MapReduce thực hiện 2 chức năng chính**:
- **Map**: Tải, phân tích dữ liệu đầu vào và chuyển đổi thành các cặp key/value.
  - Ví dụ: Đếm số lần xuất hiện của mỗi từ trong một tài liệu.
- **Reduce**: Kết hợp các cặp key/value từ Map để tạo ra kết quả cuối cùng.
  - Ví dụ: Tổng hợp số lần xuất hiện của mỗi từ.

**Quy trình hoạt động**:
1. Dữ liệu đầu vào được chia thành các block nhỏ.
2. Các task Map xử lý song song trên các block, tạo ra các cặp key/value trung gian.
3. Các cặp key/value được nhóm lại và gửi đến các task Reduce.
4. Task Reduce tổng hợp kết quả và lưu vào hệ thống lưu trữ (HDFS, S3).

### 3.3. YARN
YARN (Yet-Another-Resource-Negotiator) là framework quản lý tài nguyên và lập lịch trong Hadoop, cho phép chạy nhiều loại ứng dụng phân tán (MapReduce, Spark, v.v.).

<div align="center">

  <img src="https://raw.githubusercontent.com/minhnguyen2804/Bao-Cao-Thuc-Tap-Nguyen-Ngoc-Minh/refs/heads/main/image/yarn.png" alt="YARN Architecture" width="600">

  <p><em>Hình 13: Một sơ đồ kiến trúc YARN, thể hiện ResourceManager (quản lý toàn cục), NodeManager (quản lý node), và ApplicationMaster (quản lý ứng dụng), với các đường nối thể hiện luồng điều phối.</em></p>

</div>


**Thành phần chính**:
- **ResourceManager**: 
  - Quản lý toàn bộ tài nguyên tính toán của cụm (CPU, RAM, disk, network).
  - Gồm hai thành phần: **Scheduler** (lập lịch tài nguyên) và **ApplicationManager** (quản lý ứng dụng).
- **NodeManager**:
  - Chạy trên mỗi node, giám sát việc sử dụng tài nguyên trong các container.
  - Báo cáo trạng thái tài nguyên và tiến trình thực thi về ResourceManager.
- **ApplicationMaster**:
  - Quản lý từng ứng dụng (ví dụ: một job Spark hoặc MapReduce).
  - Yêu cầu tài nguyên từ ResourceManager và phối hợp với NodeManager để thực thi task.

**Lợi ích của YARN**:
- Tách biệt quản lý tài nguyên và lập lịch, cải thiện khả năng mở rộng.
- Hỗ trợ nhiều framework (Spark, MapReduce, Flink) trên cùng một cụm.
- Tăng hiệu quả sử dụng tài nguyên và khả năng chịu lỗi.

## 4. Batch Processing

### 4.1. Định nghĩa
Batch Processing là phương pháp xử lý dữ liệu theo lô, trong đó dữ liệu được thu thập, lưu trữ, và xử lý định kỳ (theo giờ, ngày, tuần, tháng, hoặc năm). Phương pháp này phù hợp cho các tác vụ phân tích dữ liệu lớn không yêu cầu thời gian thực, như báo cáo doanh thu, ETL pipeline, hoặc phân tích lịch sử.

### 4.2. Kiến trúc Batch Processing
- **Flow Architect**:
  - **Data Collection**: Dữ liệu được thu thập từ các nguồn như database (MySQL, PostgreSQL), file (CSV, JSON), hoặc API.
  - **Temporary Storage**: Dữ liệu thô được lưu trữ tạm thời trong hệ thống lưu trữ như HDFS, S3, hoặc message queue như Kafka.
  - **Scheduled Processing**: Theo lịch trình định kỳ (cron job), dữ liệu được xử lý bằng các công cụ như Spark, Hadoop MapReduce, hoặc Hive.
  - **Output Storage**: Kết quả được lưu vào Data Warehouse (Snowflake, BigQuery) hoặc hệ thống lưu trữ khác.

![Batch Processing Flow](https://vectormine.b-cdn.net/wp-content/uploads/batch_processing_outline_diagram-1.jpg)
*Hình 14: Một sơ đồ quy trình Batch Processing, thể hiện các giai đoạn từ thu thập dữ liệu (data collection), lưu trữ tạm thời (staging), xử lý định kỳ (processing), đến lưu kết quả vào kho dữ liệu (data warehouse), với các mũi tên chỉ luồng dữ liệu.*

**Ví dụ quy trình**:
1. Thu thập dữ liệu giao dịch từ API bán hàng mỗi ngày.
2. Lưu dữ liệu vào HDFS dưới dạng file Parquet.
3. Sử dụng Spark để tổng hợp doanh thu theo sản phẩm mỗi tuần.
4. Lưu kết quả vào Snowflake để tạo báo cáo kinh doanh.

### 4.3. Ưu điểm của Batch Processing
1. **Hiệu quả**: Xử lý lượng lớn dữ liệu cùng lúc, tiết kiệm tài nguyên hệ thống so với xử lý từng bản ghi.
2. **Đơn giản hóa**: Tự động hóa quy trình theo lịch trình, giảm can thiệp thủ công, thường sử dụng công cụ như Apache Airflow.
3. **Tính nhất quán**: Đảm bảo dữ liệu được xử lý đồng bộ và nhất quán, đặc biệt khi tổng hợp dữ liệu từ nhiều nguồn.
4. **Phù hợp với Big Data**: Hỗ trợ xử lý khối lượng dữ liệu lớn trong các tác vụ như ETL, báo cáo, và phân tích.

### 4.4. Nhược điểm
- **Độ trễ cao**: Kết quả chỉ có sẵn sau khi xử lý lô hoàn tất, không phù hợp cho các ứng dụng thời gian thực.
- **Tài nguyên lớn**: Yêu cầu lưu trữ tạm thời và khả năng tính toán mạnh mẽ, đặc biệt khi xử lý dữ liệu lớn.
- **Phức tạp trong quản lý lỗi**: Nếu lô xử lý thất bại, có thể cần chạy lại toàn bộ lô, gây tốn thời gian.
# Tuần 4 – Apache Kafka và Xử lý Dữ liệu Thời gian thực

## 1. Apache Kafka

### 1.1. Apache Kafka là gì?
Apache Kafka là một hệ thống xử lý dữ liệu thời gian thực mã nguồn mở, được tạo ra tại LinkedIn và sau đó được phát triển bởi Apache Software Foundation. Nó đã trở thành một phần quan trọng của cơ sở hạ tầng cho các ứng dụng xử lý dữ liệu lớn và phân tán.

Kafka ra đời để giải quyết các thách thức liên quan đến:
- Xử lý dữ liệu thời gian thực.
- Lưu trữ log (message log) hiệu quả.
- Chia sẻ dữ liệu giữa các ứng dụng trong môi trường phân tán và có khả năng mở rộng.

### 1.2. Ưu điểm của Apache Kafka
- **Khả năng chịu tải cao**: Có thể mở rộng quy mô xử lý bằng cách thêm broker (máy chủ) vào cluster.
- **Đảm bảo tính nhất quán**: Sử dụng mô hình log-based để đảm bảo thứ tự và không mất dữ liệu.
- **Độ tin cậy cao**: Dữ liệu vẫn truy cập được khi một số broker gặp sự cố nhờ cơ chế sao chép (replication).
- **Xử lý sự kiện thời gian thực**: Giúp ứng dụng phản ứng nhanh với các sự kiện quan trọng.

### 1.3. Nhược điểm của Apache Kafka
- **Phức tạp trong triển khai và quản lý**: Yêu cầu cấu hình chi tiết và kinh nghiệm vận hành.
- **Yêu cầu nhiều tài nguyên hệ thống**: Tiêu tốn CPU, RAM, và dung lượng đĩa.
- **Khó quản lý dữ liệu cũ**: Dữ liệu lưu trữ lâu dài có thể gây khó khăn trong việc duy trì.
- **Dễ bị quá tải**: Nếu lưu trữ quá nhiều sự kiện không cần thiết, hệ thống có thể bị chậm.

### 1.4. Kiến trúc của Apache Kafka
Kiến trúc cơ bản của Kafka bao gồm các thành phần chính:

- **Cluster**: Tập hợp nhiều máy chủ (broker) làm việc cùng nhau để cung cấp tính mở rộng, nhất quán và độ tin cậy.
- **Broker**: Máy chủ chịu trách nhiệm xử lý và lưu trữ dữ liệu Kafka, quản lý các partition thuộc các topic.
- **Topic**: Dữ liệu được phân loại thành các chủ đề (topic), mỗi topic là một luồng dữ liệu độc lập. Producer gửi dữ liệu đến topic, Consumer đọc dữ liệu từ topic.
- **Partition**: Mỗi topic có thể chia thành nhiều partition để phân tán dữ liệu và tăng hiệu suất. Dữ liệu được ghi và đọc theo thứ tự trong mỗi partition, với mỗi partition được lưu trên một broker.
- **Producer**: Thành phần gửi dữ liệu tới các topic trong Kafka.
- **Consumer**: Thành phần đọc dữ liệu từ topic Kafka. Kafka đảm bảo mỗi bản ghi chỉ được đọc bởi một consumer duy nhất trong cùng một group.
- **ZooKeeper**: Quản lý trạng thái các broker trong cluster Kafka, đảm bảo hoạt động ổn định và nhất quán.

![Kiến trúc Apache Kafka](https://khoinda.io.vn/assets/images/kafka_architecture.drawio-29303c3c702118a824930f2fd50ba4a1.svg)
*Hình 1: Sơ đồ kiến trúc Kafka, thể hiện Cluster với các Broker, Topic, Partition, Producer, Consumer, và ZooKeeper, với các mũi tên chỉ luồng dữ liệu.*

### 1.5. Apache Kafka trong hệ thống
Kafka hoạt động theo mô hình **publish-subscribe (Pub-Sub)** giống hệ thống message queue:
- Dữ liệu được phân chia thành **Topic**, mỗi topic đại diện cho một loại dữ liệu cụ thể.
- Mỗi topic được chia thành các **Partition**, chứa phần dữ liệu và được lưu trên các **Broker**. Mỗi bản ghi trong partition có một **offset** duy nhất để theo dõi vị trí.
- **Producer** gửi bản ghi dữ liệu tới topic cụ thể.
- **Consumer** đăng ký theo dõi topic và nhận dữ liệu, sử dụng offset để theo dõi tiến độ đọc.
- Kafka hỗ trợ sao lưu dữ liệu trên nhiều broker để đảm bảo tính sẵn sàng và bảo mật.

### 1.6. Tính năng mở rộng và sao lưu
- Kafka sao lưu dữ liệu trên nhiều broker thông qua cơ chế replication.
- Mỗi partition có thể có nhiều bản sao (replicas) được lưu trên các broker khác nhau, tăng độ tin cậy và khả năng khôi phục khi lỗi xảy ra.

### 1.7. Kiến trúc Pub-Sub Messaging với Apache Kafka
Kafka là giải pháp mạnh mẽ cho mô hình Pub-Sub, giúp trao đổi thông tin hiệu quả và đáng tin cậy. Quy trình hoạt động:
- **Kafka Producer** gửi message đến **Topic**.
- **Kafka Broker** lưu message vào các **Partition** được định cấu hình, phân phối cân bằng giữa các partition.
- **Kafka Consumer** subscribe vào topic, nhận offset hiện tại từ ZooKeeper.
- Consumer gửi request pull để lấy message mới, Kafka chuyển tiếp message ngay khi nhận được.
- Sau khi xử lý, Consumer gửi xác nhận, Kafka cập nhật offset. Ngay cả khi broker gặp sự cố, Consumer vẫn đọc được message tiếp theo nhờ ZooKeeper.

![Pub-Sub Messaging với Kafka](https://th.bing.com/th/id/R.ca101db423dda15ba71d093866a0fea0?rik=SLDjlJ29kqC5Jw&pid=ImgRaw&r=0)
*Hình 2: Sơ đồ Pub-Sub, thể hiện luồng từ Producer qua Broker và Partition đến Consumer, với vai trò của ZooKeeper trong quản lý offset.*

## 2. Spark Streaming

### 2.1. Spark Streaming là gì?
Spark Streaming là thành phần quan trọng của Apache Spark, cho phép xử lý dữ liệu trực tiếp và liên tục từ nhiều nguồn như Kafka, Flume, Kinesis, hoặc socket TCP/IP.

### 2.2. Micro-batch Processing
Spark Streaming sử dụng mô hình **micro-batch processing**, chia dòng dữ liệu thành các **micro-batch** và xử lý giống như dữ liệu tĩnh trong Spark.

- **Dữ liệu đầu vào (Input Data Stream)**: Được thu thập từ các nguồn như Kafka, Flume, Kinesis, hoặc TCP socket, đến liên tục theo thời gian thực.
- **Bộ chia micro-batch (Micro-batch Scheduler)**: Chia dữ liệu thành micro-batch theo khoảng thời gian cố định (ví dụ: 1 giây hoặc 5 giây), mỗi batch được biểu diễn bằng một RDD.
- **Xử lý batch (RDD Transformation)**: Áp dụng các thao tác như `map`, `filter`, `reduceByKey`, `join`, hoặc `window` trên DStream.
- **Đầu ra (Output Operations)**: Kết quả được ghi ra console, HDFS, cơ sở dữ liệu, hoặc đẩy vào hệ thống như Kafka, Elasticsearch.

### 2.3. DStream
**DStream (Discretized Stream)** là cấu trúc dữ liệu chính trong Spark Streaming, đại diện cho dòng dữ liệu liên tục.

- Mỗi DStream là chuỗi các **RDDs**, mỗi RDD chứa dữ liệu của một micro-batch tại một thời điểm cụ thể.
- Sơ đồ minh họa:
  ```
  DStream
     │
  ┌────┬────┬────┐
  ▼    ▼    ▼
  RDD(t1) RDD(t2) RDD(t3)
  (micro-batch) (micro-batch) (micro-batch)
  ```
  - `t1`, `t2`, `t3`,... là mốc thời gian (ví dụ: mỗi 1 giây).
  - Các RDD được xử lý tuần tự bằng các phép biến đổi.


## 3. Kiến trúc Lambda

### 3.1. Mục tiêu
Kiến trúc Lambda cân bằng giữa độ trễ, thông lượng và khả năng chịu lỗi, kết hợp:
- **Batch processing** để cung cấp chế độ xem toàn diện và chính xác.
- **Stream processing** để cung cấp dữ liệu thời gian thực.

### 3.2. Thành phần
- **Lớp xử lý dữ liệu batch**:
  - Tính toán trước kết quả sử dụng hệ thống phân tán (như Spark).
  - Đảm bảo độ chính xác cao, sửa lỗi bằng cách tính toán lại toàn bộ dữ liệu.
  - Kết quả lưu ở cơ sở dữ liệu chỉ đọc, thay thế dữ liệu cũ.
- **Lớp xử lý dữ liệu speed (real-time)**:
  - Xử lý dữ liệu thời gian thực, không yêu cầu sửa chữa.
  - Hy sinh thông lượng để giảm độ trễ, dữ liệu có thể không đầy đủ.
  - Được thay thế bởi batch khi hoàn tất.
- **Lớp serving (phục vụ dữ liệu)**:
  - Lưu trữ dữ liệu từ batch và speed.
  - Đáp ứng truy vấn bằng dữ liệu đã xử lý sẵn.

### 3.3. Ưu điểm và ứng dụng
- Phổ biến trong dữ liệu lớn và phân tích thời gian thực.
- Giảm độ trễ so với MapReduce truyền thống.

![Kiến trúc Lambda](https://th.bing.com/th/id/R.e17f3ca1f503cd39f8f1fd0f0c7b2478?rik=uUY%2bO7BUxOkNOw&pid=ImgRaw&r=0)
*Hình 4: Sơ đồ Kiến trúc Lambda, thể hiện luồng từ Batch Layer, Speed Layer, đến Serving Layer.*

## 4. Kiến trúc Kappa

### 4.1. Hạn chế của Kiến trúc Lambda
- Cần duy trì hai mã nguồn riêng biệt (batch và real-time).
- Phức tạp trong đồng bộ logic nghiệp vụ.

### 4.2. Giải pháp: Kiến trúc Kappa
- Được đề xuất bởi Jay Kreps, đồng sáng lập Apache Kafka.
- **Ý tưởng chính**: Lưu toàn bộ dữ liệu nguồn vào Kafka, tái sử dụng logic stream để chạy lại trên dữ liệu cũ, thống nhất mã nguồn.
- **Đặc điểm**: Lưu trữ dữ liệu nhiều năm trong Kafka, phù hợp với hệ thống cần xử lý dữ liệu lịch sử lớn.

### 4.3. So sánh nhanh
| Tiêu chí               | Lambda           | Kappa           |
|------------------------|------------------|-----------------|
| Số lượng hệ thống xử lý | 2 (batch + stream) | 1 (chỉ stream)  |
| Mã nguồn xử lý         | Riêng biệt       | Thống nhất      |
| Phức tạp               | Cao              | Thấp hơn        |
| Hỗ trợ dữ liệu lịch sử | Có (batch)       | Có (Kafka)      |



## 5. Streaming & Real-time Data

### 5.1. Kiến trúc của Kafka
- Dựa trên mô hình **kênh sự kiện (event channel)**, rất phù hợp cho streaming và xử lý thời gian thực.
- **Kiến trúc cơ bản của mô hình kênh sự kiện**:
  - Kênh sự kiện là trung gian, có tác tử theo dõi các thành viên.
  - Các thành phần dịch vụ đăng ký với kênh, gửi yêu cầu, và kênh kết nối chúng để trao đổi.
- Kafka phát triển mô hình này với **bộ đệm (buffer)** để tăng khả năng xử lý song song.

### 5.2. Chi tiết quy trình của Kafka
- Mỗi broker là một server xử lý yêu cầu.
- Khi Producer/Consumer gửi yêu cầu, dữ liệu được lưu trong buffer.
- Cơ chế phân tán cho phép các broker nhảy vào buffer để xử lý, tránh quá tải và tăng dự phòng nóng.

### 5.3. Ưu điểm của Kafka
- **Giảm thao tác đọc/ghi đĩa**: Lưu dữ liệu tạm trong buffer, ghi xuống đĩa khi đầy, giảm truy cập vật lý.
- **Tăng độ tin cậy**: Buffer bảo vệ dữ liệu khi broker lỗi, khôi phục từ replicas.
- **Cân bằng tải**: Phân phối yêu cầu hiệu quả, tránh quá tải broker.
- **Đảm bảo thứ tự**: Giữ nguyên thứ tự message trong partition.

### 5.4. Streaming Processing
- Là mô hình xử lý dữ liệu thời gian thực hoặc gần thời gian thực.
- Dữ liệu được xử lý ngay khi đến, khác với batch phải đợi định kỳ, sau đó lưu vào kho.
- **Ứng dụng**: Chứng khoán, crypto, phân tích hành vi người dùng trực tuyến.
- **Ưu điểm**:
  - Phân tích dữ liệu gần như ngay lập tức, đưa ra quyết định nhanh.
  - Cải thiện phản hồi, giảm độ trễ so với batch processing.
  - Khả năng mở rộng và chịu lỗi cao.

![Streaming Processing](https://raw.githubusercontent.com/minhnguyen2804/Bao-Cao-Thuc-Tap-Nguyen-Ngoc-Minh/refs/heads/main/image/streamprocess.png)
*Hình 6: Sơ đồ Streaming Processing, thể hiện luồng từ nguồn dữ liệu đến xử lý thời gian thực.*

# Tuần 5: Workflow & Integration
# Tuần 6: Product Pipeline
