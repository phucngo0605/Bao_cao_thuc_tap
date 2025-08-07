# Xử lý dữ liệu với Spark Shell

## Khởi chạy Spark Shell
```bash
/data/spark-3.4.3/bin/spark-shell --master yarn --deploy-mode client --num-executors 3 --executor-memory 1G
```

```scala
// Import thư viện cần thiết
import org.apache.spark.sql.functions._

// Lấy dữ liệu từ HDFS
val df = spark.read.parquet("hdfs://adt-platform-dev-106-254:8120/data/Parquet/AdnLog/*")

// Viết toàn bộ chain operations trong một lệnh
val dfFinal = df.withColumn("event_date", from_unixtime(col("time_group.time_create") / 1000, "yyyy-MM-dd"))
  .select("campaignId", "bannerId", "click_or_view", "event_date", "guid")
  .na.drop(Seq("campaignId", "click_or_view", "event_date", "guid"))
  .dropDuplicates(Seq("campaignId", "bannerId", "click_or_view", "event_date", "guid"))

// Sử dụng biến res18 (kết quả cuối cùng của chain operations)
res18.printSchema()

// Xem dữ liệu của res18
res18.show(5)

// Lưu res18 thay vì dfFinal
res18.write.mode("overwrite").option("header", "true")
  .csv("hdfs://adt-platform-dev-106-254:8120/user/phucnq/CampaignId_BannerId_Guid")

println("Data saved successfully!")
:quit
```

## Kiểm tra và tải dữ liệu về local

```bash
# Kiểm tra dữ liệu trên HDFS
hdfs dfs -ls hdfs://adt-platform-dev-106-254:8120/user/phucnq/CampaignId_BannerId_Guid

# Xem nội dung file đầu tiên
hdfs dfs -head hdfs://adt-platform-dev-106-254:8120/user/phucnq/CampaignId_BannerId_Guid/part-00000-*.csv

# Tải dữ liệu về local
hdfs dfs -get hdfs://adt-platform-dev-106-254:8120/user/phucnq/CampaignId_BannerId_Guid

# Kiểm tra dữ liệu local
ls -la CampaignId_BannerId_Guid/
head -5 CampaignId_BannerId_Guid/part-00000-*.csv
:quit
```

## Kết nối HDFS với ClickHouse Cloud

```bash
# Dọn dẹp khẩn cấp
rm -rf /tmp/* 2>/dev/null
sudo rm -rf /tmp/* 2>/dev/null
rm -rf ~/.cache/* 2>/dev/null
rm -rf ~/.local/share/Trash/* 2>/dev/null

# Kiểm tra dung lượng
df -h

# Tạo script đơn giản bằng echo thay vì heredoc
echo '#!/usr/bin/env python3' > load_to_clickhouse.py
echo 'import subprocess' >> load_to_clickhouse.py
echo 'import requests' >> load_to_clickhouse.py
echo 'import json' >> load_to_clickhouse.py
echo '' >> load_to_clickhouse.py
echo 'HOST = "ff88qal6fo.germanywestcentral.azure.clickhouse.cloud"' >> load_to_clickhouse.py
echo 'USER = "default"' >> load_to_clickhouse.py
echo 'PASS = "YTxY6~xEOu~u1"' >> load_to_clickhouse.py
echo '' >> load_to_clickhouse.py
echo 'def query(sql):' >> load_to_clickhouse.py
echo '    url = f"https://{HOST}:8443"' >> load_to_clickhouse.py
echo '    r = requests.post(url, auth=(USER, PASS), data=sql)' >> load_to_clickhouse.py
echo '    if r.status_code == 200:' >> load_to_clickhouse.py
echo '        return r.text' >> load_to_clickhouse.py
echo '    else:' >> load_to_clickhouse.py
echo '        raise Exception(f"Error: {r.status_code} - {r.text}")' >> load_to_clickhouse.py
echo '' >> load_to_clickhouse.py
echo 'def main():' >> load_to_clickhouse.py
echo '    print("Testing connection...")' >> load_to_clickhouse.py
echo '    result = query("SELECT 1")' >> load_to_clickhouse.py
echo '    print("✓ Connected!")' >> load_to_clickhouse.py
echo '    ' >> load_to_clickhouse.py
echo '    print("Creating tables...")' >> load_to_clickhouse.py
echo '    query("""CREATE TABLE IF NOT EXISTS campaign_events (' >> load_to_clickhouse.py
echo '        campaignId UInt64, bannerId UInt64, click_or_view String,' >> load_to_clickhouse.py
echo '        event_date Date, guid UInt64' >> load_to_clickhouse.py
echo '    ) ENGINE = MergeTree() ORDER BY (event_date, campaignId)""")' >> load_to_clickhouse.py
echo '    print("✓ Tables created!")' >> load_to_clickhouse.py
echo '    ' >> load_to_clickhouse.py
echo '    print("Loading sample data...")' >> load_to_clickhouse.py
echo '    cmd = "hdfs dfs -cat hdfs://adt-platform-dev-106-254:8120/user/phucnq/CampaignId_BannerId_Guid/part-00000-*.csv | head -100"' >> load_to_clickhouse.py
echo '    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)' >> load_to_clickhouse.py
echo '    ' >> load_to_clickhouse.py
echo '    if result.returncode == 0:' >> load_to_clickhouse.py
echo '        lines = result.stdout.strip().split("\\n")' >> load_to_clickhouse.py
echo '        values = []' >> load_to_clickhouse.py
echo '        for line in lines[:50]:  # Only 50 records' >> load_to_clickhouse.py
echo '            parts = line.split(",")' >> load_to_clickhouse.py
echo '            if len(parts) >= 5:' >> load_to_clickhouse.py
echo '                try:' >> load_to_clickhouse.py
echo '                    cid = int(parts[0])' >> load_to_clickhouse.py
echo '                    bid = int(parts[1]) if parts[1] else 0' >> load_to_clickhouse.py
echo '                    action = parts[2]' >> load_to_clickhouse.py
echo '                    date = parts[3]' >> load_to_clickhouse.py
echo '                    guid = int(parts[4])' >> load_to_clickhouse.py
echo '                    values.append(f"({cid}, {bid}, '"'"'{action}'"'"', '"'"'{date}'"'"', {guid})")' >> load_to_clickhouse.py
echo '                except:' >> load_to_clickhouse.py
echo '                    continue' >> load_to_clickhouse.py
echo '        ' >> load_to_clickhouse.py
echo '        if values:' >> load_to_clickhouse.py
echo '            insert_sql = "INSERT INTO campaign_events VALUES " + ",".join(values)' >> load_to_clickhouse.py
echo '            query(insert_sql)' >> load_to_clickhouse.py
echo '            print(f"✓ Loaded {len(values)} sample records!")' >> load_to_clickhouse.py
echo '        else:' >> load_to_clickhouse.py
echo '            print("No valid data found")' >> load_to_clickhouse.py
echo '    else:' >> load_to_clickhouse.py
echo '        print(f"HDFS read failed: {result.stderr}")' >> load_to_clickhouse.py
echo '    ' >> load_to_clickhouse.py
echo '    # Verify' >> load_to_clickhouse.py
echo '    count = query("SELECT COUNT(*) FROM campaign_events")' >> load_to_clickhouse.py
echo '    print(f"Total records: {count.strip()}")' >> load_to_clickhouse.py
echo '' >> load_to_clickhouse.py
echo 'if __name__ == "__main__":' >> load_to_clickhouse.py
echo '    main()' >> load_to_clickhouse.py

chmod +x load_to_clickhouse.py

# Cài đặt requests đơn giản
python3 -c "import requests" 2>/dev/null || {
    echo "Installing requests..."
    python3 -m pip install --no-cache-dir --user requests
}

# Chạy script
python3 load_to_clickhouse.py
```

## Load dữ liệu từ HDFS vào ClickHouse Cloud

```bash
echo "=== Loading ALL data from HDFS to ClickHouse Cloud ==="

# Bước 1: Sửa schema để hỗ trợ GUID âm
echo "Step 1: Updating schema for negative GUID..."

curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
     --data-binary "DROP TABLE IF EXISTS campaign_events" \
     "$CLICKHOUSE_URL"

curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
     --data-binary "CREATE TABLE campaign_events (
         campaignId UInt64,
         bannerId UInt64,
         click_or_view String,
         event_date Date,
         guid Int64
     ) ENGINE = MergeTree() ORDER BY (event_date, campaignId)" \
     "$CLICKHOUSE_URL"

curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
     --data-binary "DROP TABLE IF EXISTS banner_events" \
     "$CLICKHOUSE_URL"

curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
     --data-binary "CREATE TABLE banner_events (
         campaignId UInt64,
         bannerId UInt64,
         click_or_view String,
         event_date Date,
         guid Int64
     ) ENGINE = MergeTree() ORDER BY (event_date, bannerId)" \
     "$CLICKHOUSE_URL"

echo "✓ Tables created with Int64 for guid"

# Bước 2: Lấy tất cả files từ HDFS
echo "Step 2: Getting all files from HDFS..."
all_files=$(hdfs dfs -ls hdfs://adt-platform-dev-106-254:8120/user/phucnq/CampaignId_BannerId_Guid/ | grep "part-.*\.csv" | awk '{print $8}')

file_count=$(echo "$all_files" | wc -l)
echo "Found $file_count CSV files to process"

# Bước 3: Load từng file
echo "Step 3: Loading all files..."
total_records=0
file_num=0

for file in $all_files; do
    file_num=$((file_num + 1))
    echo "Processing file $file_num/$file_count: $(basename $file)"
    
    # Đếm records trong file
    file_records=$(hdfs dfs -cat "$file" 2>/dev/null | wc -l)
    echo "  File has $file_records lines (including header)"
    
    # Process file với batch insert
    batch_size=1000
    batch_values=""
    batch_count=0
    record_count=0
    
    # Skip header (tail -n +2) và process từng dòng
    hdfs dfs -cat "$file" 2>/dev/null | tail -n +2 | while IFS=',' read -r cid bid action date guid; do
        # Validate data
        if [[ "$cid" =~ ^[0-9]+$ ]] && [[ "$guid" =~ ^-?[0-9]+$ ]]; then
            # Clean data
            bid=${bid:-0}
            action=$(echo "$action" | tr -d '"' | tr -d "'")
            date=$(echo "$date" | tr -d '"' | tr -d "'")
            
            # Add to batch
            if [[ -z "$batch_values" ]]; then
                batch_values="($cid, $bid, '$action', '$date', $guid)"
            else
                batch_values="$batch_values, ($cid, $bid, '$action', '$date', $guid)"
            fi
            
            batch_count=$((batch_count + 1))
            record_count=$((record_count + 1))
            
            # Insert when batch is full
            if [[ $batch_count -ge $batch_size ]]; then
                echo "    Inserting batch of $batch_count records..."
                
                # Insert to campaign_events
                curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
                     --data-binary "INSERT INTO campaign_events (campaignId, bannerId, click_or_view, event_date, guid) VALUES $batch_values" \
                     "$CLICKHOUSE_URL"
                
                # Insert to banner_events
                curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
                     --data-binary "INSERT INTO banner_events (campaignId, bannerId, click_or_view, event_date, guid) VALUES $batch_values" \
                     "$CLICKHOUSE_URL"
                
                echo "    ✓ Inserted $batch_count records (File total: $record_count)"
                
                # Reset batch
                batch_values=""
                batch_count=0
            fi
        fi
    done
    
    # Insert remaining records in final batch
    if [[ $batch_count -gt 0 ]]; then
        echo "    Inserting final batch of $batch_count records..."
        curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
             --data-binary "INSERT INTO campaign_events (campaignId, bannerId, click_or_view, event_date, guid) VALUES $batch_values" \
             "$CLICKHOUSE_URL"
        curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
             --data-binary "INSERT INTO banner_events (campaignId, bannerId, click_or_view, event_date, guid) VALUES $batch_values" \
             "$CLICKHOUSE_URL"
    fi
    
    echo "  ✓ Completed file $file_num: $record_count valid records processed"
    total_records=$((total_records + $record_count))
done

echo "Step 4: Final verification..."

# Check final counts
campaign_count=$(curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
                      --data-binary "SELECT COUNT(*) FROM campaign_events" \
                      "$CLICKHOUSE_URL")

banner_count=$(curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
                    --data-binary "SELECT COUNT(*) FROM banner_events" \
                    "$CLICKHOUSE_URL")

echo "=== LOADING COMPLETED ==="
echo "✓ Total files processed: $file_count"
echo "✓ Campaign events: $campaign_count records"
echo "✓ Banner events: $banner_count records"

# Sample data
echo "Sample data from campaign_events:"
curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
     --data-binary "SELECT campaignId, bannerId, click_or_view, event_date, guid FROM campaign_events LIMIT 5" \
     "$CLICKHOUSE_URL"

echo ""
echo "Data summary:"
curl -s --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASS}" \
     --data-binary "SELECT 
         COUNT(*) as total_records,
         COUNT(DISTINCT campaignId) as unique_campaigns,
         COUNT(DISTINCT bannerId) as unique_banners,
         COUNT(DISTINCT guid) as unique_guids,
         MIN(event_date) as min_date,
         MAX(event_date) as max_date
     FROM campaign_events" \
     "$CLICKHOUSE_URL"
```

#  các hàm truy vấn trong dự án HTML Dashboard

## Tổng quan kiến trúc

### Cấu trúc file JavaScript:

```
Frontend Files/
├── analytics.js      # Hàm kết nối và phân tích database
├── comparison.js     # Hàm so sánh COUNT(DISTINCT) vs HyperLogLog
├── utils.js          # Hàm tiện ích và UI helpers
└── index.html        # Giao diện chính
```

### Cấu hình ClickHouse:

```javascript
const CLICKHOUSE_CONFIG = {
    username: 'default',
    password: 'YTxY6~xEOu~u1',
    url: 'https://ff88qal6fo.germanywestcentral.azure.clickhouse.cloud:8443/'
};
```

---

## Hàm kết nối cơ sở dữ liệu

### 1. executeQuery(query)

**Mục đích:** Thực thi truy vấn SQL trên ClickHouse Cloud

**Tham số:**
- `query` (string): Câu truy vấn SQL

**Trả về:** Promise<string> - Kết quả truy vấn dạng text

**Code:**
```javascript
async function executeQuery(query) {
    try {
        const response = await fetch(CLICKHOUSE_CONFIG.url, {
            method: 'POST',
            headers: {
                'Authorization': 'Basic ' + btoa(CLICKHOUSE_CONFIG.username + ':' + CLICKHOUSE_CONFIG.password),
                'Content-Type': 'text/plain'
            },
            body: query
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const result = await response.text();
        return result.trim();
    } catch (error) {
        throw new Error(`Query failed: ${error.message}`);
    }
}
```

**Ví dụ sử dụng:**
```javascript
const result = await executeQuery('SELECT COUNT(*) FROM campaign_events');
console.log(result); // "1505000"
```

### 2. testConnection()

**Mục đích:** Kiểm tra kết nối ClickHouse và hiển thị thông tin cơ bản

**Truy vấn sử dụng:**
```sql
-- Test connection
SELECT 1

-- Get table counts
SELECT COUNT(*) FROM campaign_events
SELECT COUNT(*) FROM banner_events
```

**Cấu trúc trả về:**
```html
<div class="success">
    <h3>Connection Status: HEALTHY</h3>
    <p><strong>Campaign Events:</strong> 1,505,000</p>
    <p><strong>Banner Events:</strong> 1,505,000</p>
    <p><strong>Database:</strong> ClickHouse Cloud</p>
    <p><strong>Host:</strong> ff88qal6fo.germanywestcentral.azure.clickhouse.cloud</p>
</div>
```

---

## Hàm phân tích dữ liệu

### 3. getDataDistribution()

**Mục đích:** Phân tích phân bố dữ liệu Views vs Clicks

**Truy vấn SQL:**
```sql
SELECT 
    click_or_view,
    COUNT(*) as total_events,
    COUNT(DISTINCT campaignId) as unique_campaigns,
    COUNT(DISTINCT bannerId) as unique_banners,
    COUNT(DISTINCT guid) as unique_users,
    MIN(event_date) as min_date,
    MAX(event_date) as max_date
FROM campaign_events 
GROUP BY click_or_view
ORDER BY click_or_view
FORMAT JSON
```

**Cấu trúc JSON trả về:**
```json
{
    "meta": [
        {"name": "click_or_view", "type": "String"},
        {"name": "total_events", "type": "UInt64"},
        {"name": "unique_campaigns", "type": "UInt64"},
        {"name": "unique_banners", "type": "UInt64"},
        {"name": "unique_users", "type": "UInt64"},
        {"name": "min_date", "type": "Date"},
        {"name": "max_date", "type": "Date"}
    ],
    "data": [
        {
            "click_or_view": "false",
            "total_events": 1400000,
            "unique_campaigns": 100,
            "unique_banners": 500,
            "unique_users": 50000,
            "min_date": "2024-12-01",
            "max_date": "2024-12-31"
        },
        {
            "click_or_view": "true",
            "total_events": 105000,
            "unique_campaigns": 50,
            "unique_banners": 200,
            "unique_users": 10000,
            "min_date": "2024-12-01",
            "max_date": "2024-12-31"
        }
    ],
    "rows": 2
}
```

**HTML output:**
```html
<h3>Data Distribution Analysis</h3>
<div class="comparison-grid">
    <div class="method-card hll-card">
        <h3>VIEW Views</h3>
        <div class="count-display">1,400,000</div>
        <p><strong>Campaigns:</strong> 100</p>
        <p><strong>Banners:</strong> 500</p>
        <p><strong>Users:</strong> 50,000</p>
    </div>
    <div class="method-card distinct-card">
        <h3>CLICK Clicks</h3>
        <div class="count-display">105,000</div>
        <p><strong>Campaigns:</strong> 50</p>
        <p><strong>Banners:</strong> 200</p>
        <p><strong>Users:</strong> 10,000</p>
    </div>
</div>
```

### 4. getTableStructure()

**Mục đích:** Hiển thị cấu trúc bảng campaign_events

**Truy vấn SQL:**
```sql
DESCRIBE campaign_events
FORMAT JSON
```

**Cấu trúc JSON trả về:**
```json
{
    "meta": [
        {"name": "name", "type": "String"},
        {"name": "type", "type": "String"},
        {"name": "default_type", "type": "String"},
        {"name": "default_expression", "type": "String"},
        {"name": "comment", "type": "String"},
        {"name": "codec_expression", "type": "String"},
        {"name": "ttl_expression", "type": "String"}
    ],
    "data": [
        {"name": "campaignId", "type": "UInt64", "default_type": "", "default_expression": "", "comment": "", "codec_expression": "", "ttl_expression": ""},
        {"name": "bannerId", "type": "UInt64", "default_type": "", "default_expression": "", "comment": "", "codec_expression": "", "ttl_expression": ""},
        {"name": "click_or_view", "type": "String", "default_type": "", "default_expression": "", "comment": "", "codec_expression": "", "ttl_expression": ""},
        {"name": "event_date", "type": "Date", "default_type": "", "default_expression": "", "comment": "", "codec_expression": "", "ttl_expression": ""},
        {"name": "guid", "type": "Int64", "default_type": "", "default_expression": "", "comment": "", "codec_expression": "", "ttl_expression": ""}
    ],
    "rows": 5
}
```

### 5. getTopEntities()

**Mục đích:** Tìm top campaigns và banners có nhiều dữ liệu nhất

**Truy vấn SQL cho campaigns:**
```sql
SELECT 
    campaignId,
    click_or_view,
    COUNT(*) as event_count,
    COUNT(DISTINCT guid) as unique_users,
    MIN(event_date) as first_date,
    MAX(event_date) as last_date
FROM campaign_events
WHERE click_or_view = 'false'
GROUP BY campaignId, click_or_view
ORDER BY event_count DESC
LIMIT 10
FORMAT JSON
```

**Truy vấn SQL cho banners:**
```sql
SELECT 
    bannerId,
    click_or_view,
    COUNT(*) as event_count,
    COUNT(DISTINCT guid) as unique_users,
    MIN(event_date) as first_date,
    MAX(event_date) as last_date
FROM banner_events
WHERE click_or_view = 'false'
GROUP BY bannerId, click_or_view
ORDER BY event_count DESC
LIMIT 10
FORMAT JSON
```

---

## Hàm so sánh hiệu suất

### 6. runComparison(entityType, entityId, clickOrView, startDate, endDate)

**Mục đích:** So sánh COUNT(DISTINCT) vs HyperLogLog

**Tham số:**
- `entityType` (string): 'campaign' hoặc 'banner'
- `entityId` (number): ID của campaign hoặc banner
- `clickOrView` (string): 'true' (clicks) hoặc 'false' (views)
- `startDate` (string): Ngày bắt đầu (YYYY-MM-DD)
- `endDate` (string): Ngày kết thúc (YYYY-MM-DD)

**Truy vấn SQL:**
```sql
SELECT 
    ${idField},
    click_or_view,
    COUNT(DISTINCT guid) as distinct_count,
    uniqHLL12(guid) as hll_count
FROM ${tableName}
WHERE event_date BETWEEN '${startDate}' AND '${endDate}'
      AND click_or_view = '${clickOrView}'
      AND ${idField} = ${entityId}
GROUP BY ${idField}, click_or_view
FORMAT JSON
```

**Ví dụ truy vấn cụ thể:**
```sql
SELECT 
    campaignId,
    click_or_view,
    COUNT(DISTINCT guid) as distinct_count,
    uniqHLL12(guid) as hll_count
FROM campaign_events
WHERE event_date BETWEEN '2024-12-11' AND '2024-12-11'
      AND click_or_view = 'false'
      AND campaignId = 24335
GROUP BY campaignId, click_or_view
FORMAT JSON
```

**Cấu trúc JSON trả về:**
```json
{
    "meta": [
        {"name": "campaignId", "type": "UInt64"},
        {"name": "click_or_view", "type": "String"},
        {"name": "distinct_count", "type": "UInt64"},
        {"name": "hll_count", "type": "UInt64"}
    ],
    "data": [
        {
            "campaignId": 24335,
            "click_or_view": "false",
            "distinct_count": 644,
            "hll_count": 637
        }
    ],
    "rows": 1
}
```

### 7. displayComparisonResults(data)

**Mục đích:** Hiển thị kết quả so sánh dưới dạng HTML

**Tham số:**
- `data` (object): Đối tượng chứa kết quả so sánh

**Cấu trúc object data:**
```javascript
{
    entityType: 'campaign',
    entityId: 24335,
    clickOrView: 'false',
    startDate: '2024-12-11',
    endDate: '2024-12-11',
    distinctCount: 644,
    hllCount: 637,
    errorRate: 1.09,
    accuracy: 98.91,
    executionTime: 673
}
```

**HTML output:**
```html
<div class="comparison-grid">
    <div class="method-card distinct-card">
        <h3>COUNT(DISTINCT)</h3>
        <div class="count-display">644</div>
        <p><strong>Accuracy:</strong> 100%</p>
        <p><strong>Method:</strong> Exact counting</p>
    </div>
    <div class="method-card hll-card">
        <h3>HyperLogLog</h3>
        <div class="count-display">637</div>
        <p><strong>Accuracy:</strong> 98.91%</p>
        <p><strong>Method:</strong> Approximate counting</p>
    </div>
</div>

<div class="performance-metrics">
    <div class="metric-card">
        <div class="metric-value">673ms</div>
        <div class="metric-label">Execution Time</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">1.09%</div>
        <div class="metric-label">Error Rate</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">7</div>
        <div class="metric-label">Difference</div>
    </div>
    <div class="metric-card">
        <div class="metric-value">644</div>
        <div class="metric-label">Unique Users</div>
    </div>
</div>
```

### 8. findBestTestCase()

**Mục đích:** Tự động tìm test case tối ưu cho so sánh

**Truy vấn SQL:**
```sql
SELECT 
    campaignId,
    COUNT(*) as event_count,
    COUNT(DISTINCT guid) as unique_users,
    MIN(event_date) as first_date,
    MAX(event_date) as last_date
FROM campaign_events
WHERE click_or_view = 'false'
GROUP BY campaignId
HAVING unique_users BETWEEN 100 AND 10000
ORDER BY unique_users DESC
LIMIT 1
FORMAT JSON
```

---

## Hàm tiện ích UI

### 9. showLoading(message)

**Mục đích:** Hiển thị trạng thái loading

**Tham số:**
- `message` (string): Thông báo loading

**HTML output:**
```html
<div class="loading">
    <div class="spinner"></div>
    <p>Testing ClickHouse Cloud connection...</p>
</div>
```

### 10. showResults(html)

**Mục đích:** Hiển thị kết quả trong results section

### 11. showError(message)

**Mục đích:** Hiển thị thông báo lỗi

**HTML output:**
```html
<div class="error">ERROR: Connection failed: Network error</div>
```

### 12. showSuccess(message)

**Mục đích:** Hiển thị thông báo thành công

**HTML output:**
```html
<div class="success">SUCCESS: ClickHouse Cloud connection successful!</div>
```

### 13. updateDateInputs()

**Mục đích:** Cập nhật input ngày tháng dựa trên lựa chọn range

**Logic:**
```javascript
function updateDateInputs() {
    const range = document.getElementById('dateRange').value;
    const startDate = document.getElementById('startDate');
    const endDate = document.getElementById('endDate');
    
    switch(range) {
        case 'single':
            startDate.value = '2024-12-11';
            endDate.value = '2024-12-11';
            break;
        case 'week':
            startDate.value = '2024-12-11';  // Updated from user change
            endDate.value = '2024-12-17';    // Updated from user change
            break;
        case 'month':
            startDate.value = '2024-12-01';
            endDate.value = '2024-12-31';
            break;
    }
}
```

---

## Tóm tắt

Dashboard HTML này cung cấp một interface hoàn chỉnh để:

1. **Kết nối ClickHouse Cloud** - Truy vấn trực tiếp từ browser
2. **Phân tích dữ liệu** - Views vs Clicks distribution, table structure
3. **So sánh hiệu suất** - COUNT(DISTINCT) vs HyperLogLog
4. **UI thân thiện** - Loading states, error handling, responsive design

Tất cả các hàm đều được thiết kế để hoạt động độc lập và có thể tái sử dụng trong các dự án khác.


