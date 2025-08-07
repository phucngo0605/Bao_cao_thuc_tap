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

## Upload file zip lên Teleport để chạy server và test API truy vấn

