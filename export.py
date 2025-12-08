#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import csv
import datetime
import os
import tarfile

# === 数据库连接配置（改成和你 test_pg_conn.py 里的一样）===
DB_CONFIG = {
    "host": "1.1.1.1",   # 数据库主机
    "port": 5432,          # 端口
    "dbname": "1.1.1.1",   # 数据库名
    "user": "1",        # 用户名
    "password": "1"  # 密码
}

# === 要导出的日期（update_stamp 的那一天）===
DAY_STR = "2025-12-07"
DAY_START = f"{DAY_STR} 00:00:00"
day_obj = datetime.datetime.strptime(DAY_STR, "%Y-%m-%d") + datetime.timedelta(days=1)
DAY_END = day_obj.strftime("%Y-%m-%d 00:00:00")

# === 生成带时间戳的输出文件名 ===
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")

csv_path = f"/var/tmp/orders_{DAY_STR}_{timestamp}.csv"
tar_path = f"/var/tmp/orders_{DAY_STR}_{timestamp}.tar.gz"

print(f"🚀 开始导出 public.orders 中 update_stamp = {DAY_STR} 的数据到: {csv_path}")

# === 连接数据库 ===
conn = psycopg2.connect(**DB_CONFIG)

# 关键：库是 SQL_ASCII，用一个“全字节都合法”的编码来解码，避免 UnicodeDecodeError
# 这里用 LATIN1，0x00-0xFF 都对应一个字符，不会报错
conn.set_client_encoding('LATIN1')

# === 服务器端游标：流式读取数据 ===
stream_cur = conn.cursor(name='orders_stream_cursor')

query = """
    SELECT *
    FROM public.orders
    WHERE update_stamp >= %s
      AND update_stamp <  %s;
"""
print("📦 正在执行查询，请稍候 ...")
stream_cur.execute(query, (DAY_START, DAY_END))

# === 普通游标：拿表头（列名） ===
header_cur = conn.cursor()
header_cur.execute(
    """
    SELECT *
    FROM public.orders
    WHERE update_stamp >= %s
      AND update_stamp <  %s
    LIMIT 0;
    """,
    (DAY_START, DAY_END)
)
col_names = [desc[0] for desc in header_cur.description]
header_cur.close()

# === 导出到 CSV ===
count = 0

def to_str_safe(v):
    """把每个字段安全转换成字符串，避免 None 和奇怪类型出问题"""
    if v is None:
        return ""
    return str(v)

with open(csv_path, "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    # 写表头
    writer.writerow(col_names)

    # 逐行写出数据
    for row in stream_cur:
        # row 是 tuple，这里逐个转成字符串，确保 csv 模块只处理 str
        safe_row = [to_str_safe(v) for v in row]
        writer.writerow(safe_row)

        count += 1
        if count % 10000 == 0:
            print(f"✅ 已导出 {count:,} 行...")

stream_cur.close()
conn.close()

print(f"✅ 导出完成，共导出 {count:,} 行数据。")

# === 压缩为 .tar.gz 并删除原 CSV ===
print(f"📦 正在压缩到 {tar_path} ...")
with tarfile.open(tar_path, "w:gz") as tar:
    tar.add(csv_path, arcname=os.path.basename(csv_path))

os.remove(csv_path)

print(f"🎯 压缩完成: {tar_path}")
