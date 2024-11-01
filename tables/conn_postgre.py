import psycopg2
from psycopg2 import sql

# 连接到 PostgreSQL 数据库
def conn_postgre():
    conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Sql960167",
    host="localhost",
    port="5432"
)
    cur = conn.cursor()
    return cur,conn

def create_table(name, columns):
    cur, conn = conn_postgre()
    # Convert table name to sql.Identifier
    table_name = sql.Identifier(name)
    # Convert column names to sql.Identifier and format the columns
    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} FLOAT4").format(sql.Identifier(col)) for col in columns
    )
    # Create the SQL query
    create_table_query = sql.SQL("""
        CREATE TABLE {} (
            {}
        )
    """).format(
        table_name,
        column_definitions
    )
    # Execute the query
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

def insert_data(name, df):
    cur,conn = conn_postgre() 
    # 确保表名是正确的格式
    table_name = sql.Identifier(name)
    # 获取列名
    columns = df.columns.tolist()
    # 生成 SQL 查询字符串
    column_placeholders = ", ".join([sql.Identifier(col).as_string(conn) for col in columns])
    value_placeholders = ", ".join(["%s"] * len(columns))
    insert_query = sql.SQL("""
        INSERT INTO {} ({})
        VALUES ({})
    """).format(table_name,
        sql.SQL(column_placeholders),
        sql.SQL(value_placeholders)
    )
    try:
        # 执行插入操作
        for i, row in df.iterrows():
            cur.execute(insert_query, tuple(row))
        # 提交事务
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        # 关闭游标和连接
        cur.close()
        conn.close()

def change_column_type_to_timestamp(table_name, column_name="date"):
    # 定義需要保留 date 欄位的表格清單
    keep_date_tables = ["open", "close", "high", "low", "volume"]
    
    # 若表格在保留清單中，跳過轉換
    if table_name in keep_date_tables:
        print(f"Skipping modification for table: {table_name}")
        return

    cur, conn = conn_postgre()
    try:
        # 確認 table 是否具有 date 欄位
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = '{column_name}';")
        if not cur.fetchone():
            print(f"Table {table_name} does not have a '{column_name}' column. Skipping modification.")
            return
        
        # 添加临时列
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name}_temp TIMESTAMP;")
        
        # 檢查臨時列後進行更新
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = '{column_name}_temp';")
        if cur.fetchone():
            cur.execute(f"""
                UPDATE {table_name}
                SET {column_name}_temp = '1970-01-01'::DATE + ({column_name} * INTERVAL '1 day');
            """)
        
        # 刪除舊的 date 欄位
        cur.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name};")
        
        # 將新列重命名為 date
        cur.execute(f"ALTER TABLE {table_name} RENAME COLUMN {column_name}_temp TO {column_name};")
        
        # 設定 year 和 quarter 為複合主鍵
        cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (year, quarter);")
        
        conn.commit()
        print(f"Table {table_name}: Column 'date' changed to timestamp and primary key updated.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()







