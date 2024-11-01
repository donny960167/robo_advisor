from sqlalchemy import create_engine, Table, Column, Integer, Numeric, MetaData, String, PrimaryKeyConstraint
import pandas as pd

def format_code(code):
    if isinstance(code, str) and code.isdigit():
        code_int = int(code)
        if code_int < 100:
            return f"{code_int:04d}"
    return code
# 定義 engine，以便連接至 PostgreSQL
engine = create_engine("postgresql://postgres:Sql960167@localhost:5432/postgres")
def create_table(table_name, columns):
    
    metadata = MetaData()
    # 動態生成欄位結構，year 和 quarter 設為複合主鍵
    table_columns = [Column("year", Integer, primary_key=True),
                     Column("quarter", Integer, primary_key=True)]
    
    # 剩餘的指標欄位以 Numeric 型別
    for column_name in columns[2:]:  # 跳過 'year' 和 'quarter'
        table_columns.append(Column(column_name, Numeric))

    # 定義表格
    table = Table(table_name, metadata, *table_columns,
                  PrimaryKeyConstraint('year', 'quarter'))
    
    # 在 PostgreSQL 中創建表格
    metadata.create_all(engine)
    print(f"Table '{table_name}' created with composite primary key (year, quarter).")

def insert_data(table_name, df):
    with engine.begin() as connection:
        for _, row in df.iterrows():
            row_data = row.to_dict()
            query = Table(table_name, MetaData(), autoload_with=engine).insert().values(row_data)
            connection.execute(query)
    print(f"Data inserted successfully into '{table_name}'.")



# 1. ROE 表格
def get_all_roe_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_roe" for code in code_list]
    
    columns = ["year", "quarter"] + code_list
    return columns

# 2. ROA 表格
def get_all_roa_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_roa" for code in code_list]
    
    columns = ["year", "quarter"] + code_list
    return columns

def get_all_revenue_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_revenue" for code in code_list]  # 每月營收

    columns = ["year", "quarter"] + code_list
    return columns

# 4. Gross Margin 表格
def get_all_gpm_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_gpm" for code in code_list]  # 毛利率

    columns = ["year", "quarter"] + code_list
    return columns

# 5. EPS 表格
def get_all_eps_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_eps" for code in code_list]  # EPS

    columns = ["year", "quarter"] + code_list
    return columns

# 6. Operating Margin 表格
def get_all_opm_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_opm" for code in code_list]  # 營業利益率

    columns = ["year", "quarter"] + code_list
    return columns

# 7. P/E Ratio 表格
def get_all_pe_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_pe" for code in code_list]  # 本益比

    columns = ["year", "quarter"] + code_list
    return columns

# 8. Free fcf Flow 表格
def get_all_fcf_columns():
    stock_code = pd.read_csv("robo_advisor/crawler/stock_code.csv")
    code_list = stock_code["0"].astype(str).tolist()
    code_list = [format_code(code) for code in code_list]
    code_list = [code + "_fcf" for code in code_list]  # 自由現金流

    columns = ["year", "quarter"] + code_list
    return columns

# 用於創建所有複合主鍵表格的主函式
def create_all_composite_tables():
    roe_columns = get_all_roe_columns()
    create_table("roe", roe_columns)
    
    roa_columns = get_all_roa_columns()
    create_table("roa", roa_columns)
    
    # 根據需要添加其他表格的創建
    # create_table("revenue", get_all_revenue_columns())
    # create_table("gpm", get_all_gpm_columns())
    # 依此類推

# 範例使用：插入 ROE 表格數據
#def insert_all_data():
    #roe_data = pd.DataFrame(...)  # 填入實際的 DataFrame 資料
    #insert_data("roe", roe_data)

    #roa_data = pd.DataFrame(...)  # 填入實際的 DataFrame 資料
    #insert_data("roa", roa_data)