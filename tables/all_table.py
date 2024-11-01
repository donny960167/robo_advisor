# Import 複合主鍵的 `create_table` 函式
from indicators import (
    get_all_roe_columns, get_all_roa_columns, get_all_revenue_columns, 
    get_all_gpm_columns, get_all_eps_columns, 
    get_all_opm_columns, get_all_pe_columns, 
    get_all_fcf_columns, create_table
)

def create_all_tables():
    # 創建包含複合主鍵 (year, quarter) 的表格
    roe_columns = get_all_roe_columns() 
    create_table("roe", roe_columns)

    roa_columns = get_all_roa_columns()  
    create_table("roa", roa_columns)
    
    # 創建每月營收表
    revenue_columns = get_all_revenue_columns()
    create_table("revenue", revenue_columns)

    # 創建毛利率表
    gpm_columns = get_all_gpm_columns()
    create_table("gpm", gpm_columns)

    # 創建 EPS 表
    eps_columns = get_all_eps_columns()
    create_table("eps", eps_columns)

    # 創建營業利益率表
    opm_columns = get_all_opm_columns()
    create_table("opm", opm_columns)

    # 創建本益比表
    pe_columns = get_all_pe_columns()
    create_table("pe", pe_columns)

    # 創建自由現金流表
    fcf_columns = get_all_fcf_columns()
    create_table("fcf", fcf_columns)

if __name__ == "__main__":
    create_all_tables()
