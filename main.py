# 导入其他文件的主函数
from Commodity_Option import main as commodity_option_main
from ETF_Option import main as etf_option_main
from Commodity_Future import main as commodity_future_main
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from DataCheck import data_check, get_period, txt_conversion, get_api_price, add_api_price, extract_etf_name
import pandas as pd
import os
from api_backtest.interface import *
from api_backtest.requests.api_login import LoginUtils
from api_backtest.requests.env_conf import Env
from DailyPortfolio import portfolio_performance_analyzer
from plot import two_plots

def main():
    os.chdir(r'C:\\Users\\tongyu\\Desktop\\Yuchen_file\\PyProject\\project_1')
    # 初始化 商品期权 商品期货 ETF期权 的数据
    comm_future_df, comm_option_df = txt_conversion()
    etf_option_df=pd.read_csv('2024年6月24日.csv',index_col=0, encoding='gbk')
    
    # 商品期货/期权需要修改名字，保证后续传入DailyPortfolio类时不出问题
    COLUMN_MAPPING = {
        '品种': '合约名称',
        '合约': '合约编码',
        '手数': '成交数量',
        '成交价': '成交价格',
        '成交额': '成交金额', #需要检查
        '收盘价': 'closePrice'
    }
    comm_future_df.rename(columns=COLUMN_MAPPING, inplace=True)
    comm_option_df.rename(columns=COLUMN_MAPPING, inplace=True)
    
    # 获取历史每日收盘价数据, 商品期货的历史价格数据已经在data_check中从本地获取
    comm_future_history_price = data_check.data
    comm_option_history_price = get_api_price(comm_option_df)
    etf_option_history_price = get_api_price(etf_option_df)
    
    # 商品期权和ETF期权原本的数据中没有收盘价，需要调用API获取并且加入到原数据中，商品期货的历史价格数据已经在从本地获取后直接加入df中
    comm_option_df = add_api_price(comm_option_df, comm_option_history_price)
    etf_option_df = add_api_price(etf_option_df, etf_option_history_price)
    
    # 把日期转换成datetime格式,目前ETF期权数据顺序是倒序的，因此需要转换，同时合约乘数直接加上10000
    etf_option_df = etf_option_df.iloc[::-1].reset_index(drop=True)
    etf_option_df['合约乘数'] = 10000
    comm_future_df['成交日期'] = pd.to_datetime(comm_future_df['成交日期'])
    
    # 这里的成交日期需要以yyyy-mm-dd的str格式传入，否则会报错
    # comm_option_df['成交日期'] = pd.to_datetime(comm_option_df['成交日期'])
    # etf_option_df['成交日期'] = pd.to_datetime(etf_option_df['成交日期'])
    
    # 分为期货和期权的tradingday，获取方式略有差异
    comm_future_tradingday = get_period(comm_future_df, check_type='future')
    comm_option_tradingday = get_period(comm_option_history_price, check_type='option')
    etf_option_tradingday = get_period(etf_option_history_price, check_type='option')

    # 统一设置initial_cash
    initial_cash = 100000000
    
    # 三种不同类型的DailyPortfolio，他们的中间表
    total_comm_var_diff_0,pnl_total_0,holding_diff_0 = portfolio_performance_analyzer(comm_future_tradingday, initial_cash, comm_future_history_price,comm_future_df)
    total_comm_var_diff_1,pnl_total_1,holding_diff_1 = portfolio_performance_analyzer(comm_option_tradingday, initial_cash, comm_option_history_price,comm_option_df)
    total_comm_var_diff_2,pnl_total_2,holding_diff_2 = portfolio_performance_analyzer(etf_option_tradingday, initial_cash, etf_option_history_price,etf_option_df)
    
    # 画图
    two_plots(pnl_total_0,'commodity_future')
    two_plots(pnl_total_1,'commodity_option')
    # ETF数据需要再次合并
    pnl_total_2['合约名称'] = pnl_total_2['合约名称'].apply(extract_etf_name)
    two_plots(pnl_total_2,'etf_option')
    
    

    
    # print("Running Commodity_Future script...")
    # commodity_future_chart1, commodity_future_chart2 = commodity_future_main()
    # print("Commodity_Future script completed.")
    
    # print("Running Commodity_Option script...")
    # commodity_option_chart1,commodity_option_chart2 = commodity_option_main()
    # print("Commodity_Option script completed.")

    # print("Running ETF_Option script...")
    # etf_option_chart1, etf_option_chart2 = etf_option_main()
    # print("ETF_Option script completed.")
    
    # # 创建 3 行 2 列的 subplot 布局
    # fig, axs = plt.subplots(3, 2, figsize=(36, 36))

    # axs[0, 0].imshow(commodity_future_chart1.canvas.buffer_rgba())
    # axs[0, 0].axis('off')

    # axs[0, 1].imshow(commodity_future_chart2.canvas.buffer_rgba())
    # axs[0, 1].axis('off')

    # axs[1, 0].imshow(commodity_option_chart1.canvas.buffer_rgba())
    # axs[1, 0].axis('off')

    # axs[1, 1].imshow(commodity_option_chart2.canvas.buffer_rgba())
    # axs[1, 1].axis('off')

    # axs[2, 0].imshow(etf_option_chart1.canvas.buffer_rgba())
    # axs[2, 0].axis('off')
    
    # axs[2, 1].imshow(etf_option_chart2.canvas.buffer_rgba())
    # axs[2, 1].axis('off')
    
    # plt.tight_layout()
    # plt.savefig('combined_result.png')
    # plt.show()
    
if __name__ == "__main__":
    main()