import os
import pandas as pd
import pickle
import re
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import seaborn as sns
import tqdm as tqdm
from DataCheck import data_check, DataCheck, convert_column_type, read_statement, name_correction, add_price, add_multiple_index, check_or_create, get_period, process_statements
from DailyPortfolio import DailyPortfolio_Generator
from tqdm import tqdm

# 更改工作目录到指定大文件夹
os.chdir(r'C:\\Users\\tongyu\\Desktop\\Yuchen_file\\PyProject\\project_1')

# 检查Data pickle，调用该类可以实现从本地文件夹中读取数据，或从创建的新的data.pickle调取过往已经储存的函数
# 创建一个空列表来存储所有的statement和DataFrame
statements = {}
dataframes = []


    
big_dataframe = process_statements(statements, dataframes)

# comm_df: 所有商品期货（非期权）的数据
trading_data = pd.DataFrame()
    
option_df = big_dataframe[big_dataframe['品种'].str.contains('期权')].reset_index(drop=True)
trading_data = big_dataframe[~big_dataframe['品种'].str.contains('期权')].reset_index(drop=True)

trading_data.insert(trading_data.shape[1], "收盘价", None)
trading_data = name_correction(trading_data)

# 只用于计算商品成交价(comm_df)，不用于计算期权(option_df)
        
trading_data = trading_data.apply(add_price, axis = 1)
trading_data.insert(trading_data.shape[1], "合约乘数", None)
# 计算合约乘数，目前只有IF/IC供选择，后续可以增加

trading_data =trading_data.apply(add_multiple_index, axis = 1)
tradingday = get_period(trading_data)
option_df = option_df.apply(add_multiple_index, axis = 1)
COLUMN_MAPPING = {
    '合约名称': '品种',
    '合约编码': '合约',
    '成交数量': '手数',
    '成交价格': '成交价',
    '收盘价': '收盘价',
    '成交金额': '成交额', #需要检查
    '合约乘数': '合约乘数',
    '收盘总价': '收盘总价',
    '操作误差': '操作误差',
    '日期': '日期',
    '收益变化': '收益变化',
    '变化来源': '变化来源',
    'closePrice': '收盘价'
}
reverse_column_mapping = {v: k for k, v in COLUMN_MAPPING.items()}
trading_data.rename(columns=reverse_column_mapping, inplace=True)
# option_df.to_csv('option_df.csv', encoding='gbk')
# trading_data.to_csv('trading_data_comm_df.csv', encoding='gbk')
# 设置初始现金
initial_cash = 50000000

# 0807 删除了filename
# filename = 'daily_statement.pickle'
# 提取交易表里的开始日和截止日，然后在价目单里索取在这之间的交易
# 这里加上 tradingday, initial_cash, history_price, trading_data

history_price = data_check.data
total_comm_var_diff,pnl_total,holding_diff = DailyPortfolio_Generator(tradingday, initial_cash, history_price, trading_data)
 
pnl_total.to_csv('pnl_total_commodity.csv', encoding='gbk')
pnl_group_df = pnl_total.groupby(['合约名称','日期', '变化来源'], as_index=False)[['操作误差', '收益变化']].sum()

pnl_group_df.to_csv('pnl_group_df_commodity_merged_name.csv', encoding='gbk')
holding_diff['sum_slippage'] = holding_diff['slippage'].cumsum()
# holding_diff.to_csv('holding_diff.csv', encoding='gbk')
total_comm_var_diff.to_csv('total_comm_var_diff.csv', encoding='gbk')

# 根据合约名合并并汇总操作误差
grouped_df_name = total_comm_var_diff.groupby('合约名称')['操作误差'].sum().reset_index()
# grouped_df_name.to_csv('grouped_df_name.csv', encoding='gbk')
grouped_df_date = total_comm_var_diff.groupby('日期')['操作误差'].sum().reset_index()
grouped_df_date['误差总量'] = grouped_df_date['操作误差'].cumsum()
# grouped_df_date.to_csv('grouped_df_date.csv', encoding='gbk')
# 数据可视化
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


# 计算每日收益变化汇总
daily_pnl_sum = pnl_group_df.groupby('日期')['收益变化','操作误差'].sum().reset_index()
daily_pnl_sum.columns = ['日期', '总收益变化','总操作误差']
daily_pnl_sum['累计收益变化'] = daily_pnl_sum['总收益变化'].cumsum()
daily_pnl_sum['累计操作误差'] = daily_pnl_sum['总操作误差'].cumsum()
daily_pnl_sum['真实交易汇总'] = (daily_pnl_sum['总收益变化'] + daily_pnl_sum['总操作误差']).cumsum()
# daily_pnl_sum.to_csv('daily_pnl_sum.csv', encoding='gbk')

# 将日期列转换为日期类型
pnl_group_df['日期'] = pd.to_datetime(pnl_group_df['日期'])
daily_pnl_sum['日期'] = pd.to_datetime(daily_pnl_sum['日期'])

# 画图
plt.figure(figsize=(12, 8))
sns.lineplot(data=daily_pnl_sum, x='日期', y='累计收益变化', marker='o', label='收益变化（收盘价）', linewidth=2)

contracts = pnl_group_df['合约名称'].unique()
for contract in contracts:
    contract_df = pnl_group_df[pnl_group_df['合约名称'] == contract]
    contract_cumsum = contract_df.groupby('日期')['收益变化'].sum().reset_index()
    contract_cumsum['累计收益变化'] = contract_cumsum['收益变化'].cumsum()
    sns.lineplot(data=contract_cumsum, x='日期', y='累计收益变化', linestyle='--', label=f'{contract} 累计收益变化')

plt.title('各品种（合约名称）收益变化')
plt.xlabel('日期')
plt.ylabel('收益变化')
plt.legend()
plt.grid(True)
plt.show()

# 画图
plt.figure(figsize=(12, 8))
sns.lineplot(data=daily_pnl_sum, x='日期', y='累计收益变化', marker='o', label='收益变化（收盘价）', linestyle=':')
sns.lineplot(data=daily_pnl_sum, x='日期', y='累计操作误差', marker='o', label='累计操作误差', linestyle='--')
sns.lineplot(data=daily_pnl_sum, x='日期', y='真实交易汇总', marker='o', label='真实交易汇总', linewidth=2)

plt.title('收益变化与操作误差汇总')
plt.xlabel('日期')
plt.ylabel('收益变化')
plt.legend()
plt.grid(True)
plt.show()

