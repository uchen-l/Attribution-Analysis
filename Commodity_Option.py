import pandas as pd
import numpy as np
import os
from api_backtest.interface import *
from api_backtest.requests.api_login import LoginUtils
from api_backtest.requests.env_conf import Env

from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import seaborn as sns
import re
from DailyPortfolio import DailyPortfolio_Generator

LoginUtils.set_auth_info(
        username='intern1',
        password='Intern123456'
    )
# 映射关系
os.chdir(r'C:\\Users\\tongyu\\Desktop\\Yuchen_file\\PyProject\\project_1')

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
    '变化来源': '变化来源'
}
reverse_column_mapping = {v: k for k, v in COLUMN_MAPPING.items()}

trading_data=pd.read_csv('option_df.csv',index_col=0, encoding='gbk')
trading_data['合约'] = trading_data['合约'].str.upper()
ins_id_list = [ins_id.upper() for ins_id in trading_data['合约'].unique().tolist()]
trading_data.rename(columns=reverse_column_mapping, inplace=True)
all_start_date = trading_data['成交日期'].min()
all_end_date = trading_data['成交日期'].max()

price_dict = {}
for ins_id in tqdm(ins_id_list):
    instrument_id_list = [str(ins_id)]
    ins_data = trading_data[trading_data['合约编码'] == ins_id]
    result = get_quote_close(
        instrument_id_list=instrument_id_list,
        start_date=all_start_date,
        end_date=all_end_date
    )
    res_df=pd.DataFrame(result)

    price_dict[ins_id] = res_df


trading_data['closePrice'] = np.nan
for index, row in trading_data.iterrows():
    ins_id = row['合约编码']
    trade_date = row['成交日期']
    if ins_id in price_dict:
        res_df = price_dict[ins_id]
        # 确保 tradeDate 的类型一致
        res_df['tradeDate'] = res_df['tradeDate'].astype(str)
        # 查找对应的 closePrice
        close_price = res_df.loc[res_df['tradeDate'] == trade_date, 'closePrice']
        if not close_price.empty:
            trading_data.at[index, 'closePrice'] = close_price.values[0]
    else:
        print(f"合约编码 {ins_id} 未找到对应的价格数据")

# trading_data.to_csv('商品期权收盘价updated.csv', encoding='gbk')

all_dates = set()
for res_df in price_dict.values():
    all_dates.update(res_df['tradeDate'].astype(str).unique().tolist())

# 将这些日期合并到一个列表中
tradingday = sorted(list(all_dates))
initial_cash = 1000000

total_comm_var_diff,pnl_total,holding_diff = DailyPortfolio_Generator(tradingday, initial_cash, price_dict, trading_data)

pnl_group_df = pnl_total.groupby(['合约名称','日期', '变化来源'], as_index=False)[['操作误差', '收益变化']].sum()

# pnl_group_df.to_csv('pnl_group_df_etf_option_merged_name.csv', encoding='gbk')
holding_diff['sum_slippage'] = holding_diff['slippage'].cumsum()
# holding_diff.to_csv('holding_diff.csv', encoding='gbk')
# total_comm_var_diff.to_csv('total_comm_var_diff.csv', encoding='gbk')

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
daily_pnl_sum.to_csv('daily_pnl_sum.csv', encoding='gbk')

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