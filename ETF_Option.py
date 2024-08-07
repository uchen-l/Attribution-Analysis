import pandas as pd
import numpy as np
import os
from api_backtest.interface import *
from api_backtest.requests.api_login import LoginUtils
from api_backtest.requests.env_conf import Env
from DailyPortfolio import DailyPortfolio
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import seaborn as sns
import re


LoginUtils.set_auth_info(
        username='intern1',
        password='Intern123456'
    )
os.chdir(r'C:\\Users\\tongyu\\Desktop\\Yuchen_file\\PyProject\\project_1')
trading_data=pd.read_csv('2024年6月24日.csv',index_col=0, encoding='gbk')
if 'Unnamed: 14' in trading_data.columns:
    trading_data.drop(columns=['Unnamed: 14'], inplace=True)
ins_id_list = trading_data['合约编码'].unique().tolist()
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
    # res_df.to_csv(f'{ins_id}_closePrice.csv', encoding='gbk')
    price_dict[ins_id] = res_df
# print(price_dict)
# 创建一个新的列来存储找到的 closePrice
trading_data['closePrice'] = np.nan
# 遍历 data，找到对应的合约编码和日期，然后在 price_dict 中找到对应的价格
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


# 新增一列合约乘数，目前全部设置成10000
trading_data['合约乘数'] = 10000
trading_data = trading_data.iloc[::-1].reset_index(drop=True)
# trading_data.to_csv('2024年6月24日_更新2.csv', encoding='gbk')
# 引入 DailyPortfolio 类,处理方法和option_main类似，但是需要修改一些细节
initial_cash = 100000 

# tradingday = trading_data['成交日期'].unique().tolist()

all_dates = set()
for res_df in price_dict.values():
    all_dates.update(res_df['tradeDate'].astype(str).unique().tolist())

# 将这些日期合并到一个列表中
tradingday = sorted(list(all_dates))
print(tradingday)
total_portfolio = {}
total_comm_var_diff = pd.DataFrame(columns = ['合约编码','操作误差'])
holding_diff = pd.DataFrame()
pnl_total = pd.DataFrame()
for i, date in enumerate(tqdm(tradingday)):
     # 检查是不是第一天，如果是的话创立一个空Daily Portfolio，如果不是则copy昨天的DailyPortfolio
    if date == tradingday[0]:
        total_portfolio[date] = DailyPortfolio(date, initial_cash, price_dict)
    else:
        previous_date = tradingday[i-1]
        previous_portfolio = total_portfolio[previous_date]
        new_portfolio = DailyPortfolio(date, previous_portfolio.cash, price_dict)
        new_portfolio.holdings = previous_portfolio.holdings.copy()
        new_portfolio.total_assets = previous_portfolio.total_assets    
        total_portfolio[date] = new_portfolio
    # 检查该交易日有没有在comm_df中出现，如果有的话识别是买/卖，没有的话正常更新
    if date in trading_data['成交日期'].values:
        transcations = trading_data[trading_data['成交日期'] == date]
        # 保存历史总资产
        total_portfolio[date].yesterday_total_assets = total_portfolio[date].total_assets
        for contract_name in total_portfolio[date].holdings['合约编码']:
            # 直接调用 update_holding 函数，这里data通过合约名字contract加入新的名字
            data = {'合约编码': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '合约编码'].values[0],
                    #'合约': contract_name,
                    '合约名称': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '合约名称'].values[0],
                    '成交数量': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '成交数量'].values[0],
                    '收盘价': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '收盘价'].values[0],
                    '合约乘数': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '合约乘数'].values[0]}
            total_portfolio[date].update_holding(data)    
        # print(f"日期{date}的交易记录：\n{transcations}")
        for _, transcation in transcations.iterrows():
            if transcation['备注'] == '买入开仓':
                total_portfolio[date].buy_holding_open(transcation)
            elif transcation['备注'] == '卖出开仓':
                total_portfolio[date].sell_holding_open(transcation)
            elif transcation['备注'] == '买入平仓':
                total_portfolio[date].buy_holding_close(transcation)
            elif transcation['备注'] == '卖出平仓':
                total_portfolio[date].sell_holding_close(transcation)
        total_portfolio[date].calculate_today_difference()
    else:
        total_portfolio[date].yesterday_total_assets = total_portfolio[date].total_assets

        # print(f"这里的合约有:{total_portfolio[date].holdings['合约']}")
        for contract_name in total_portfolio[date].holdings['合约编码']:
            # 直接调用 update_holding 函数，这里data通过合约名字contract加入新的名字
            data = {'合约编码': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '合约编码'].values[0],
                    # '合约': contract_name,
                    '合约名称': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '合约名称'].values[0],
                    '成交数量': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '成交数量'].values[0],
                    '收盘价': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '收盘价'].values[0],
                    '合约乘数': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约编码'] == contract_name, '合约乘数'].values[0]}
            total_portfolio[date].update_holding(data)

        total_portfolio[date].calculate_today_difference()
    holding_diff = holding_diff.append({
        '日期': date,
        'slippage': total_portfolio[date].slippage,
        'close_holding_value': total_portfolio[date].close_holding_value,
        'actual_holding_value': total_portfolio[date].actual_holding_value
    }, ignore_index=True)
    # self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
    # total_portfolio[date].comm_var_diff = total_comm_var_diff
    total_comm_var_diff = pd.concat([total_comm_var_diff, total_portfolio[date].comm_var_diff], ignore_index=True)
    pnl_total = pd.concat([pnl_total, total_portfolio[date].pnl_df], ignore_index=True)
    
# pnl_total.to_csv('pnl_total.csv', encoding='gbk')

# pnl_group_df = pnl_total.groupby(['日期'],['合约'],['变化来源'], as_index=False).sum()
# pnl_group_df = pnl_total.groupby(['合约名称','日期', '合约编码', '变化来源'], as_index=False)[['操作误差', '收益变化']].sum()
# pnl_group_df['手续费'] = -pnl_group_df['手续费']

def extract_etf_name(contract_name):
    pattern = re.compile(r'^\d+ETF')
    match = pattern.match(contract_name)
    return match.group() if match else None

pnl_total['合约名称'] = pnl_total['合约名称'].apply(extract_etf_name)
pnl_group_df = pnl_total.groupby(['合约名称','日期', '变化来源'], as_index=False)[['操作误差', '收益变化']].sum()

# pnl_group_df.to_csv('pnl_group_df_etf_option_merged_name.csv', encoding='gbk')
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