import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def two_plots(pnl_total,category):
    
    category_to_filename={
        'commodity_future':'商品期货',
        'commodity_option':'商品期权',
        'etf_option':'ETF期权',
    }
    filename = category_to_filename[category]
    
    # 中文支持 目前字体为黑体
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 这里copy的是商品期权的code，理论上来说画图函数都应该是一样的 需要进行多次检查
    pnl_group_df = pnl_total.groupby(['合约名称','日期', '变化来源'], as_index=False)[['操作误差', '收益变化']].sum()
    
    # pnl_total.to_csv(f'pnl_total_{category}.csv', encoding='gbk')
    # pnl_group_df.to_csv(f'pnl_group_df_{category}.csv', encoding='gbk')

    # holding_diff['sum_slippage'] = holding_diff['slippage'].cumsum()
    # grouped_df_date = total_comm_var_diff.groupby('日期')['操作误差'].sum().reset_index()
    # grouped_df_date['误差总量'] = grouped_df_date['操作误差'].cumsum()
    # 数据可视化

    # 计算每日收益变化汇总
    daily_pnl_sum = pnl_group_df.groupby('日期')['收益变化','操作误差'].sum().reset_index()
    daily_pnl_sum.columns = ['日期', '总收益变化','总操作误差']
    daily_pnl_sum['累计收益变化'] = daily_pnl_sum['总收益变化'].cumsum()
    daily_pnl_sum['累计操作误差'] = daily_pnl_sum['总操作误差'].cumsum()
    daily_pnl_sum['真实交易汇总'] = (daily_pnl_sum['总收益变化'] + daily_pnl_sum['总操作误差']).cumsum()
    daily_pnl_sum.to_csv(f'daily_pnl_sum_{category}_0912.csv', encoding='gbk')

    # 将日期列转换为日期类型
    pnl_group_df['日期'] = pd.to_datetime(pnl_group_df['日期'])
    daily_pnl_sum['日期'] = pd.to_datetime(daily_pnl_sum['日期'])

    # 画图
    fig1=plt.figure(figsize=(12, 8))
    sns.lineplot(data=daily_pnl_sum, x='日期', y='累计收益变化', marker='o', label='收益变化（收盘价）', linewidth=2)

    contracts = pnl_group_df['合约名称'].unique()
    for contract in contracts:
        contract_df = pnl_group_df[pnl_group_df['合约名称'] == contract]
        contract_cumsum = contract_df.groupby('日期')['收益变化'].sum().reset_index()
        contract_cumsum['累计收益变化'] = contract_cumsum['收益变化'].cumsum()
        sns.lineplot(data=contract_cumsum, x='日期', y='累计收益变化', linestyle='--', label=f'{contract} 累计收益变化')

    plt.title(f'{filename}各品种（合约名称）收益变化')
    plt.xlabel('日期')
    plt.ylabel('收益变化')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{filename}各品种（合约名称）收益变化_912.png')
    plt.close(fig1)

    # 画图
    fig2=plt.figure(figsize=(12, 8))
    sns.lineplot(data=daily_pnl_sum, x='日期', y='累计收益变化', marker='o', label='收益变化（收盘价）', linestyle=':')
    sns.lineplot(data=daily_pnl_sum, x='日期', y='累计操作误差', marker='o', label='累计操作误差', linestyle='--')
    sns.lineplot(data=daily_pnl_sum, x='日期', y='真实交易汇总', marker='o', label='真实交易汇总', linewidth=2)

    plt.title(f'{filename}收益变化与操作误差汇总')
    plt.xlabel('日期')
    plt.ylabel('收益变化')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{filename}收益变化与操作误差汇总_912.png')
    plt.close(fig2)

    return fig1, fig2