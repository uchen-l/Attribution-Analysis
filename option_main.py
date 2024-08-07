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
from DataCheck import DataCheck

# 更改工作目录到指定大文件夹
os.chdir(r'C:\\Users\\tongyu\\Desktop\\Yuchen_file\\PyProject\\project_1')

# 检查Data pickle，调用该类可以实现从本地文件夹中读取数据，或从创建的新的data.pickle调取过往已经储存的函数

class DailyPortfolio:
    def __init__(self, date, cash):
        self.date = date
        self.holdings = pd.DataFrame(columns = ['品种','合约','手数','成交价','收盘价','成交额','收盘总价','合约乘数','操作误差'])
        self.today_buying = pd.DataFrame(columns = ['品种','合约','手数','成交价','收盘价','成交额','收盘总价','合约乘数','操作误差'])
        self.today_selling = pd.DataFrame(columns = ['品种','合约','手数','成交价','收盘价','成交额','收盘总价','合约乘数','操作误差'])
        self.comm_var_diff = pd.DataFrame(columns = ['日期','品种','合约','操作误差'])
        self.pnl_df = pd.DataFrame(columns = ['日期','品种','合约','手续费','操作误差','收益变化','变化来源'])
        self.cash = cash
        # 收盘价计算的总资产
        self.total_assets = cash
        # 实际总资产
        self.actual_total_assets = cash
        # 储存昨日收益
        self.yesterday_total_assets = self.total_assets
        # 计算差价
        # 今日收盘价持仓额
        self.close_holding_value = 0
        # 今日实际持仓额
        self.actual_holding_value = 0
        self.difference = 0
        # 操作误差
        self.slippage = 0
        
    def buy_holding(self,data):
        total_value = data['手数'] * data['收盘价'] * data['合约乘数']
        # 扣除手续费
        self.cash = self.cash - data['手续费'] - total_value
        new_holding = pd.DataFrame([{
            '品种': data['品种'],
            '合约': data['合约'],
            '手数': data['手数'],
            '成交价': data['成交价'],
            '收盘价': data['收盘价'],
            '成交额': data['成交额'],
            '合约乘数': data['合约乘数'],
            '收盘总价': total_value,
            '操作误差': -(data['成交额'] - total_value)
        }])
        var_diff = pd.DataFrame([{
            '日期': data['成交日期'],
            '品种': data['品种'],
            '合约': data['合约'],
            '操作误差': -(data['成交额'] - total_value)
        }])
        buy_pnl = pd.DataFrame([{ 
            '日期': data['成交日期'],
            '品种': data['品种'],
            '合约': data['合约'],
            '手续费': data['手续费'],
            '操作误差': -(data['成交额'] - total_value),
            '收益变化': 0,
            '变化来源': '买入'
        }])
        self.pnl_df = pd.concat([self.pnl_df, buy_pnl], ignore_index=True)
        self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
        self.holdings = pd.concat([self.holdings, new_holding], ignore_index=True)
        self.today_buying = pd.concat([self.today_buying, new_holding], ignore_index=True)
        self.update_total_assets()
        self.slippage -= data['成交额'] - total_value
    
    def sell_holding(self, data):
        contract_name = data['合约'].strip()
        total_value = data['手数'] * data['收盘价'] * data['合约乘数']
        self.slippage += data['成交额'] - total_value
        new_holding = pd.DataFrame([{
            '品种': data['品种'],
            '合约': data['合约'],
            '手数': data['手数'],
            '成交价': data['成交价'],
            '收盘价': data['收盘价'],
            '成交额': data['成交额'],
            '合约乘数': data['合约乘数'],
            '收盘总价': total_value,
            '操作误差': data['成交额'] - total_value
        }])
        self.today_selling = pd.concat([self.today_selling, new_holding], ignore_index=True)
        var_diff = pd.DataFrame([{
            '日期': data['成交日期'],
            '品种': data['品种'],
            '合约': data['合约'],
            '操作误差': data['成交额'] - total_value
        }])
        self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
        if contract_name in self.holdings['合约'].values:
            sell_pnl = pd.DataFrame([{ 
                '日期': data['成交日期'],
                '品种': data['品种'],
                '合约': data['合约'],
                '手续费': data['手续费'],
                '操作误差': data['成交额'] - total_value,
                '收益变化': 0,
                '变化来源': '卖出'
            }])
            self.pnl_df = pd.concat([self.pnl_df, sell_pnl], ignore_index=True)
            remaining_to_sell = data['手数']
            for idx in self.holdings[self.holdings['合约'] == contract_name].index:
                if remaining_to_sell <= 0:
                    break

                holding_row = self.holdings.loc[idx]
                if holding_row['手数'] > remaining_to_sell:
                    # 部分卖出
                    self.holdings.at[idx, '手数'] -= remaining_to_sell
                    self.holdings.at[idx, '收盘总价'] = self.holdings.at[idx, '手数'] * holding_row['收盘价'] * holding_row['合约乘数']
                    remaining_to_sell = 0
                else:
                    # 完全卖出该行
                    remaining_to_sell -= holding_row['手数']
                    self.holdings = self.holdings.drop(idx)
            
            self.holdings.reset_index(drop=True, inplace=True)
            self.cash = self.cash + total_value - data['手续费']
            self.update_total_assets()
        else:
            print("Error: Holding not found")
    def update_holding(self, data):
        #这里的data变成了一个单个的合约名，而非带着这个名字的dataframe了
        self.today_buying = self.today_buying.drop(self.today_buying.index)
        self.today_selling = self.today_selling.drop(self.today_selling.index)
        contract_name = data['合约']
        if contract_name in self.holdings['合约'].values:
            matching_indicies = self.holdings[self.holdings['合约'] == contract_name].index
            # print(f"找到{contract_name}的持仓，索引为{matching_indicies}")
            for idx in matching_indicies:
                history_df = data_check.data[contract_name]
                old_value = self.holdings.at[idx, '收盘总价']
                price = history_df.loc[history_df['Date'] == self.date, 'CLOSE']                
                self.holdings.at[idx, '收盘价'] = float(price.iloc[0])
                self.holdings.at[idx, '收盘总价'] = self.holdings.at[idx, '手数'] * price * data['合约乘数']
                value_difference = self.holdings.at[idx, '收盘总价'] - old_value
                update_pnl = pd.DataFrame([{ 
                    '日期': self.date,
                    '品种': data['品种'],
                    '合约': contract_name,
                    '手续费': 0,
                    '操作误差': 0,
                    '收益变化': float(value_difference),
                    '变化来源': '价格变动'
                }])
                self.pnl_df = pd.concat([self.pnl_df, update_pnl], ignore_index=True)
                # 操作误差清零
                self.holdings.at[idx, '操作误差'] = 0
            self.update_total_assets()
        else:
            print('Error: 未找到该合约，更新价格失败')

    def update_total_assets(self):
        self.holdings['收盘总价'] = self.holdings['手数'] * self.holdings['收盘价'] * self.holdings['合约乘数']
        self.close_holding_value = self.holdings['收盘总价'].sum()
        self.actual_holding_value = self.close_holding_value + self.slippage
        self.total_assets = float(self.cash) + float(self.close_holding_value)
        # self.calculate_today_difference()
    # 新增计算持仓差价函数
    def calculate_today_difference(self):
        self.difference = self.total_assets - self.yesterday_total_assets
           
    def __repr__(self):
        buying_info = f"\n今日买入:\n{self.today_buying}" if not self.today_buying.empty else ""
        selling_info = f"\n今日卖出:\n{self.today_selling}" if not self.today_selling.empty else ""
        
        return (f"\n日期: {self.date}, 现金: {round(self.cash,2)}, 总资产: {round(self.total_assets,2)}"
                f"{buying_info}"
                f"{selling_info}"
                f"\n持仓:\n{self.holdings}"
                f"\n今日收益（不包含操作误差）: {round(self.difference,2)}, 今日操作误差汇总:{round(self.slippage,2)}")

# 因为有些地方dtypes自动转化格式失败，所以添加此判断函数作为简单分类
def convert_column_type(column):
    try:
        # 尝试转换为整数
        column = column.astype('int64')
    except ValueError:
        try:
            # 尝试转换为浮点数
            column = column.astype('float64')
        except:
            pass
    return column

# 阅读每个月的结算单（文本文档格式），转化为dataframe并识别正确的数据类型
def read_statement(name):
    with open(name,'r',encoding='gbk') as file:
        lines = file.readlines()
    record_lines = []
    start_extracting = False
    # 抓取文件中的成交记录，在平仓明细出现前中止
    for line in lines:
        if "成交记录 Transaction Record" in line:
            start_extracting = True
            continue
        elif "开---Open" in line:
            break
        if start_extracting:
            record_lines.append(line.strip())
    # 如果没有找到该月/日有成交记录，返回None
    if not start_extracting:
        print(f"未找到{name}中的成交记录")
        return None
    # 删除分割线
    separator_positions = [i for i, line in enumerate(record_lines) if '-----------' in line]
    # 删除后续dataframe不需要的内容，进行数据预处理
    if len(separator_positions) > 3:
        record_lines = record_lines[:separator_positions[-2]]
    record_lines = [line for line in record_lines if '-----------' not in line ]
    # 数据预处理
    month_statement = [line.split("|") for line in record_lines]
    month_statement = [line for line in month_statement if line != ['']]
    month_statement = [[cell.strip() for cell in line] for line in month_statement]

    # 因为成交价格csv里没有英文列名，此处删除了英文列名，仅保留中文列名进行索引
    month_statement.pop(1)
    df_month = pd.DataFrame(month_statement[1:], columns= month_statement[0])
    # print(df_month)
    # 这里用dtypes好像不太行，所以手动添加判断规则 convert_column_type
    for col in df_month.columns:
        df_month[col] = convert_column_type(df_month[col])
    # 转化日期
    df_month["成交日期"] = pd.to_datetime(df_month["成交日期"], format='%Y%m%d')
    return df_month

april_statement = read_statement('结算单_202404.txt')
may_statement = read_statement('结算单_202405.txt')

statements = {}

# 获取当前目录中的所有文件和目录名
for filename in os.listdir('.'):
    # 如果文件名匹配正则表达式（以‘结算单’开头并以‘.txt’结尾）
    if re.match(r'^结算单_\d+\.txt$', filename):
        # 读取并存储结算单
        statements[filename] = read_statement(filename)
        
# 假设statements字典已经按照之前的步骤填充好了
# 创建一个空列表来存储所有的DataFrame
dataframes = []

# 遍历statements字典，将每个DataFrame添加到列表中
for filename in statements:
    # 避免添加空DataFrame
    if statements[filename] is not None:
        dataframes.append(statements[filename])

# 使用pandas.concat合并所有的DataFrame
big_dataframe = pd.concat(dataframes, ignore_index=True)
big_dataframe.to_csv('4-7月结算单.csv', encoding='gbk')
# 现在big_dataframe包含了所有文件的数据

data_check = DataCheck('data.pickle',r"\\xw\\SHARE\\lrs\\lrs_data")
# print(data_check.data['IF2407'])

# comm_df: 所有商品期货（非期权）的数据
comm_df = pd.DataFrame()
    
option_df = big_dataframe[big_dataframe['品种'].str.contains('期权')].reset_index(drop=True)
comm_df = big_dataframe[~big_dataframe['品种'].str.contains('期权')].reset_index(drop=True)

comm_df.insert(comm_df.shape[1], "收盘价", None)

# 修正合约名
def name_correction(df):
    for index, row in df.iterrows():
        correct_name = row['合约'].strip().upper()
        df.at[index, '合约'] = correct_name
        if re.match(r'^[A-Z]{1,2}\d{3}$', row['合约']):
            name = row['合约']
            # 针对TA409这种情况(前序英文字母长度为2)，将409变成2409
            if len(name) == 5:
                name = name[:2] + '2' + name[2:]
            # 针对T409这种情况（前序英文字母长度为1），将409变成2409 
            elif len(name) == 4:
                name = name[:1] + '2' + name[1:]
            df.at[index, '合约'] = name
    return df

comm_df = name_correction(comm_df)

# 只用于计算商品成交价(comm_df)，不用于计算期权(option_df)
def add_price(row):
    search_date = row['成交日期']
    target_name = row['合约']
    searching_df = data_check.get_dataframe(target_name)
    price = data_check.find_date_price(searching_df, search_date)
    row['收盘价'] = price
    return row
comm_df = comm_df.apply(add_price, axis = 1)
comm_df.insert(comm_df.shape[1], "合约乘数", None)
# 计算合约乘数，目前只有IF/IC供选择，后续可以增加
def add_multiple_index(row):
    row['合约乘数'] = row['成交额'] / row['手数'] / row['成交价']
    return row

comm_df = comm_df.apply(add_multiple_index, axis = 1)
option_df = option_df.apply(add_multiple_index, axis = 1)
option_df.to_csv('option_df.csv', encoding='gbk')
comm_df.to_csv('comm_df.csv', encoding='gbk')
# 设置初始现金
initial_cash = 50000000

filename = 'daily_statement.pickle'
def check_or_create(filename):
    if not os.path.exists(filename):
        print(f"{filename} 不存在，正在初始化...")
        with open(filename, 'wb') as file:
            pickle.dump({}, file)
        print(f"{filename} 已创建。")
    else:
        print(f"{filename} 已存在。")
        
# 提取交易表里的开始日和截止日，然后在价目单里索取在这之间的交易
def get_period(merged_data):
    start_date = merged_data['成交日期'].min()
    end_date = merged_data['成交日期'].max()
    sample_biaodi = merged_data['合约'].iloc[0]
    # print(f'start date : {start_date}, end date {end_date}, biaodi {sample_biaodi}')
    searching_df = data_check.get_dataframe(sample_biaodi)
    trading_date = searching_df[(searching_df['Date'] >= start_date) & (searching_df['Date'] <= end_date)]['Date']
    trading_date = trading_date.reset_index(drop = True)
    return trading_date
total_portfolio = {}
tradingday = get_period(comm_df)
total_comm_var_diff = pd.DataFrame(columns = ['合约','操作误差'])
holding_diff = pd.DataFrame()
pnl_total = pd.DataFrame()
for i, date in enumerate(tradingday):
    # 检查是不是第一天，如果是的话创立一个空Daily Portfolio，如果不是则copy昨天的DailyPortfolio
    if i == 0:
        total_portfolio[date] = DailyPortfolio(date, initial_cash)
    else:
        previous_date = tradingday[i-1]
        previous_portfolio = total_portfolio[previous_date]
        new_portfolio = DailyPortfolio(date, previous_portfolio.cash)
        new_portfolio.holdings = previous_portfolio.holdings.copy()
        new_portfolio.total_assets = previous_portfolio.total_assets    
        total_portfolio[date] = new_portfolio
    # 检查该交易日有没有在comm_df中出现，如果有的话识别是买/卖，没有的话正常更新
    if date in comm_df['成交日期'].values:
        transcations = comm_df[comm_df['成交日期'] == date]
        # 保存历史总资产
        total_portfolio[date].yesterday_total_assets = total_portfolio[date].total_assets
        for contract_name in total_portfolio[date].holdings['合约']:
            # 直接调用 update_holding 函数，这里data通过合约名字contract加入新的名字
            data = {'品种': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '品种'].values[0], '合约': contract_name, '手数': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '手数'].values[0], '收盘价': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '收盘价'].values[0], '合约乘数': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '合约乘数'].values[0]}
            total_portfolio[date].update_holding(data)    
        # print(f"日期{date}的交易记录：\n{transcations}")
        for _, transcation in transcations.iterrows():
            if transcation['买/卖'] == '买':
                total_portfolio[date].buy_holding(transcation)
            elif transcation['买/卖'] == '卖':
                total_portfolio[date].sell_holding(transcation)
        total_portfolio[date].calculate_today_difference()
    else:
        total_portfolio[date].yesterday_total_assets = total_portfolio[date].total_assets

        # print(f"这里的合约有:{total_portfolio[date].holdings['合约']}")
        for contract_name in total_portfolio[date].holdings['合约']:
            # 直接调用 update_holding 函数，这里data通过合约名字contract加入新的名字
            data = {'品种': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '品种'].values[0],'合约': contract_name, '手数': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '手数'].values[0], '收盘价': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '收盘价'].values[0], '合约乘数': total_portfolio[date].holdings.loc[total_portfolio[date].holdings['合约'] == contract_name, '合约乘数'].values[0]}
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
pnl_total.to_csv('pnl_total.csv', encoding='gbk') 
# pnl_group_df = pnl_total.groupby(['日期'],['合约'],['变化来源'], as_index=False).sum()
pnl_group_df = pnl_total.groupby(['日期', '品种', '变化来源'], as_index=False)[['手续费', '操作误差', '收益变化']].sum()
pnl_group_df['手续费'] = -pnl_group_df['手续费']
pnl_group_df.to_csv('pnl_group_df.csv', encoding='gbk')
  
holding_diff['sum_slippage'] = holding_diff['slippage'].cumsum()
# holding_diff.to_csv('holding_diff.csv', encoding='gbk')
total_comm_var_diff.to_csv('total_comm_var_diff.csv', encoding='gbk')
# 根据合约名合并并汇总操作误差
grouped_df_name = total_comm_var_diff.groupby('合约')['操作误差'].sum().reset_index()
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

contracts = pnl_group_df['品种'].unique()
for contract in contracts:
    contract_df = pnl_group_df[pnl_group_df['品种'] == contract]
    contract_cumsum = contract_df.groupby('日期')['收益变化'].sum().reset_index()
    contract_cumsum['累计收益变化'] = contract_cumsum['收益变化'].cumsum()
    sns.lineplot(data=contract_cumsum, x='日期', y='累计收益变化', linestyle='--', label=f'{contract} 累计收益变化')

plt.title('各品种收益变化')
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


# 使用 seaborn 生成条形统计图
plt.figure(figsize=(12, 6))
barplot = sns.barplot(x='合约', y='操作误差', data=grouped_df_name)

# 旋转 x 轴标签以防重叠
barplot.set_xticklabels(barplot.get_xticklabels(), rotation=45, horizontalalignment='right')

# 添加数值标签
for index, row in grouped_df_name.iterrows():
    barplot.text(index, row['操作误差'], round(row['操作误差'], 2), color='black', ha="center")

# 添加图表标题和标签
plt.title('合约名与操作误差汇总条形图')
plt.xlabel('合约')
plt.ylabel('操作误差汇总')

# 显示图表
plt.show()

with open('output.txt', 'w') as file:
    for date, portfolio in total_portfolio.items():
        file.write(f"{portfolio}\n")

def option_calculation(option_df):
    temp = {}
    for index, row in option_df.iterrows():
        if row['品种'] not in temp:
            temp[row['品种']] = pd.DataFrame(columns=['成交日期', '权利金收支', '手数', '累计权利金收支', '累计持仓手数', '累计手续费'])
            new_row = pd.DataFrame([{
                '成交日期': row['成交日期'], 
                '手数': row['手数'], 
                '权利金收支': 0, 
                '累计权利金收支': 0, 
                '累计持仓手数': 0, 
                '累计手续费': 0,
                '合约乘数': row['合约乘数']
            }])
        else:
            new_row = pd.DataFrame([{
                '成交日期': row['成交日期'], 
                '手数': row['手数'], 
                '权利金收支': 0, 
                '累计权利金收支': temp[row['品种']]['累计权利金收支'].iloc[-1], 
                '累计持仓手数': temp[row['品种']]['累计持仓手数'].iloc[-1], 
                '累计手续费': temp[row['品种']]['累计手续费'].iloc[-1],
                '合约乘数': temp[row['品种']]['合约乘数'].iloc[-1]
            }])
        if row['买/卖'] == '买':
            new_row.at[0, '权利金收支'] = row['手数'] * row['成交价'] * row['合约乘数'] * -1
            new_row.at[0, '累计持仓手数'] = new_row.at[0, '累计持仓手数'] + row['手数']
        elif row['买/卖'] == '卖':
            new_row['手数'] = new_row['手数'] * -1
            new_row.at[0, '权利金收支'] = row['手数'] * row['成交价'] * row['合约乘数']
            new_row.at[0, '累计持仓手数'] = new_row.at[0, '累计持仓手数'] - row['手数']
        new_row.at[0, '累计权利金收支'] = new_row.at[0, '累计权利金收支'] + new_row.at[0, '权利金收支'] - row['手续费']
        new_row.at[0, '累计手续费'] = new_row.at[0, '累计手续费'] + row['手续费']
        temp[row['品种']] = pd.concat([temp[row['品种']], new_row], ignore_index=True)
    return temp

option_result = option_calculation(option_df)
for key, value in option_result.items():
    value.to_csv(f'{key}_option.csv', encoding='gbk')
# 数据可视化

for key, df in option_result.items():
    df_last = df.groupby('成交日期').last().reset_index()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12), sharex=True)
    
    # ax1 权利金收支折线图
    ax1.plot(df_last['成交日期'], df_last['累计权利金收支'], color ='#1f77b4', label='累计权利金收支', marker='o', linestyle='-')
    ax1.set_xlabel('成交日期', fontsize=12)
    ax1.set_ylabel('累计权利金收支', color='#1f77b4', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#1f77b4')
    
    for i, v in enumerate(df_last['累计权利金收支']):
        ax1.text(df_last['成交日期'][i], v, round(v, 2), color ='#1f77b4', ha='right', va='bottom', fontsize=8)
    
    # ax2 累计持仓手数柱状图
    ax2.plot(df_last['成交日期'], df_last['累计持仓手数'], color='#ff7f0e', alpha=0.6, label='累计持仓手数')
    ax2.set_xlabel('成交日期', fontsize=12)
    ax2.set_ylabel('累计持仓手数', color='#ff7f0e', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#ff7f0e')
    
    for i, v in enumerate(df_last['累计持仓手数']):
        ax2.text(df_last['成交日期'][i], v, round(v, 2), color ='#ff7f0e', ha='right', va='bottom', fontsize=8)
    
    ax1.set_xticks(df_last['成交日期'])
    ax1.set_xticklabels(df_last['成交日期'].dt.strftime('%Y-%m-%d'), rotation=45, ha='right', fontsize=10)

    plt.suptitle(f'{key} 持仓明细', fontsize=14)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.legend(loc='upper left', bbox_to_anchor=(0.1, 0.9), fontsize=10)
    plt.show()