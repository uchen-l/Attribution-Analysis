from datetime import datetime
import pandas as pd 
import re
from tqdm import tqdm
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class DailyPortfolio:
    def __init__(self, date, cash, history_price):
        self.date = date
        self.history_price = history_price
        self.holdings = pd.DataFrame (columns = ['合约名称','合约编码','成交数量','成交价格','收盘价','成交金额','合约乘数','收盘总价','操作误差'])
        self.today_buying = pd.DataFrame(columns = ['合约名称','合约编码','成交数量','成交价格','收盘价','成交金额','合约乘数','收盘总价','操作误差'])
        self.today_selling = pd.DataFrame(columns = ['合约名称','合约编码','成交数量','成交价格','收盘价','成交金额','合约乘数','收盘总价','操作误差'])
        self.comm_var_diff = pd.DataFrame(columns = ['日期','合约名称','合约编码','操作误差'])
        self.pnl_df = pd.DataFrame(columns = ['日期','合约名称','合约编码','操作误差','收益变化','变化来源'])
        self.virtual_df = pd.DataFrame(columns = ['成交日期','合约名称','合约编码','等权面值','今日等权手数','昨日等权手数','手数变化'])
        self.cash = cash
        # 收盘价计算的总资产
        self.total_assets = cash
        # 实际总资产
        self.actual_total_assets = cash
        # 储存昨日收益
        self.yesterday_total_assets = self.total_assets
        # 今日收盘价持仓额
        self.close_holding_value = 0
        # 今日实际持仓额
        self.actual_holding_value = 0
        self.difference = 0
        # 操作误差
        self.slippage = 0
    
    # 买入开仓    
    def buy_holding_open(self,data):
        # 收盘价计算的总资产
        total_value = data['成交数量'] * data['closePrice'] * data['合约乘数']
        self.cash = self.cash - total_value
        new_holding = pd.DataFrame([{
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '成交数量': data['成交数量'],
            '成交价格': data['成交价格'],
            '收盘价': data['closePrice'],
            '成交金额': data['成交金额'],
            '合约乘数': data['合约乘数'],
            '收盘总价': total_value,
            '操作误差': -(data['成交金额'] - total_value)
        }])
        var_diff = pd.DataFrame([{
            '日期': data['成交日期'],
            # '品种': data['品种'],
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '操作误差': -(data['成交金额'] - total_value)
        }])
        buy_pnl = pd.DataFrame([{ 
            '日期': data['成交日期'],
            # '品种': data['品种'],
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '操作误差': -(data['成交金额'] - total_value),
            '收益变化': 0,
            '变化来源': '买入开仓'
        }])
        self.pnl_df = pd.concat([self.pnl_df, buy_pnl], ignore_index=True)
        self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
        self.holdings = pd.concat([self.holdings, new_holding], ignore_index=True)
        self.today_buying = pd.concat([self.today_buying, new_holding], ignore_index=True)
        self.update_total_assets()
        self.slippage -= data['成交金额'] - total_value
    
    
    # 买入平仓 
    def buy_holding_close(self, data):
        contract_name = data['合约编码']
        total_value = data['成交数量'] * data['closePrice'] * data['合约乘数']
        self.slippage += total_value - data['成交金额']
        new_holding = pd.DataFrame([{
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '成交数量': data['成交数量'],  # 买入数量为负?
            '成交价格': data['成交价格'],
            '收盘价': data['closePrice'],
            '成交金额': data['成交金额'],
            '合约乘数': data['合约乘数'],
            '收盘总价': total_value,
            '操作误差': total_value - data['成交金额']
        }])
        self.today_buying = pd.concat([self.today_buying, new_holding], ignore_index=True)
        var_diff = pd.DataFrame([{
            '日期': data['成交日期'],
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '操作误差': total_value - data['成交金额']
        }])
        self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
        if contract_name in self.holdings['合约编码'].values:
            buy_pnl = pd.DataFrame([{ 
                '日期': data['成交日期'],
                '合约名称': data['合约名称'],
                '合约编码': data['合约编码'],
                '操作误差': total_value - data['成交金额'],
                '收益变化': 0,
                '变化来源': '买入平仓'
            }])
            self.pnl_df = pd.concat([self.pnl_df, buy_pnl], ignore_index=True)
            remaining_to_buy = data['成交数量']
            for idx in self.holdings[self.holdings['合约编码'] == contract_name].index:
                if remaining_to_buy <= 0:
                    break
                holding_row = self.holdings.loc[idx]
                if abs(holding_row['成交数量']) <= abs(remaining_to_buy):
                    # 完全买入该行
                    remaining_to_buy += holding_row['成交数量']
                    self.holdings = self.holdings.drop(idx)
                else:
                    # 部分买入
                    self.holdings.at[idx, '成交数量'] += remaining_to_buy
                    self.holdings.at[idx, '收盘总价'] = self.holdings.at[idx, '成交数量'] * holding_row['收盘总价'] * holding_row['合约乘数']
                    remaining_to_buy = 0
            self.holdings.reset_index(drop=True, inplace=True)
            self.cash = self.cash - total_value
            self.update_total_assets()
        else:
            print(f"Error: Holding not found - buy_holding_close{self.date} with name {contract_name}")
    
    # 卖出平仓
    def sell_holding_close(self, data):
        # contract_name = data['合约'].strip()
        contract_name = data['合约编码']
        total_value = data['成交数量'] * data['closePrice'] * data['合约乘数']
        self.slippage += data['成交金额'] - total_value
        new_holding = pd.DataFrame([{
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '成交数量': -data['成交数量'], # 卖出数量为负
            '成交价格': data['成交价格'],
            '收盘价': data['closePrice'],
            '成交金额': data['成交金额'],
            '合约乘数': data['合约乘数'],
            '收盘总价': total_value,
            '操作误差': data['成交金额'] - total_value
        }])
        self.today_selling = pd.concat([self.today_selling, new_holding], ignore_index=True)
        var_diff = pd.DataFrame([{
            '日期': data['成交日期'],
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '操作误差': data['成交金额'] - total_value
        }])
        self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
        if contract_name in self.holdings['合约编码'].values:
            sell_pnl = pd.DataFrame([{ 
                '日期': data['成交日期'],
                '合约名称': data['合约名称'],
                '合约编码': data['合约编码'],
                '操作误差': data['成交金额'] - total_value,
                '收益变化': 0,
                '变化来源': '卖出平仓'
            }])
            self.pnl_df = pd.concat([self.pnl_df, sell_pnl], ignore_index=True)
            remaining_to_sell = data['成交数量']
            for idx in self.holdings[self.holdings['合约编码'] == contract_name].index:
                if remaining_to_sell <= 0:
                    break

                holding_row = self.holdings.loc[idx]
                if holding_row['成交数量'] > remaining_to_sell:
                    # 部分卖出
                    self.holdings.at[idx, '成交数量'] -= remaining_to_sell
                    self.holdings.at[idx, '收盘总价'] = self.holdings.at[idx, '成交数量'] * holding_row['收盘总价'] * holding_row['合约乘数']
                    remaining_to_sell = 0
                else:
                    # 完全卖出该行
                    remaining_to_sell -= holding_row['成交数量']
                    self.holdings = self.holdings.drop(idx)
            
            self.holdings.reset_index(drop=True, inplace=True)
            self.cash = self.cash + total_value
            self.update_total_assets()
        else:
            print(f"Error: Holding not found - sell_holding_close at {self.date}")
    
    # 卖出开仓
    def sell_holding_open(self, data):
    # 收盘价计算的总资产
        total_value = data['成交数量'] * data['closePrice'] * data['合约乘数']
        self.cash = self.cash + total_value
        new_holding = pd.DataFrame([{
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '成交数量': -data['成交数量'],  # 卖出数量为负
            '成交价格': data['成交价格'],
            '收盘价': data['closePrice'],
            '成交金额': data['成交金额'],
            '合约乘数': data['合约乘数'],
            '收盘总价': total_value,
            '操作误差': data['成交金额'] - total_value
        }])
        var_diff = pd.DataFrame([{
            '日期': data['成交日期'],
            # '品种': data['品种'],
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '操作误差': data['成交金额'] - total_value
        }])
        sell_pnl = pd.DataFrame([{
            '日期': data['成交日期'],
            # '品种': data['品种'],
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '操作误差': data['成交金额'] - total_value,
            '收益变化': 0,
            '变化来源': '卖出开仓'
        }])
        self.pnl_df = pd.concat([self.pnl_df, sell_pnl], ignore_index=True)
        self.comm_var_diff = pd.concat([self.comm_var_diff, var_diff], ignore_index=True)
        self.holdings = pd.concat([self.holdings, new_holding], ignore_index=True)
        self.today_selling = pd.concat([self.today_selling, new_holding], ignore_index=True)
        self.update_total_assets()
        self.slippage += data['成交金额'] - total_value   
    
    def update_holding(self, data):
        self.today_buying = self.today_buying.drop(self.today_buying.index)
        self.today_selling = self.today_selling.drop(self.today_selling.index)
        contract_name = data['合约编码']
        if contract_name in self.holdings['合约编码'].values:
            matching_indicies = self.holdings[self.holdings['合约编码'] == contract_name].index
            # print(f"找到{contract_name}的持仓，索引为{matching_indicies}")
            for idx in matching_indicies:
                history_df = self.history_price[contract_name]
                old_value = self.holdings.at[idx, '收盘总价']
                # Date变成tradeDate
                price = history_df.loc[history_df['tradeDate'] == self.date, 'closePrice']               
                self.holdings.at[idx, '收盘价'] = float(price.iloc[0])
                self.holdings.at[idx, '收盘总价'] = self.holdings.at[idx, '成交数量'] * price * data['合约乘数']
                value_difference = self.holdings.at[idx, '收盘总价'] - old_value
                update_pnl = pd.DataFrame([{ 
                    '日期': self.date,
                    # '品种': data['品种'],
                    '合约名称': data['合约名称'],
                    '合约编码': contract_name,
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
        self.holdings['收盘总价'] = self.holdings['成交数量'] * self.holdings['收盘价'] * self.holdings['合约乘数']
        self.close_holding_value = self.holdings['收盘总价'].sum()
        self.actual_holding_value = self.close_holding_value + self.slippage
        self.total_assets = float(self.cash) + float(self.close_holding_value)
        # self.calculate_today_difference()
    # 新增计算持仓差价函数
    def calculate_today_difference(self):
        self.difference = self.total_assets - self.yesterday_total_assets
    
    # 新增等权面值持仓手数计算
    def calculate_virtual_holding(self, yesterday_virtual_holding):
        # 这里直接拿收盘价算的总价 如果是按照实际交易时的持仓额算的话，需要改成actual_holding_value，仅在真实产生交易时候会发生区别
        total_value = self.close_holding_value
        unique_contract_list = self.holdings['合约编码'].unique()
        num_of_contract = len(unique_contract_list)
        
        # 等权面值计算
        average_value = total_value / num_of_contract
        
        # 等权手数计算
        # lots = round(average_value / self.holdings['合约乘数'] / self.holdings['收盘价'])
        
        # 初始化虚拟持仓的 DataFrame
        self.virtual_df = pd.DataFrame(columns=['成交日期', '合约名称', '合约编码', '等权面值', '今日等权手数', '昨日等权手数', '手数变化'])
        
        # 遍历今天的合约编码列表
        for contract in unique_contract_list:
            # yesterday_lots = 0 if yesterday_virtual_holding == 0 or contract not in yesterday_virtual_holding['合约编码'].values else yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '今日等权手数'].values[0]
            
            if yesterday_virtual_holding.empty or contract not in yesterday_virtual_holding['合约编码'].values:
                yesterday_lots = 0
            else:
                yesterday_lots = yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '今日等权手数'].values[0]
                
            lots = round(average_value / self.holdings.loc[self.holdings['合约编码'] == contract, '合约乘数'].values[0] / self.holdings.loc[self.holdings['合约编码'] == contract, '收盘价'].values[0])
            single_virtual_df = pd.DataFrame([{
                '成交日期': self.date,
                '合约名称': self.holdings.loc[self.holdings['合约编码'] == contract, '合约名称'].values[0],
                '合约编码': contract,
                '等权面值': average_value,
                '今日等权手数': lots,
                '昨日等权手数': yesterday_lots,
                '手数变化': lots - yesterday_lots,
                '收盘价':self.holdings.loc[self.holdings['合约编码'] == contract, '收盘价'].values[0],
                '合约乘数':self.holdings.loc[self.holdings['合约编码'] == contract, '合约乘数'].values[0],
                '收盘总价':self.holdings.loc[self.holdings['合约编码'] == contract, '收盘价'].values[0] * (lots - yesterday_lots) * self.holdings.loc[self.holdings['合约编码'] == contract, '合约乘数'].values[0],
            }])
            
            self.virtual_df = pd.concat([self.virtual_df, single_virtual_df], ignore_index=True)
        
        # 遍历昨天的合约编码列表，查找今天已经不再持有的合约
        if not yesterday_virtual_holding.empty:
            for contract in yesterday_virtual_holding['合约编码'].unique():
                # 确保是昨天有今天无的合约，并且在昨天的等权手数不为0，这一步的目的是防止出现一直持有的合约在昨天卖出后今天还在持有的情况
                if contract not in unique_contract_list and yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '今日等权手数'].values[0] != 0:
                    # 获取昨天的等权手数
                    yesterday_lots = yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '今日等权手数'].values[0]
                    
                    # 创建一个记录已卖出的合约信息的 DataFrame
                    single_virtual_df = pd.DataFrame([{
                        '成交日期': self.date,
                        '合约名称': yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '合约名称'].values[0],
                        '合约编码': contract,
                        '等权面值': 0,  # 今天已经没有持仓，等权面值为0
                        '今日等权手数': 0,  # 今天已经没有持仓，等权手数为0
                        '昨日等权手数': yesterday_lots,
                        '手数变化': 0 - yesterday_lots,  # 手数变化为负
                        '收盘价':yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '收盘价'].values[0],
                        '合约乘数':yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '合约乘数'].values[0],
                        '收盘总价':yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '收盘价'].values[0] * (0 - yesterday_lots) * yesterday_virtual_holding.loc[yesterday_virtual_holding['合约编码'] == contract, '合约乘数'].values[0],
                    }])
                    
                    self.virtual_df = pd.concat([self.virtual_df, single_virtual_df], ignore_index=True)
                
        return self.virtual_df
           
    def __repr__(self):
        buying_info = f"\n今日买入:\n{self.today_buying}" if not self.today_buying.empty else ""
        selling_info = f"\n今日卖出:\n{self.today_selling}" if not self.today_selling.empty else ""
        
        return (f"\n日期: {self.date}, 现金: {round(self.cash,2)}, 总资产: {round(self.total_assets,2)}"
                f"{buying_info}"
                f"{selling_info}"
                f"\n持仓:\n{self.holdings}"
                # f"\n收盘时持仓总额:{round(self.close_holding_value,2)}\n"
                f"\n今日收益（不包含操作误差）: {round(self.difference,2)}, 今日操作误差汇总:{round(self.slippage,2)}")
        
        
# 仅用于虚拟持仓 根据手数变化设置开平仓
# 添加并设置'开平'列
def set_kaiping(row):
    if row['买卖'] == '买':
        return '平' if row['昨日等权手数'] < 0 else '开'
    elif row['买卖'] == '卖':
        return '平' if row['昨日等权手数'] > 0 else '开'

def portfolio_performance_analyzer(tradingday, initial_cash, history_price, trading_data):
    total_portfolio = {}
    # total_virtual_holding = {}
    # virtual_holding_summary_table = pd.DataFrame()
    total_comm_var_diff = pd.DataFrame(columns = ['合约','操作误差'])
    holding_diff = pd.DataFrame()
    pnl_total = pd.DataFrame()
    

    for i, date in enumerate(tqdm(tradingday)):
        # 检查是不是第一天，如果是的话创立一个空Daily Portfolio，如果不是则copy昨天的DailyPortfolio
        if i == 0:
            total_portfolio[date] = DailyPortfolio(date, initial_cash, history_price)
        else:
            previous_date = tradingday[i-1]
            previous_portfolio = total_portfolio[previous_date]
            new_portfolio = DailyPortfolio(date, previous_portfolio.cash, history_price)
            new_portfolio.holdings = previous_portfolio.holdings.copy()
            new_portfolio.total_assets = previous_portfolio.total_assets    
            total_portfolio[date] = new_portfolio
            
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
        # 检查该交易日有没有在trading_data中出现，如果有的话识别是买/卖，没有的话正常更新
        if date in trading_data['成交日期'].values:
            transcations = trading_data[trading_data['成交日期'] == date]
            # 保存历史总资产
            # total_portfolio[date].yesterday_total_assets = total_portfolio[date].total_assets
            buy_sell_column = next((col for col in transcations.columns if re.match(r'买/?卖', col)), None)
            for _, transcation in transcations.iterrows():
                if re.search(r'买', transcation[buy_sell_column]):
                    if re.search(r'开', transcation['开平']):
                        total_portfolio[date].buy_holding_open(transcation)
                    elif re.search(r'平', transcation['开平']):
                        total_portfolio[date].buy_holding_close(transcation)
                    else:
                        raise Exception(f"未知的开平类型{transcation['开平']}")
                elif re.search(r'卖', transcation[buy_sell_column]):
                    if re.search(r'开', transcation['开平']):
                        total_portfolio[date].sell_holding_open(transcation)
                    elif re.search(r'平', transcation['开平']):
                        total_portfolio[date].sell_holding_close(transcation)
                    else:
                        raise Exception(f"未知的开平类型{transcation['开平']}")
                else:
                    raise Exception(f"未知的买卖类型{transcation[buy_sell_column]}")
            # total_portfolio[date].calculate_today_difference()
        # else:
            # total_portfolio[date].yesterday_total_assets = total_portfolio[date].total_assets
            # total_portfolio[date].calculate_today_difference()
        total_portfolio[date].calculate_today_difference()
        holding_diff = holding_diff.append({
            '日期': date,
            'slippage': total_portfolio[date].slippage,
            'close_holding_value': total_portfolio[date].close_holding_value,
            'actual_holding_value': total_portfolio[date].actual_holding_value
        }, ignore_index=True)
        total_comm_var_diff = pd.concat([total_comm_var_diff, total_portfolio[date].comm_var_diff], ignore_index=True)
        pnl_total = pd.concat([pnl_total, total_portfolio[date].pnl_df], ignore_index=True)
        
        # 文件输出检查
        # buying_info = f"\n今日买入:\n{total_portfolio[date].today_buying}" if not total_portfolio[date].today_buying.empty else ""
        # selling_info = f"\n今日卖出:\n{total_portfolio[date].today_selling}" if not total_portfolio[date].today_selling.empty else ""
        
        # print(f"\n日期: {total_portfolio[date].date}, 现金: {round(total_portfolio[date].cash,2)}, 总资产: {round(total_portfolio[date].total_assets,2)}"
        #         f"{buying_info}"
        #         f"{selling_info}"
        #         f"\n持仓:\n{total_portfolio[date].holdings}"
        #         f"\n收盘时持仓总额:{round(total_portfolio[date].close_holding_value,2)}\n"
        #         f"\n今日收益（不包含操作误差）: {round(total_portfolio[date].difference,2)}, 今日操作误差汇总:{round(total_portfolio[date].slippage,2)}")
        
    
    # 测试：写入每天的portfolio到汇总txt文件
    for portfolio in total_portfolio.items():
        with open(f'portfolio_test.txt', 'a') as f:
            f.write(str(portfolio))
    # 这里如果要计算虚拟持仓的话就导出，如果虚拟持仓计算已经替换原有df就comment掉        
    
    return total_comm_var_diff,pnl_total,holding_diff