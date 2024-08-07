from datetime import datetime
import pandas as pd 

class DailyPortfolio:
    def __init__(self, date, cash, history_price):
        self.date = date
        self.history_price = history_price
        self.holdings = pd.DataFrame (columns = ['合约名称','合约编码','成交数量','成交价格','收盘价','成交金额','合约乘数','收盘总价','操作误差'])
        self.today_buying = pd.DataFrame(columns = ['合约名称','合约编码','成交数量','成交价格','收盘价','成交金额','合约乘数','收盘总价','操作误差'])
        self.today_selling = pd.DataFrame(columns = ['合约名称','合约编码','成交数量','成交价格','收盘价','成交金额','合约乘数','收盘总价','操作误差'])
        self.comm_var_diff = pd.DataFrame(columns = ['日期','合约名称','合约编码','操作误差'])
        self.pnl_df = pd.DataFrame(columns = ['日期','合约名称','合约编码','操作误差','收益变化','变化来源'])
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
        # contract_name = data['合约'].strip()
        contract_name = data['合约编码']
        total_value = data['成交数量'] * data['closePrice'] * data['合约乘数']
        self.slippage += total_value - data['成交金额']
        new_holding = pd.DataFrame([{
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '成交数量': -data['成交数量'],  # 买入数量为负
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
                if holding_row['成交数量'] > remaining_to_buy:
                    # 部分买入
                    self.holdings.at[idx, '成交数量'] -= remaining_to_buy
                    self.holdings.at[idx, '收盘总价'] = self.holdings.at[idx, '成交数量'] * holding_row['收盘总价'] * holding_row['合约乘数']
                    remaining_to_buy = 0
                else:
                    # 完全买入该行
                    remaining_to_buy -= holding_row['成交数量']
                    self.holdings = self.holdings.drop(idx)
            
            self.holdings.reset_index(drop=True, inplace=True)
            self.cash = self.cash - total_value
            self.update_total_assets()
        else:
            print(f"Error: Holding not found - buy_holding_close{self.date}")
    
    # 卖出平仓
    def sell_holding_close(self, data):
        # contract_name = data['合约'].strip()
        contract_name = data['合约编码']
        total_value = data['成交数量'] * data['closePrice'] * data['合约乘数']
        self.slippage += data['成交金额'] - total_value
        new_holding = pd.DataFrame([{
            '合约名称': data['合约名称'],
            '合约编码': data['合约编码'],
            '成交数量': data['成交数量'],
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
        #这里的data变成了一个单个的合约名，而非带着这个名字的dataframe了
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
           
    def __repr__(self):
        buying_info = f"\n今日买入:\n{self.today_buying}" if not self.today_buying.empty else ""
        selling_info = f"\n今日卖出:\n{self.today_selling}" if not self.today_selling.empty else ""
        
        return (f"\n日期: {self.date}, 现金: {round(self.cash,2)}, 总资产: {round(self.total_assets,2)}"
                f"{buying_info}"
                f"{selling_info}"
                f"\n持仓:\n{self.holdings}"
                f"\n今日收益（不包含操作误差）: {round(self.difference,2)}, 今日操作误差汇总:{round(self.slippage,2)}")