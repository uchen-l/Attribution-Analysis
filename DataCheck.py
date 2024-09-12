import pickle
import re
import os
import pandas as pd
import numpy as np
from api_backtest.interface import *
from api_backtest.requests.api_login import LoginUtils
from api_backtest.requests.env_conf import Env
from tqdm import tqdm

class DataCheck:
    def __init__(self, filename, checkpath):
        self.filename = filename
        self.check_or_create()
        self.data = self.load_data()
        self.checkpath = checkpath
    def check_or_create(self):
        if not os.path.exists(self.filename):
            print(f"{self.filename} 不存在，正在初始化...")
            with open(self.filename, 'wb') as file:
                pickle.dump({}, file)
            print(f"{self.filename} 已创建。")
        else:
            print(f"{self.filename} 已存在。")
    def load_data(self):
        with open(self.filename, 'rb') as file:
            data = pickle.load(file)
        return data

    def save_data(self):
        with open(self.filename, 'wb') as file:
            pickle.dump(self.data, file)

    def get_dataframe(self, target_name):
        # 检查 data.pickle 中是否存在标的名字的 key
        if target_name in self.data:
            # print(f"从 data.pickle 加载 {target_name} 的数据")
            return self.data[target_name]       
        # 根据规则查找相应的目录
        directory = self.get_directory(target_name)
        # 搜索目录中的 CSV 文件并加载数据
        csv_file = self.find_csv_file(directory, target_name)
        if csv_file:
            # print(f"正在从本地文件中加载{target_name}的数据")
            df = pd.read_csv(csv_file)
            df.rename(columns = {'Unnamed: 0': 'tradeDate', 'CLOSE': 'closePrice'}, inplace=True)
            
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            self.data[target_name] = df
            self.save_data()
            return df
        else:
            print(f"在目录 {directory} 中未找到 {target_name} 对应的 CSV 文件")
            return None

    # 正则表达式 索引
    def get_directory(self, target_name):
        target_name = target_name.upper().strip()
        # 优化正则表达式
        if re.match(r'^T(F|B|L)\d{4}$', target_name) or re.match(r'^T\d{4}$', target_name):
            return self.checkpath + r"\\bond_future"
        elif re.match(r'^I[FHIMC]', target_name) or re.match(r'^\d{6}', target_name):
            return self.checkpath + r"\\stock_index_future"
        else:
            return self.checkpath + r"\\commodity_future"

    def find_csv_file(self, directory, target_name):
        for root, _, files in os.walk(directory):
            for file in files:
                if file.startswith(target_name) and file.endswith('.csv'):
                    return os.path.join(root, file)
        return None
    
    def find_date_price(self, target_df, search_date):
        search_date =pd.to_datetime(search_date)
        for index, row in target_df.iterrows():
            if row['tradeDate'] == search_date:
                return row['closePrice']
        print(f"未找到{search_date}的相关数据")
        return None

data_check = DataCheck('data.pickle',r"\\xw\\SHARE\\lrs\\lrs_data")    
    
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
    # 20240909:本来在这一步会转化成2024-09-09，但是因为后续需要用到这个日期作为string格式传入，所以不转化
    # df_month["成交日期"] = pd.to_datetime(df_month["成交日期"], format='%Y%m%d')
    return df_month

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

def add_price(row):
    search_date = row['成交日期']
    target_name = row['合约']
    searching_df = data_check.get_dataframe(target_name)
    price = data_check.find_date_price(searching_df, search_date)
    row['收盘价'] = price
    return row

def add_multiple_index(row):
    row['合约乘数'] = row['成交额'] / row['手数'] / row['成交价']
    return row

def check_or_create(filename):
    if not os.path.exists(filename):
        print(f"{filename} 不存在，正在初始化...")
        with open(filename, 'wb') as file:
            pickle.dump({}, file)
        print(f"{filename} 已创建。")
    else:
        print(f"{filename} 已存在。")

# def get_period(merged_data):
#     start_date = merged_data['成交日期'].min()
#     end_date = merged_data['成交日期'].max()
#     sample_biaodi = merged_data['合约编码'].iloc[0]
#     # print(f'start date : {start_date}, end date {end_date}, biaodi {sample_biaodi}')
#     searching_df = data_check.get_dataframe(sample_biaodi)
#     trading_date = searching_df[(searching_df['tradeDate'] >= start_date) & (searching_df['tradeDate'] <= end_date)]['tradeDate']
#     trading_date = trading_date.reset_index(drop = True)
#     return trading_date  

# def get_period_option(history_price):
#     all_dates = set()
#     for res_df in history_price.values():
#         all_dates.update(res_df['tradeDate'].astype(str).unique().tolist())

#     # 将这些日期合并到一个列表中
#     tradingday = sorted(list(all_dates))
#     return tradingday

def get_period(merged_data, check_type=''):
    if 'future' in check_type:
        # 处理 future 情况
        start_date = merged_data['成交日期'].min()
        end_date = merged_data['成交日期'].max()
        sample_biaodi = merged_data['合约编码'].iloc[0]
        # print(f'start date : {start_date}, end date {end_date}, biaodi {sample_biaodi}')
        searching_df = data_check.get_dataframe(sample_biaodi)
        trading_date = searching_df[(searching_df['tradeDate'] >= start_date) & (searching_df['tradeDate'] <= end_date)]['tradeDate']
        trading_date = trading_date.reset_index(drop=True)
        return trading_date
    elif 'option' in check_type:
        # 处理 option 情况
        all_dates = set()
        for res_df in merged_data.values():
            all_dates.update(res_df['tradeDate'].astype(str).unique().tolist())
        # 将这些日期合并到一个列表中
        trading_date = sorted(list(all_dates))
        # trading_date = pd.to_datetime(trading_date)
        return trading_date
    else:
        raise ValueError("Invalid check_type provided. Please specify 'future' or 'option'.")



def process_statements(statements, dataframes):
    for filename in os.listdir('.'):
    # 如果文件名匹配正则表达式（以‘结算单’开头并以‘.txt’结尾）
        if re.match(r'^结算单_\d+\.txt$', filename):
            # 读取并存储结算单
            statements[filename] = read_statement(filename)
    for filename in statements:
    # 避免添加空DataFrame
        if statements[filename] is not None:
            dataframes.append(statements[filename])
    merged_dataframe = pd.concat(dataframes, ignore_index=True)
    return merged_dataframe

# 从txt交易单文件转化为csv文件，最终分为商品期权和商品期货两个csv文件
def txt_conversion():
    statements = {}
    dataframes = []
    big_dataframe = process_statements(statements, dataframes)
    option_df = big_dataframe[big_dataframe['品种'].str.contains('期权')].reset_index(drop=True)
    future_df = big_dataframe[~big_dataframe['品种'].str.contains('期权')].reset_index(drop=True)
    
    # 针对两类df的成交日期进行分类处理，因为API获取价格时需要用到成交日期的string格式，本地获取价格直接通过datetime索引
    future_df['成交日期'] = pd.to_datetime(future_df['成交日期'], format='%Y%m%d')
    option_df['成交日期'] = option_df['成交日期'].astype(str).apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")
    
    # 只有商品期货需要用add_price，因为商品期货的收盘价数据在本地文件，而商品期权，ETF期权的收盘价数据在API中可获取
    # 只有商品期货需要用name_correction，因为商品期货的合约名称有一些特殊情况，需要进行修正
    future_df.insert(future_df.shape[1], "收盘价", None)
    future_df = name_correction(future_df)
    future_df = future_df.apply(add_price, axis = 1)
    future_df.insert(future_df.shape[1], "合约乘数", None)
    
    # 合约名称转化为大写，以便后续检索
    option_df['合约'] = option_df['合约'].str.upper()
    future_df['合约'] = future_df['合约'].str.upper()
    
    # 计算合约乘数
    future_df = future_df.apply(add_multiple_index, axis = 1)
    option_df = option_df.apply(add_multiple_index, axis = 1)
    future_df.to_csv('future_df.csv', encoding='gbk')
    option_df.to_csv('option_df.csv', encoding='gbk')
    return future_df, option_df

def get_api_price(trading_data):
    ins_id_list = trading_data['合约编码'].unique().tolist()
    all_start_date = trading_data['成交日期'].min()
    all_end_date = trading_data['成交日期'].max()
    price_dict = {}
    LoginUtils.set_auth_info(
            username='intern1',
            password='Intern123456'
        )
    for ins_id in tqdm(ins_id_list):
        instrument_id_list = [str(ins_id)]
        # ins_data = trading_data[trading_data['合约编码'] == ins_id]
        result = get_quote_close(
            instrument_id_list=instrument_id_list,
            start_date=all_start_date,
            end_date=all_end_date
        )
        res_df=pd.DataFrame(result)
        price_dict[ins_id] = res_df
    return price_dict

def add_api_price(trading_data, price_dict):
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
            raise ValueError(f"合约编码 {ins_id} 未找到对应的价格数据")
    return trading_data


def extract_etf_name(contract_name):
    pattern = re.compile(r'^\d+ETF')
    match = pattern.match(contract_name)
    return match.group() if match else None