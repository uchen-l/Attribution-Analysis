import pickle
import re
import os
import pandas as pd

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
            print(f"从 data.pickle 加载 {target_name} 的数据")
            return self.data[target_name]       
        # 根据规则查找相应的目录
        directory = self.get_directory(target_name)
        # 搜索目录中的 CSV 文件并加载数据
        csv_file = self.find_csv_file(directory, target_name)
        if csv_file:
            # print(f"正在从本地文件中加载{target_name}的数据")
            df = pd.read_csv(csv_file)
            df.rename(columns = {'Unnamed: 0': 'Date'}, inplace=True)
            df['Date'] = pd.to_datetime(df['Date'])
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
            if row['Date'] == search_date:
                return row['CLOSE']
        print(f"未找到{search_date}的相关数据")
        return None