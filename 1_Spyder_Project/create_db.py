from data_config import *
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm

class Create_DB: 
    def __init__(self):
        self.add_col = ['Maintain Time','Maintain Staff']
        self.add_type = ['datetime2(7)','nvarchar(50)']
        self.staff_ID = 'Staff ID'  
        self.time_col = ['Col_1', 'Col_29', 'Col_37', 'Col_38']
        self.modify_col = ['Col_4'] + self.time_col
        self.time_format = '%Y/%m/%d %p %I:%M:%S'
        self.time_chi = ['上午','下午']
        self.time_eng = ['AM','PM']
    
    def Get_Origin(self) -> pd.DataFrame:
        db_folder = os.path.join(FOLDER_PATH, FOLDER_NAME)
        db_files = os.listdir(db_folder)
        db_src = [os.path.join(db_folder, file) for file in db_files]
        data = pd.DataFrame([])
        for file in db_src:
            df = pd.read_excel(file, encoding='ANSI', header=2)
            df = df[:-1] 
            data = data.append(df)
            
        data[self.modify_col[0]] = [j.replace("'",'’') for j in data[self.modify_col[0]]]
        
        data[self.modify_col[1]] = [str(datetime.datetime.strptime(j, '%Y%m')) 
                                    for j in data.loc[:,self.modify_col[1]].astype(int).astype(str)]

        data[self.modify_col[2]] = [str(datetime.datetime.strptime(j, "%Y/%m/%d"))
                                    for j in data.loc[:, self.modify_col[2]]] 

        data[self.modify_col[3]] = data[self.modify_col[3]].str.replace(self.time_chi[0], self.time_eng[0]).str.replace(self.time_chi[1], self.time_eng[1])
        idx = data[self.modify_col[3]].notna()
        data.loc[idx,self.modify_col[3]] = [str(datetime.datetime.strptime(j, self.time_format))
                                            for j in data.loc[idx,self.modify_col[3]]]

        data[self.modify_col[4]] = data[self.modify_col[4]].str.replace(self.time_chi[0], self.time_eng[0]).str.replace(self.time_chi[1], self.time_eng[1]) 
        data[self.modify_col[4]] = [str(datetime.datetime.strptime(j,self.time_format)) 
                                    for j in data.loc[:,self.md_col[4]]]

        data = data.replace(np.nan,'NULL')
        data = data.reset_index(drop=True)        
        origin_type = data.dtypes.astype(str)  
        return data , origin_type

    def Time_Detect(self) -> list:
        df, origin_type = self.Get_Origin()
        get_bool = df.columns.isin(self.time_col)
        for i, col_name, is_time in zip(range(df.shape[1]), df.columns, get_bool):
            if is_time:           
                origin_type[i] = 'datetime64[ns]'
        return origin_type
    
    def Modify_DB(self) -> pd.Series:
        origin_type = self.Time_Detect()
        origin_str = ['object', 'int64', 'datetime64[ns]', 'float64']
        db_str = ['nvarchar(200)','int','datetime','float']
        type_map = dict(zip(origin_str, db_str))
        db_type = origin_type.apply(lambda x: type_map[x])
        return db_type
    
    def Auto_Create(self):
        db_type = self.Modify_DB()
        col_name = list(db_type.index) + self.add_col
        col_type = list(db_type.values) + self.add_type
        conn = CONN
        cursor = CONN.cursor()
        sqlstr = f'CREATE TABLE "{TB_NAME}" ('
        sqlstr += ',\n'.join([f'[{i}] {j}' for i, j in zip(col_name, col_type)])
        sqlstr += ')'
        cursor.execute(sqlstr)
        conn.commit()
        cursor.close()
    
    def Delete_DB(self):
        conn = CONN
        cursor = CONN.cursor()
        sqlstr = f'DELETE "{TB_NAME}"'
        cursor.execute(sqlstr)
        conn.commit()
        cursor.close()
    
    def Insert_DB(self):
        origin_type = self.Time_Detect()
        maintain_time = str(datetime.datetime.today())[:19]
        conn = CONN
        cursor = CONN.cursor()
        cursor.fast_executemany = True
        chunk = 1000
        for i in tqdm(range(0, len(df), chunk)):
            sqlstr = f'INSERT INTO "{TB_NAME}" VALUES'
            for row in df[i:i+chunk].itertuples(index=False, name=None):
                sqlstr += '(' + str(list(row))[1:-1].replace("'NULL'",'NULL')
                sqlstr += f", '{maintain_time}', '{self.staff_ID}'),"
            sqlstr = sqlstr[:-1] + ';'
            try:
                cursor.execute(sqlstr)
                conn.commit()
            except Exception as e:
                print(sqlstr)
        cursor.close()