"""多个excel文件合并成一个excel"""

import pandas as pd
import os

#文件路径
file_dir = r'E:\实盘模拟\0401北交所数据'

#构建新的表格名称
new_filename = file_dir + '\\0401北交所股票数据.csv'

#os.listdir() 方法用于返回指定的文件夹包含的文件或文件夹的名字的列表。
file_list = os.listdir(file_dir)
new_list = []

for file in file_list:
    #重构文件路径
    file_path = os.path.join(file_dir,file)
    #将excel转换成DataFrame
    dataframe = pd.read_excel(file_path)
    #保存到新列表中
    new_list.append(dataframe)

#多个DataFrame合并为一个
df = pd.concat(new_list)
#写入到一个新excel表中
df.to_csv(new_filename,encoding='utf_8_sig',index=False,header=True)
