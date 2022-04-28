"""choice量化接口-试用1个月"""
from EmQuantAPI import *
loginresult = c.start()
#loginresult为c.EmQuantData类型数据
print (loginresult)
"""------先导入一个北交所股票的代码列表，后续传入接口取数-------"""
import pandas as pd
bj = pd.read_csv('北交所股票.csv',encoding='utf-8')
bj.head()
"""-----取数-----"""
# 添加股票代码后缀，传入choice接口
bj_codes = [str(code)+'.BJ' for code in bj.code.tolist()]
bj_codes[:5]

# 获取北交所股票的收盘价格、流通股本、成交量数据
bj_data = c.csd(bj_codes,"CLOSE,AMOUNT,LIQASHARE",
                "2021-11-15","2022-04-01","Ispandas=1,,RowIndex=2")  
bj_data.head()

"""-----取出来的数长这样----"""
	CODES	CLOSE	AMOUNT	LIQASHARE
DATES				
2021/11/15	430047.BJ	16.95	1.78722e+08	136286809
2021/11/15	430090.BJ	8.15	1.53067e+08	88912243
2021/11/15	430198.BJ	11.32	5.15861e+07	64644831
2021/11/15	430418.BJ	16.21	4.17963e+07	39790926
2021/11/15	430489.BJ	17.55	4.77496e+07	52278143

"""----开始处理数据-----"""
# 查看交易时间
dates = bj_data.index.unique().tolist()
print(dates[:5])
print(len(dates))

# 查看数据的缺失情况
# 收盘价CLOSE总共存在372个空值
print(bj_data.isnull().sum())
print(bj_data.shape)

# 剔除缺失值
bj_data.dropna(axis=0,how='any',subset=['CLOSE'],inplace=True)
print(bj_data.isnull().sum())
print(bj_data.shape)
"""---开始计算----"""
# 基期为2021/11/15，基点为1000点
bj_index = [1000,]
# 计算北证指数
for i in range(len(dates)-1):
    fenzi = 0  # 公式分子
    fenmu = 0  # 公式分母
    stocks_now = bj_data[bj_data.index==dates[i]]  # 当前交易日数据作为分母
    stocks_next = bj_data[bj_data.index==dates[i+1]]  # 下一个交易日数据作为分子
    for j in range(len(stocks_now)):  # 遍历当前的每只股票
        # 当前收盘价
        close_now = stocks_now.iloc[j,1]  
        # 下一交易日收盘价
        close_next = stocks_next[stocks_next.CODES==stocks_now.iloc[j,0]].iloc[0,1]  
        # 下一交易日流通股本数
        ltgb = stocks_next[stocks_next.CODES==stocks_now.iloc[j,0]].iloc[0,3] 
        # 累加
        fenzi += close_next*ltgb
        fenmu += close_now*ltgb
    bj_index.append(bj_index[-1]*fenzi/fenmu)
print(len(bj_index))
print(bj_index)
"""---输出---"""
test = pd.DataFrame()
test['DATES'] = dates
test['index'] = bj_index
test.to_csv('北交所指数测试.csv')

"""---计算4.1号当天每分钟的指数----"""
import pandas as pd

bj_0401_data = pd.read_csv('0401北交所分钟行情数据.csv',index_col=0)
bj_0401_data.head()

# 查看一下日期的顺序
minutes = bj_0401_data.index.unique().tolist()
minutes[:5]
>>
['2022/4/1 9:31',
 '2022/4/1 9:32',
 '2022/4/1 9:33',
 '2022/4/1 9:34',
 '2022/4/1 9:35']

# 为数据添加一列‘流通股本数’
# 找出来历史数据中2022/4/1的股票
bj_0401_ltgb = bj_data[bj_data.index=='2022/4/1']
# 将股票代码的后缀去掉
bj_0401_ltgb.CODES = bj_0401_ltgb.CODES.map(lambda x:x.rstrip('.BJ'))
# 字典映射{股票代码：股票流通股本}
code_ltgb_dic = dict(zip(bj_0401_ltgb.CODES,bj_0401_ltgb.LIQASHARE))
# 添加流通股本
ltgb_0401 = []
for i in range(len(bj_0401_data)):
    # 找出当前该股票的流通股本数
    ltgb_ = code_ltgb_dic[str(bj_0401_data.iloc[i,0])]   # 把数值转换为字符串 
    ltgb_0401.append(ltgb_)
bj_0401_data['LIQASHARE'] = ltgb_0401
bj_0401_data.head()
>>
	code	open	close	high	low	volume	money	belong	name	pre_close	change_rate	LIQASHARE
time												
2022/4/1 9:31	430047	14.40	14.40	14.40	14.40	1000	14400	京交所	诺思兰德	14.42	-0.001387	147788948
2022/4/1 9:32	430047	14.40	14.40	14.40	14.40	2400	34560	京交所	诺思兰德	14.42	-0.001387	147788948
2022/4/1 9:33	430047	14.40	14.40	14.40	14.40	0	0	京交所	诺思兰德	14.42	-0.001387	147788948
2022/4/1 9:34	430047	14.40	14.39	14.40	14.37	13089	188325	京交所	诺思兰德	14.42	-0.002080	147788948
2022/4/1 9:35	430047	14.39	14.39	14.39	14.39	2200	31658	京交所	诺思兰德	14.42	-0.002080	147788948
  
"""接下来的计算方法就和上面一样..."""
# 省略若干行代码--.
