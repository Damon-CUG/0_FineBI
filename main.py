import pymysql
# linux服务器
db = pymysql.connect(host='^-^',
                     user='***',
                     password='***',
                     database='***')
print('linux数据库连接成功')


"""---------------实时更新-------------------"""
import pandas as pd
import time


"""---------1.沪、深、京大盘指数数据----------"""

index0401 = pd.read_csv(r"0401指数数据.csv",encoding="gbk")

"""---------2.申万一级行业数据-------"""

industry0401 = pd.read_csv(r"0401申万行业指数.csv",encoding="utf-8")
industry0401.dropna(inplace=True)  # 删除含有空值的数据行

"""---------3.板块股票数据-----------"""

# 上海主板
shmain0401 = pd.read_csv(r"0401上海主板数据.csv",encoding="utf-8")
shmain0401.dropna(inplace=True) 
# 上海科创版
kc0401 = pd.read_csv(r"0401上海科创板数据.csv",encoding="utf-8")
kc0401.dropna(inplace=True)  
# 深圳主板
szmain0401 = pd.read_csv(r"0401深圳主板数据.csv",encoding="utf-8")
szmain0401.dropna(inplace=True)  
# 深圳创业板
cy0401 = pd.read_csv(r"0401深圳创业板数据.csv",encoding="utf-8")
cy0401.dropna(inplace=True)  
# 北京主板
bjmain0401 = pd.read_csv(r"0401北交所数据.csv",encoding='utf-8')
bjmain0401.dropna(inplace=True)

"""-----4.亚太国家大盘指数（韩、日、香港、台湾、新加坡------"""

index_Asia_0401 = pd.read_csv(r"亚太指数（5）.csv",encoding="utf-8")

"""--------------数据入库----------------"""

cursor = db.cursor()

# 9:30-15:00 共240分钟（各个数据源统一时间格式 2022/4/1 9:31）
time_ = index0401.time.tolist()[:240]   # 按时间顺序遍历更新

# 获取每分钟的数据
for t in time_:
    print('当前更新时间：{}'.format(t))

    """------1.加载每分钟的大盘指数数据--------"""

    index_data = index0401[index0401.time == t]  # 沪、深、京 三个大盘指数
    sql_index = """INSERT INTO 0401_index VALUES (%s,%s,%s,%s,%s,%s)"""
    for i in range(len(index_data)):
        cursor.execute(sql_index,(
                index_data.iloc[i,0],index_data.iloc[i,1],
                float(index_data.iloc[i,2]),float(index_data.iloc[i,3]),
                float(index_data.iloc[i,4]), float(index_data.iloc[i,5])))
    db.commit()
    print('沪、深、京大盘指数更新成功！')

    """------2.加载每分钟的亚太大盘指数数据--------"""

    index_Asia_data = index_Asia_0401[index_Asia_0401.time == t]  # 亚太地区5个大盘指数
    sql_Asia_index = """INSERT INTO 0401_Asia_index VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    for i in range(len(index_Asia_data)):
        cursor.execute(sql_Asia_index,(index_Asia_data.iloc[i,0],
                index_Asia_data.iloc[i,1],index_Asia_data.iloc[i,2],
                float(index_Asia_data.iloc[i,3]),float(index_Asia_data.iloc[i,4]),
                float(index_Asia_data.iloc[i,5]),float(index_Asia_data.iloc[i,6]),
                float(index_Asia_data.iloc[i,7]),float(index_Asia_data.iloc[i,8])))
    db.commit()
    print('亚太地区大盘指数更新成功！')

    """------2.加载每分钟的申万一级行业数据------"""

    if t != '2022/4/1 9:31':  # 第一分钟无涨跌幅更新
        industry_data = industry0401[industry0401['交易时间'] == t]
        sql_industry = 'insert into 0401_industry values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        for i in range(len(industry_data)):  
            cursor.execute(sql_industry,(
                        industry_data.iloc[i,0],industry_data.iloc[i,1],industry_data.iloc[i,2],
                        float(industry_data.iloc[i,3]),float(industry_data.iloc[i,4]),
                        float(industry_data.iloc[i,5]),float(industry_data.iloc[i,6]),
                        float(industry_data.iloc[i,7]),float(industry_data.iloc[i,8]),
                        float(industry_data.iloc[i,9]),float(industry_data.iloc[i,10])))
        db.commit()
        print('申万一级行业更新成功！')

    
    """-------3.加载每分钟的各个板块的股票数据--------"""

    # 上海主板
    shmain_data = shmain0401[shmain0401.time == t]
    sql_sh_main = 'insert into 0401_sh_main values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for i in range(len(shmain_data)):  
        cursor.execute(sql_sh_main,(shmain_data.iloc[i,0],shmain_data.iloc[i,1],
                    float(shmain_data.iloc[i,2]),float(shmain_data.iloc[i,3]),
                    float(shmain_data.iloc[i,4]),float(shmain_data.iloc[i,5]),
                    float(shmain_data.iloc[i,6]),float(shmain_data.iloc[i,7]),
                    shmain_data.iloc[i,8],shmain_data.iloc[i,9],
                    float(shmain_data.iloc[i,10]),float(shmain_data.iloc[i,11])))
    db.commit()
    print('上海主板更新成功！')

    # 上海科创板
    kc_data = kc0401[kc0401.time == t]  
    sql_sh_kc = 'insert into 0401_sh_kc values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for i in range(len(kc_data)):  
        cursor.execute(sql_sh_kc,(kc_data.iloc[i,0],kc_data.iloc[i,1],
                    float(kc_data.iloc[i,2]),float(kc_data.iloc[i,3]),
                    float(kc_data.iloc[i,4]),float(kc_data.iloc[i,5]),
                    float(kc_data.iloc[i,6]),float(kc_data.iloc[i,7]),
                    kc_data.iloc[i,8],kc_data.iloc[i,9],
                    float(kc_data.iloc[i,10]),float(kc_data.iloc[i,11])))
    db.commit()
    print('上海科创板更新成功！')

    # 深圳主板
    szmain_data = szmain0401[szmain0401.time == t]
    sql_sz_main = 'insert into 0401_sz_main values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for i in range(len(szmain_data)):  
        cursor.execute(sql_sz_main,(szmain_data.iloc[i,0],szmain_data.iloc[i,1],
                    float(szmain_data.iloc[i,2]),float(szmain_data.iloc[i,3]),
                    float(szmain_data.iloc[i,4]),float(szmain_data.iloc[i,5]),
                    float(szmain_data.iloc[i,6]),float(szmain_data.iloc[i,7]),
                    szmain_data.iloc[i,8],szmain_data.iloc[i,9],
                    float(szmain_data.iloc[i,10]),float(szmain_data.iloc[i,11])))
    db.commit()
    print('深圳主板更新成功！')

    # 深圳创业板
    cy_data = cy0401[cy0401.time == t]
    sql_sz_cy = 'insert into 0401_sz_cy values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for i in range(len(cy_data)):  
        cursor.execute(sql_sz_cy,(cy_data.iloc[i,0],cy_data.iloc[i,1],
                    float(cy_data.iloc[i,2]),float(cy_data.iloc[i,3]),
                    float(cy_data.iloc[i,4]),float(cy_data.iloc[i,5]),
                    float(cy_data.iloc[i,6]),float(cy_data.iloc[i,7]),
                    cy_data.iloc[i,8],cy_data.iloc[i,9],
                    float(cy_data.iloc[i,10]),float(cy_data.iloc[i,11])))
    db.commit()
    print('深圳创业板更新成功！')

    # 北京主板
    bj_data = bjmain0401[bjmain0401.time == t]
    sql_bj_main = 'insert into 0401_bj_main values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for i in range(len(bj_data)):  
        cursor.execute(sql_bj_main,(bj_data.iloc[i,0],bj_data.iloc[i,1],
                    float(bj_data.iloc[i,2]),float(bj_data.iloc[i,3]),
                    float(bj_data.iloc[i,4]),float(bj_data.iloc[i,5]),
                    float(bj_data.iloc[i,6]),float(bj_data.iloc[i,7]),
                    bj_data.iloc[i,8],bj_data.iloc[i,9],
                    float(bj_data.iloc[i,10]),float(bj_data.iloc[i,11])))
    db.commit()
    print('北京主板更新成功！')

    # 沉睡五秒zzz....
    time.sleep(5)
