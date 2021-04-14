# -
挖地兔平台股票与股东数据爬取与节点创建
#%%
import tushare as ts  # 参考Tushare官网提供的安装方式
import csv
import time
import pandas as pd
# 设置token
pro = ts.pro_api('730dac02341cae9d2605e3ea3e688b42f1d1761774185854f30c2d24')
stock_basic = pro.stock_basic(list_status='L', fields='ts_code, symbol, name, industry')
print(stock_basic[:5])
print(len(stock_basic))
# 重命名行，便于后面导入neo4j
basic_rename = {'ts_code': 'TS代码', 'symbol': '股票代码', 'name': '股票名称', 'industry': '行业'}
stock_basic.rename(columns=basic_rename, inplace=True)
#修改股票代码的格式，否则0省略
stock_basic['股票代码'] = '|'+stock_basic['股票代码']
# 保存为stock_basic.csv
stock_basic.to_csv('d:\\stock_basic.csv', encoding='gbk')
holders = pd.DataFrame(columns=('ts_code', 
                                'start_date', 'end_date', 
                                'holder_name', 'hold_amount', 'hold_ratio'))
# 获取一年内所有上市股票股东信息（可以获取一个报告期的）
for i in range(0,len(stock_basic)):
    try:
        code = stock_basic['TS代码'].values[i]
        holders = pro.top10_holders(ts_code=code, start_date='20200101', end_date='20201231')
        holders = holders.append(holders)       
    except:
        time.sleep(1)# 数据接口限制
# 只保留年报数据（end_date='20201231'）
#holders = holders[holders['end_date']=='20201231']
# 保存为stock_holders.csv
    holders.to_csv('d:\\stock_holders.csv', encoding='gbk')
#%%
def get_holders(ts_code='', ann_date='', start_date='', end_date=''):
    for _ in range(3):
        try:
            if ann_date:
                df = pro.top10_holders(ts_code=ts_code, ann_date=ann_date)
            else:
                df = pro.top10_holders(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except:
            time.sleep(0.4)
        else:
            return df
for i in range(len(stock_basic)):
    code = stock_basic['TS代码'].values[i]
    start_date='20200101'
    end_date='20201231'
    df = get_holders(ts_code=code)
    holder=holder.append(df)
# 保存为stock_holders.csv
    holder.to_csv('d:\\stock_holders_df.csv', encoding='gbk')    
#%%   
import tushare as ts  # 参考Tushare官网提供的安装方式
import csv
import time
import pandas as pd
# 设置token
pro = ts.pro_api('730dac02341cae9d2605e3ea3e688b42f1d1761774185854f30c2d24')
from pandas import DataFrame
from py2neo import Graph,Node,Relationship,NodeMatcher
import numpy as np
import os
# 连接Neo4j数据库
graph = Graph('http://localhost:7474/db/data/',username='neo4j',password='guo@zhang170314')
#读取股票和股东数据
stock = pd.read_csv('D:\\stock_basic.csv',encoding="gbk")
holder = pd.read_csv('D:\\stock_holders.csv',encoding="gbk")
#股东去重，只保留不重复的股东
holder = holder.drop_duplicates(subset=None, keep='first', inplace=False)
#创建股票实体
for i in stock.values:
    a = Node('股票',TS代码=i[1],股票名称=i[3],行业=i[4])
    print('TS代码:'+str(i[1]),'股票名称:'+str(i[3]),'行业:'+str(i[4]))
    graph.create(a)
#创建股东实体
for i in holder.values:
    a = Node('股东',TS代码=i[0],股东名称=i[1],持股数量=i[2],持股比例=i[3])
    print('TS代码:'+str(i[0]),'股东名称:'+str(i[1]),'持股数量:'+str(i[2]))
    graph.create(a)
#创建关系
matcher = NodeMatcher(graph)
for i in holder.values:
    a = matcher.match("股票",TS代码=i[0]).first()
    b = matcher.match("股东",TS代码=i[0])
    for j in b:
        r = Relationship(j,'参股',a)
        graph.create(r)
        print('TS',str(i[0]))
#%%
