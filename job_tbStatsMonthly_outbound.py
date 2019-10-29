# -*- coding: utf-8 -*-
#### logger ##################################################
import logging
from func.loggy import log_init
log_init('./log/tbStatsMonthly_outbound.log')

#### main ###################################################
# 觀光局_出境人次
# https://admin.taiwan.net.tw/FileUploadCategoryListC003330.aspx?CategoryID=5d16abba-e4d6-4498-8928-d391c7c7e28a&appname=FileUploadCategoryListC003330

from os import listdir
import pandas as pd
import xlrd
from urllib.request import urlretrieve
import func.tbStatsMonthly as tbStatsMonthly

### file update 
cat_name='outbound'
cat_id='5d16abba-e4d6-4498-8928-d391c7c7e28a'
tbStatsMonthly.scrapyUpdate(cat_id,cat_name)


# Newfiles on web
tbStatsMonthly.createFolder('./src/tbStatsMonthly/'+cat_name)
files = listdir('./src/tbStatsMonthly/'+cat_name)
dl_list=pd.read_csv('./src/tbStatsMonthly/' + cat_name+'_filelist.csv')
dl_list=dl_list[~dl_list['title'].isin(files)]
try:
    for l,t in list(zip(list(dl_list['link']),list(dl_list['title']))):
        url= 'https://admin.taiwan.net.tw/'+l
        urlretrieve(url, './src/tbStatsMonthly/outbound/' + t  )
        logging.info(t + ': FileDownloads OK')
except:
    logging.error(t + ': FileDownloads ERROR')
    
# 創造初始空dataframe
df_all=pd.DataFrame() 


# 整理+合併資料
for f in list(dl_list['title']):
    xls = xlrd.open_workbook(r'./src/tbStatsMonthly/outbound/'+f, on_demand=True)
    if ('Sheet3' in xls.sheet_names())==True:
        try:
            df_add=tbStatsMonthly.outbound_reshape(f,'Sheet3')
            df_all=df_all.append(df_add,ignore_index = True, sort=False)
            logging.info(f  + ': Data ETL OK')
        except xlrd.XLRDError:
            logging.error(f  + ": Data ETL Error")
    elif ('出國按目的地' in xls.sheet_names())==True:
        try:
            df_add=tbStatsMonthly.outbound_reshape(f,'出國按目的地')
            df_all=df_all.append(df_add,ignore_index = True, sort=False)
            logging.info(f  + ': Data ETL OK')
        except xlrd.XLRDError:
            logging.error(f  + ": Data ETL Error") 
    
# 清理原始資料中不一致的日期資料格式
df_all['YEAR']=df_all['YEAR'].apply(tbStatsMonthly.modify_year)

#df_all.to_excel('./outbound.xls',sheet_name='outbound')
#### db #######################################################
from func.sql import get_db_key, dbConn, dbGet
usr, pwd = get_db_key()
engine = dbConn(usr, pwd)

try:
    df_all.to_sql('EXD_TB_STATSMONTHLY_OUTBOUND', engine, index=False, if_exists='append')
    logging.info('EXD_TB_STATSMONTHLY_OUTBOUND: DB OK')
except:
    logging.error('EXD_TB_STATSMONTHLY_OUTBOUND: DB ERROR')