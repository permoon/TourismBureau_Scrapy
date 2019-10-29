# -*- coding: utf-8 -*-

import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from os import  path, makedirs

def scrapyInit(cat_id,stop=1):
    link=[]
    title=[]
    # 取得有效的檔案名稱及連結
    for i in range(1,stop+1):
        url='https://admin.taiwan.net.tw/FileUploadCategoryListC003330.aspx?Pindex=' + str(i) +\
        '&CategoryID=' + cat_id + '&appname=FileUploadCategoryListC003330'
        source = requests.get(url)
        print(url)
        check = source.text
        if check.find('XLSX檔案')==-1 & check.find('XLS檔案')==-1:
            break
        soup = BeautifulSoup(source.text, 'html.parser')
        a_tags = soup.find_all('a', class_="kf_dload kf_dload-xlsx")     
        for i in a_tags:
            link.append(i.get('href'))
            title.append(i.get('title')+'.xls')
    # 創建下載清單df
    df=pd.DataFrame({'link':link,'title':title})              
    return df
 

def file_filter(df):    
    #刪除多餘檔案_出境
    if df['title'][0].find('出國')>-1:
        df=df[df['title'].str.contains("年1月") | 
                df['title'].str.contains("~") | 
                df['title'].str.contains("年 1月")].reset_index(drop=True) 
    #刪除多於檔案_入境
    elif df['title'][0].find('來台')>-1:
        df=df[~df['title'].str.contains("至") & 
              ~df['title'].str.contains("~")].reset_index(drop=True)    
    return df


def scrapyUpdate(cat_id,cat_name):
    exists=pd.read_csv('./src/tbStatsMonthly/'+cat_name+'_filelist.csv')
    new=scrapyInit(cat_id,stop=1)
    new=file_filter(new)
    exists.update(new)
    exists.to_csv('./src/tbStatsMonthly/'+cat_name+ '_filelist.csv', index=0)


def createFolder(directory):
    try:
        if not path.exists(directory):
            makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)


def outbound_reshape(file,sheet,path='./src/tbStatsMonthly/outbound/'):
    df_output=pd.read_excel(path+file,sheet_name=sheet)
    df_output=df_output.iloc[:,0:3]
    df_output.columns=['CONTINENT','COUNTRY','PEOPLE']
    df_output['CONTINENT']=df_output['CONTINENT'].astype(str)
    df_output['COUNTRY']=df_output['COUNTRY'].astype(str)
    for i in range(1,len(df_output)):
        if df_output['CONTINENT'][i]=='nan':
            df_output['CONTINENT'][i]=(df_output['CONTINENT'][i-1])
    df_output=df_output[(df_output['COUNTRY']!='nan') & (~df_output['COUNTRY'].str.contains("計"))]
    df_output['YEAR']=int(file[:file.find('年')])
    if file.find('~')>-1:
        df_output['MONTH']=int(file[file.find('~')+1:file.find('月')])
    elif file.find('至')>-1:
        df_output['MONTH']=int(file[file.find('至')+1:file.rfind('月')])
    elif file.find('至')==-1 & file.find('~')==-1:
        df_output['MONTH']=int(file[file.find('年')+1:file.rfind('月')])
    return df_output


def inbound_reshape(file,sheet,path='./src/tbStatsMonthly/inbound/'):
    df_output=pd.read_excel(path+ '/'+file,sheet_name=sheet)
    df_output=df_output.iloc[:,0:6]
    df_output.columns=['CONTINENT','AREA','COUNTRY','PEOPLE_TOT','OVERSEAS_CHINESE','FOREIGNER']
    for i in ['CONTINENT','AREA','COUNTRY']:
        df_output[i]=df_output[i].astype(str)
    for i in range(1,len(df_output)):
        if df_output['CONTINENT'][i]=='nan':
            df_output['CONTINENT'][i]=(df_output['CONTINENT'][i-1])
        if df_output['AREA'][i]=='nan':
            df_output['AREA'][i]=(df_output['AREA'][i-1])
    df_output=df_output[(df_output['AREA']!='nan') & (~df_output['AREA'].str.contains("計")) & (~df_output['COUNTRY'].str.contains("計"))]
    df_output['YEAR']=int(file[:file.find('年')])
    df_output['MONTH']=int(file[file.find('年')+1:file.find('月')])
    return df_output

# 清理原始資料中不一致的日期資料格式
def modify_year(year):
    year_m=0
    if year<1990:
        year_m=year+1911
    else:
        year_m=year
    return year_m

# 第一次執行，取網頁上全部檔案清單
if __name__ == "__main__":
    # 存init file  
    parent_path = path.abspath("..")    
    dl_list={'outbound':'5d16abba-e4d6-4498-8928-d391c7c7e28a',
             'inbound':'95e51de6-53cd-4a2d-9214-c89fc9936ba2'}
    cat_name=list(dl_list.keys())
    cat_id=list(dl_list.values())    
    for i,j in list(zip(cat_name,cat_id)):
        data=scrapyInit(j,stop=40)
        data=file_filter(data) 
        data.to_csv(parent_path+'/src/tbStatsMonthly/'+i+'_filelist.csv', index=0,encoding='utf8')
    