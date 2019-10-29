from sqlalchemy import create_engine
import pandas as pd
import json

def get_db_key():
    with open('./src/DB.json' , 'r') as reader:
        jf = json.loads(reader.read())
    return jf['usr'], jf['pwd']

def dbConn(usr, pwd):
    srv, db = '192.168.1.184', 'LionGroupAnalytics' 
    dbStr = 'mssql+pyodbc://' + usr + ':' + pwd + '@' + srv + '/' + db + '?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(dbStr)
    return engine

def dbGet(sqlstr, engine):
    with engine.connect() as conn, conn.begin():
        data = pd.read_sql(sqlstr, conn)
    return data


