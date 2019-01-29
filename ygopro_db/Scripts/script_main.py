import os
import sqlite3
import pandas as pd
import numpy as np
import os

#os.chdir(r"C:\Users\nwfxy\Desktop\ygopro_db")

def main():

    df_eng = pd.DataFrame()
    for dbname in os.listdir("data/eng_db"):
        db_data = importDb("data/eng_db/" + dbname)
        col = getColNames("data/eng_db/" + dbname)
        df = coverttoPd(db_data, col)
        print(df)
        df_eng = df_eng.append(df)

    df_data = importDb("data/cn_db/cards.cdb")
    col = getColNames("data/cn_db/cards.cdb")
    df_cn = coverttoPd(df_data,col)
    df_cn_ind = df_cn.ix[:,"id"]
    merged = df_eng.merge(df_cn_ind.to_frame(), 'right', on='id')
    merged.dropna(how = "all")
    merged.to_sql("texts", sqlite3.connect("cards.cdb"), index = False)

    data_sql = getData("data/cn_db/cards.cdb")

    df = coverttoPd(db_data, col)

    datas_df = getData("data/cn_db/cards.cdb")
    datas_df.to_sql("datas",sqlite3.connect("cards.cdb"), index = False)




def getData(dbname):
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('SELECT * FROM datas')
    db_data = (c.fetchall())
    col = []
    colnames = c.description
    for row in colnames:
        col.append(row[0])
    return coverttoPd(db_data,col)

def importDb(dbname):
    print(dbname)
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('SELECT * FROM texts')
    db_data = (c.fetchall())
    
    return db_data






def getColNames(dbname):
    conn = sqlite3.connect(dbname)
    c = conn.cursor()
    c.execute('SELECT * FROM texts')
    col = []
    colnames = c.description
    for row in colnames:
        col.append(row[0])
        
    return col
    

def coverttoPd(db_data,col):
    df = pd.DataFrame(np.array(db_data).reshape(len(db_data),len(col)), columns = col)
    return df



if __name__ == "__main__":
    main()