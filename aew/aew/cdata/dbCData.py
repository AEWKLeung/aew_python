import sqlalchemy as sa
from sqlalchemy import create_engine
import urllib
import pyodbc
from pandas.core.frame import DataFrame
import pandas as pd 


connString_CData='Driver={ODBC Driver 17 for SQL Server};Server=tcp:aew-sql.database.windows.net,1433;Database=AEW_ChemDB;Uid=kleung300db;Pwd=300Junction;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=100;'


 
def importExcel(fname,tname):
   try:
        connData=pyodbc.connect(connString_CData,autocommit=True)
        """
        coxn = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connString_CData))
        """
        df=pd.read_excel(fname)
        df.to_sql(tname,con=connData,if_exists='append') 
        return "success"
   except :
        return "failed"

def dropTempTable(tname):

    """
        coxn = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connString_CData))
        insp=sa.inspect(coxn)
        if (insp.has_table(tname, schema="dbo"))==True:
            tname.__table__.drop(coxn)
            """
    connData=pyodbc.connect(connString_CData,autocommit=True)

    cursorData=connData.cursor()

    sql="DROP TABLE IF EXISTS " + tname
    cursorData.execute(sql)
    return
