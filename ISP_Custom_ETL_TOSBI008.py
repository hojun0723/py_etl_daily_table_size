#======================================================
#
# 일별 특정 DB 테이블별 용량 및 데이터 갯수 저장
#
#======================================================

import pyodbc
#import MySQLdb
#import mysqlclient
#import pymysql
import datetime
# import threading
import sys
import time

def MSSQLtoMSSQL():

    try:
        # SOURCE SERVER INFO
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=123.123.123.123;DATABASE=database;UID=userid;PWD=password')
        cursor = cnxn.cursor()
        
        # TARGET SERVER INFO
        cnxn2 = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=123.123.123.123;DATABASE=database;UID=userid;PWD=password')
        cursor2 = cnxn2.cursor()
		
        msg = "\nselect start: " + str(datetime.datetime.now())
        print(msg)

        sql = ""	   
        sql = sql + " SELECT	 A.*, CONVERT(NVARCHAR(20), B.T_ROWCNT) AS T_ROWCNT                     "
        sql = sql + " 		 , CONVERT(NVARCHAR(8), GETDATE(), 112) AS CRT_DT                       "
        sql = sql + " 	         , 'ISP' AS CRT_USER_ID                                                 "
        sql = sql + " 	         , GETDATE() AS DATA_CRT_DTM                                          "
        sql = sql + " FROM                                                                              "
        sql = sql + " (                                                                                 "
        sql = sql + "                                                                                   "
        sql = sql + " 	SELECT CONVERT(VARCHAR(30), MIN(o.name)) AS T_NAME                              "
        sql = sql + " 	     , LTRIM(STR(SUM(reserved) * 8192.0 / 1024.0, 15, 0)) AS T_SIZE_KB          "
        sql = sql + " 	FROM   sysindexes i                                                             "
        sql = sql + " 	           INNER JOIN sysobjects o ON o.id = i.id                               "
        sql = sql + " 	WHERE  i.indid IN (0, 1, 255)                                                   "
        sql = sql + " 	   AND o.xtype = 'U'                                                            "
        sql = sql + " 	GROUP BY                                                                        "
        sql = sql + " 	       i.id                                                                     "
        sql = sql + " 	                                                                                "
        sql = sql + " ) A LEFT OUTER JOIN (                                                             "
        sql = sql + "                                                                                   "
        sql = sql + " 	SELECT o.name                                                                   "
        sql = sql + " 	     , i.rows AS T_ROWCNT                                                       "
        sql = sql + " 	FROM   sysindexes i                                                             "
        sql = sql + " 	           INNER JOIN sysobjects o ON i.id = o.id                               "
        sql = sql + " 	WHERE  i.indid < 2                                                              "
        sql = sql + " 	   AND o.xtype = 'U'                                                            "
        sql = sql + "                                                                                   "
        sql = sql + " ) B ON A.t_name = B.name                                                          "

        r = cursor.execute(sql)
        c = [column[0] for column in r.description]
        results = []
        
        k = 0
        for row in cursor.fetchall():

            k += 1
            results.append(dict(zip(c, row)))

            #print(row)

        msg = "%s row(s)" % (str(k))
        print(msg)
        
        msg = "select end  : " + str(datetime.datetime.now())
        print(msg)
        
        msg = "\ninsert start: " + str(datetime.datetime.now())
        print(msg)

        k = 0
        for myDict in results:
            
            k += 1
            columns = ','.join(myDict.keys())

            # %s 인 경우에는 에러 발생하여 ? 로 placeholders 변경
			placeholders = ','.join(['?'] * len(myDict))
            #print(placeholders)

            sql = "insert into " + "TOSBI008" + " (%s) values (%s)" % (columns, placeholders)

            #print(sql)
            #return

            #print(sql)
            #print(list(myDict.values()))
            #return
            
            cursor2.execute(sql, list(myDict.values()))
            
            #sys.stdout.write("#")
            #sys.stdout.flush()

            #msg = "%s row(s)" % (str(k))
            #print(msg, end='')
            #print("\r", end='')

            
        msg = "%s row(s)" % (str(k))
        print(msg)

        msg = "insert end  : " + str(datetime.datetime.now())
        print(msg)
        
        cnxn2.commit()

        #time.sleep(5)
        
    except Exception as e:
        print('error:', e)

MSSQLtoMSSQL()
