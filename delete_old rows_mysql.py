
import mysql.connector
import datetime
import sys
import time

mysqluser="newuser"
Password="password"
DBname="test"
tablename="cel"
maxsize=8
event_log="logfile"


def log_append(lfile,msg):
    logfile=open(lfile,'a')
    logfile.write("\n")
    logfile.write(msg)
    logfile.close()


try :
    dataBase = mysql.connector.connect( 
                     host = "localhost", 
                     user = mysqluser, 
                     passwd = Password, 
                     database = DBname ) 


    cursorObject = dataBase.cursor() 

    
    while True :
        
        
        Query="""SELECT TABLE_NAME AS "Table",ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)" FROM information_schema.TABLES WHERE table_schema = "test" and TABLE_NAME = "%s"
""" %(tablename)
        print(Query)
        cursorObject.execute(Query) 

        myresult = cursorObject.fetchall() 
        tablesize=float(myresult[0][1])
        print(tablesize)
    
        topdateq="""select  eventtime from %s order by eventtime limit 1""" %(tablename)
    
        cursorObject.execute(topdateq) 
    
        topdater = cursorObject.fetchall() 
    
        enddate=topdater[0][0]+datetime.timedelta(days=1) 
        
        if tablesize > maxsize :
            
            deleten="""select COUNT(*) from %s where eventtime < '%s'""" %(tablename,enddate)
            print(deleten)
            cursorObject.execute(deleten)
        
            drows = cursorObject.fetchall()[0][0]
            
            deleteq="""DELETE from %s where eventtime < '%s'""" %(tablename,enddate)

            try :
                
                cursorObject.execute(deleteq)
             #   dataBase.close()
                time.sleep(2.4)
                logline=str(datetime.datetime.now())+' '+"table size is %s and is higher than threshold value %s , deleting %s enties before %s" %(tablesize,maxsize,drows,enddate)
                print(logline)
                log_append(event_log,logline)
                
            except mysql.connector.Error as err:
                
                logline=str(datetime.datetime.now())+' '+str(err.sqlstate)+' '+str(err.msg)
                log_append(event_log,logline)

                break
            
        else :
        
            logline=str(datetime.datetime.now())+' '+"table size is %s and is lower that threshold value %s" %(tablesize,maxsize)
            print(logline)
            log_append(event_log,logline)
            break
            
except mysql.connector.Error as err:

    print("Message", err.msg)

    logline=str(datetime.datetime.now())+' '+str(err.sqlstate)+' '+str(err.msg)
    log_append(event_log,logline)
    
cursorObject.close()
dataBase.close()
