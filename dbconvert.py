#-*- coding:utf8 -*- -
import os
import sys
from sys import argv
import time
import cx_Oracle
import MySQLdb
from warnings import filterwarnings

class pylog():
    def __init__(self):
        self.success = 'success.log'
        self.fail = 'fail.log'
        self.wr = 'wr.log'
    def logsuccess(self,s):
        with open(self.success,'a') as f:
            f.write('%s\n') % str(s)
    def logfail(self,s):
        with open(self.self.fail,'a') as f:
            f.write('%s\n') % s
    def logwr(self,s):
        with open(self.self.wr,'a') as f:
            f.write('%s\n') % s

class colour_output():
    def red(self,s):
        print '\033[31;40m%s\033[0m' % s
    def green(self,s):
        print '\033[32;40m%s\033[0m' % s

def str2timetuple(date):
    #时间转换函数
    time_format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(date, time_format)

def conver(table):
    #sql = 'select * from "%s" ' % table
    sql = 'select * from %s ' % table
    try: 
        row_list = oracle.sqlSelect(sql)
    except Exception ,e:
        co.red('------------------------------------------------')
        co.red('|--TAB Output--| %s' % table)
        co.red('|--SQL Output--| %s' % sql)
        co.red('|--TAB  FAIL --| Error : %s' % e)
        co.red('------------------------------------------------')
        #exit()
        return 0
    desclist = []
    for desc in row_list[0]:
        desclist.append(desc[0])
    dataLen = len(row_list[1])
    co.green('------------------------------------------------')
    co.green('|--TAB Output--| %s' % table)
    co.green('|--SQL Output--| %s' % sql)
    co.green('|--TAB  INFO --| dataLen: %d' % dataLen)
    co.green('------------------------------------------------')
    cycle_count = 0
    success  = 0
    fail = 0
    keyword_list = []
    for row in row_list[1]:
        rowkv = dict(zip(desclist,row))
        for k, v in rowkv.items():
            if len(keyword_list) > 0:
                if k in keyword_list:
                    rowkv['`%s`' % k] = rowkv.pop(k)
                    k = '`%s`' % k
                else :
                    pass
            else :
                rowkv['`%s`' % k] = rowkv.pop(k)
                k = '`%s`' % k
            if not v:
                rowkv.pop(k)
            elif type(v) =='datetime.datetime':
               rowkv[k] = str(str2timetuple(v)).decode('GBK').encode('UTF-8')
               #rowkv[k] = str(str2timetuple(v))
            else :
               try :
                   rowkv[k] = str(v).decode('GBK').encode('UTF-8')
                   #rowkv[k] = v
               except Exception,e:
                   rowkv[k] = v
        #exit(0)
        cycle_count += 1
        qmarks = ', '.join(['%s'] * len(rowkv))
        cols = ', '.join(rowkv.keys())
        #sql = 'INSERT INTO `%s` (%s) VALUES (%s)' % (table, cols, qmarks)
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, cols, qmarks)
        #print sql,tuple(rowkv.values())       
        #sof = 'success'
        sof = mysql.insert(sql,tuple(rowkv.values()))
        if sof == 'success':
            co.green('table %s row %d success , left %d' %(table,cycle_count,dataLen-cycle_count))
            success += 1
        else :
            co.red('table %s row %d FAIL: %s , left %d' % (table,cycle_count,str(sof),dataLen-cycle_count))
            fail += 1
        #return 1
    co.green('TABLE: %s Done , SUCCESS %d , FAIL %d' %(table,success,fail))
    print '\n'
    return 1

class mbdc():
    def __init__(self):
        self.connection =  MySQLdb.connect(host='127.0.0.1',port=3306,user='phs',passwd='phs',db='phs3_test',charset='utf8')
        self.cursor = self.connection.cursor()
    def insert(self, query, params):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            return 'success'
        #except MySQLdb.Warning, w:
            #return str(w)
        except Exception, e:
            self.connection.rollback()
            return e
    def query(self, query, params):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query, params)
        return cursor.fetchall()
    def select(self, sql):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sql)
        return cursor.fetchall()
    def version(self):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("SELECT VERSION()")
        ver = cursor.fetchone()
        print 'MySQL database version: %s' % ver['VERSION()']
        return ver
    def __del__(self):
        self.connection.close()

class odbc():
    def __init__(self):
        connstr = 'phsconvert/phsconvert@192.168.88.190:1521/orcl'
        #self.db=cx_Oracle.connect(connstr,encoding = "UTF-8", nencoding = "UTF-8")
        self.db=cx_Oracle.connect(connstr)
        print 'Oracle database version: %s'% self.db.version
    def sqlSelect(self,sql):
        cr=self.db.cursor()
        cr.execute(sql)
        retdesc = cr.description
        returnstr=cr.fetchall()
        cr.close()
        return retdesc,returnstr
    def __del__(self):
        self.db.close()
        
if __name__=='__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
    #filterwarnings('error', category = MySQLdb.Warning)
    print '--------------------init--------------------'
    oracle = odbc()
    mysql = mbdc()
    mysql.version()
    co = colour_output()
    pl = pylog()
    try:
        table_list_str = argv[1]
        co.green('Get Arg %s' % table_list_str)
    except:
        co.green('NO Arg read tablist')
        tablist = open('tablist')
        try:
            table_list_str = tablist.read()
        except Exception ,e:
            co.red('Open tablist Fail : %s') % str(e)
        finally:
            tablist.close()
    table_list = table_list_str.replace('\n','').split(',')
    success_list = []
    fail_list = []
    tables_success = 0
    tables_fail = 0
    print '--------------------------------------------\n\n\n'

    time.sleep(1)
    #os.system('clear')
    time.sleep(1)

    try :
        for table in table_list:
            co.green('convert %s ...' % table)
            if (conver(table)):
                tables_success += 1
                success_list.append(table)
            else :
                tables_fail += 1
                fail_list.append(table)
            #exit()
        co.green('\n\n\n--------------------DONE--------------------')
        print ''
        co.green('DB convert SUCCESS , SUCCESS : %d , FAIL : %d' % (tables_success,tables_fail))
        print ''
        co.green('--------------------------------------------\n')
        co.green('SCCESS tables : %s' % str(success_list))
        print '\n'
        co.red('FAIL tables : %s' % str(fail_list))
    except Exception,e :
        co.red('\n\n\n--------------------%s--------------------' % str(e))
        print ''
        co.red('DB convert ERROR , SUCCESS : %d , FAIL : %d' % (tables_success,tables_fail))
        print ''
        co.red('--------------------------------------------\n')
        co.green('SCCESS tables : %s' % str(success_list))
        print '\n'
        co.red('FAIL tables : %s' % str(fail_list))
