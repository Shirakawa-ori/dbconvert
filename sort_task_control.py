#-*- coding:utf8 -*- -

import os
import sys
import operator
import cx_Oracle
import time

class colour_output():
    def red(self,s):
        print '\033[31;40m%s\033[0m' % s
    def green(self,s):
        print '\033[32;40m%s\033[0m' % s

class odbc():
    def __init__(self):
        connstr = 'phsconvert/phsconvert@192.168.88.190:1521/orcl'
        self.db=cx_Oracle.connect(connstr,encoding = "UTF-8", nencoding = "UTF-8")
        print 'Oracle database version: %s'% self.db.version
    def sqlSelect(self,sql):
        cr=self.db.cursor()
        print sql
        cr.execute(sql)
        retdesc = cr.description
        returnstr=cr.fetchall()
        cr.close()
        return retdesc,returnstr
    def __del__(self):
        self.db.close()
        
if __name__=='__main__':
    oracle = odbc()
    co = colour_output()

    filename = open('tablist')
    try:
        table_list_str = filename.read( )
    finally:
        filename.close( )
    
    table_list =  table_list_str.replace('\n','').split(',')
    numdic = {}
    for table in table_list:
        sql = 'select count(*) from %s' % table
        numdic[table] = int(oracle.sqlSelect(sql)[1][0][0])
    numlit = sorted(numdic.items(),key=operator.itemgetter(1),reverse=True)
    tasklist = []
    co.green('---------------------------------------------')
    for n in numlit:
        co.green(n[0] + ' , ' + str(n[1]))
        tasklist.append(n[0])
    co.green('---------------------------------------------')
    tasklen = len(tasklist)

    channel_number = 10

    channel_list = []
    for i in range(channel_number):
        channel_list.append([])
    index = 0
    for task in tasklist :
        channel_list[index % channel_number].append(task)
        index += 1
    co.green('\n\n\n\n\n\n---------------------------------------------')
    co.green('%d task' % tasklen)
    co.green('%d channel'%  channel_number)
    index =0
    for channel in channel_list:
        co.green('channel %d , task %d' % (index,len(channel)))
        index += 1
    co.green('---------------------------------------------')
    
    if raw_input('Input yes to continue:') == 'yes':
        pass
    else :
        co.red('stop')
        exit()

    index = 0
    for channel in channel_list:
        channel = ','.join(channel)
        cmd = 'nohup python dbconvert.py %s > log_dbconvert_channel_%d.log &' % (channel,index)
        co.red('\nchannel_%d cmd:\n %s' % (index,cmd))
        #raw_input('Press Anykey Start channel_'+str(index)+'')
        os.system(cmd)
        time.sleep(1)
        co.red('channel_%d Start\n' % index) 
        index += 1
