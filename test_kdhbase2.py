import random
import time
import sys
import thread
import kdhbase
import happybase

def listTables():
    connection = connection.Connection('192.168.7.131')
    connection.open()
    print(connection.tables())

def generatorValue():
    n=0
    while n<100:
        n = n + 1
        yield 'r%d'%n,'ok'

def g():
    v=''
    while True:
        if len(v)<1024:
            v='v%s'%random.sample('zyxwvutsrqponmlkjihgfedcba',1)[0]
        else:
            return v

#def test_put(table,rk,num):
#    start_time=time.time()
#    connection = kdhbase.Connection('dev-01',transport='buffered',protocol='binary')
#    table=connection.table(table)
#    n=0
#    wuhile n<1000:
#        n=n+1
#        rowkey='%s%d'%(rk,n)
#        table.put(rowkey,{'f1:t1':'a'*num})
#        print('put table:%s rowkey:%s'%(table,rowkey))
#    end_time=time.time()
#    print('time cost',end_time-start_time,'s')
#
def test_attributes_get(table,rk):
    start_time=time.time()
    connection = kdhbase.Connection('192.168.7.131',port=6666,transport='buffered',protocol='binary')
    table=connection.table(table)
    result = table.row(rk,attributes={'username':'hao','password':'hao'})
    print(result)
    end_time=time.time()
    print('time cost',end_time-start_time,'s')

def test_attributes_put(table,rk,data):
    start_time=time.time()
    connection = kdhbase.Connection('dev-01',port=6666,transport='buffered',protocol='binary')
    table=connection.table(table)
    table.put(rk,data,attributes={'username':'hao','password':'hao'})
    end_time=time.time()
    print('time cost',end_time-start_time,'s')

def test_attributes_scan(table,username,password):
    start_time=time.time()
    connection = kdhbase.Connection('dev-01',port=6666,transport='buffered',protocol='binary')
    table=connection.table(table)
    result = table.scan(attributes={'username':username,'password':password})
    print(result.next())
    end_time=time.time()
    print('time cost',end_time-start_time,'s')

def test_attributes_delete(table,rk):
    start_time=time.time()
    connection = kdhbase.Connection('dev-01',port=6666,transport='buffered',protocol='binary')
    table=connection.table(table)
    #result = table.row(rk,attributes={'username':'hao','password':'hao'})
    table.delete(rk,attributes={'username':'ha','password':'hao'})
    end_time=time.time()
    print('time cost',end_time-start_time,'s')

def test_attributes_deletecls(table,rk):
    start_time=time.time()
    connection = kdhbase.Connection('dev-01',port=6666,transport='buffered',protocol='binary')
    table=connection.table(table)
    #result = table.row(rk,attributes={'username':'hao','password':'hao'})
    table.delete(rk,columns={'data:data'},attributes={'username':'hao','password':'hao'})
    end_time=time.time()
    print('time cost',end_time-start_time,'s')

if __name__ == '__main__':

#    table = sys.argv[1]
#    username = sys.argv[2]
#    password = sys.argv[3]

    test_attributes_get('yue','rk1')
    #test_attributes_put('yue','rk100',{'data:data':'ok'})
    #test_attributes_delete(table,'rk1')
    #test_attributes_deletecls(table,'rk1')
