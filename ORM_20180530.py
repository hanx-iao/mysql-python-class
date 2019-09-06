import re
import os
import glob
import mysql.connector
import datetime


class Connect():
    def __init__(self, host, user, password, db, port):
        self.host=host
        self.password = password
        self.user=user
        self.db = db
        self.port = port
        self.cnx = mysql.connector.connect(
            host=self.host, user=self.user,password=self.password, db=self.db, port=self.port
               )
        self.cursor = self.cnx.cursor()

class Connect():
    def __init__(self, host, user, password, db, port):
        self.host=host
        self.password = password
        self.user=user
        self.db = db
        self.port = port
        self.cnx = mysql.connector.connect(host=self.host, user=self.user,
                                           password=self.password, db=self.db, port=self.port)
        self.cursor = self.cnx.cursor()

    def close(self):
        if self.cnx:
            self.cnx.close()

    def alter_column(self, table, column, feture):
        # alter table mytable alter column name varchar(255) null not null DEFAULT ''
        sql = 'alter table '+ table + ' CHANGE column '+ column + ' '+ feture
        print(sql)
        self.cursor.execute(sql)

    # UPDATE `gsa_record` SET rs2='TT' rs1='AA' WHERE gsa_id=1
    # UPDATE table_name SET column1 = value1, column2 = value2, ...WHERE some_column = some_value;
    def update(self, tbname, data, condition):
        _fields=[]
        _prefix=''.join(['UPDATE `', tbname, '`', ' SET '])
        for key in data.keys():
            strings = key + "=" + "'" + self.cnx.converter.escape(data[key]) + "'"
            _fields.append(strings)
        # print( (_fields) )
        # print(_prefix)
        _sql="".join([_prefix, ','.join(_fields), " WHERE ", condition ])
        # print(_sql)
        self.cursor.execute(_sql)
        self.cnx.commit()
        return

    def delete(self, tbname, condition):
        _prefix="".join(['DELETE FROM  `',tbname,'`',' WHERE '])
        _sql="".join([_prefix, condition])
        self.cursor.execute(_sql)
        self.cnx.commit()

    def select(self,sql):
        self.cursor.execute(sql)
        all = self.cursor.fetchall()
        return all

    def show_table(self,table):
        self.cursor.execute('desc ' + table)

    def total_number(self,table):
        _sql = "select count(*) from "+table
        self.cursor.execute(_sql)

    def add_data(self, table_name, data):
        columns=data.keys()
        _prefix="".join(['INSERT INTO `',table_name,'`'])
        _fields=",".join(["".join(['`',column,'`']) for column in columns])
        _values=",".join(["%s" for i in range(len(columns))]) #n多个s%
        _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"  ])
        # print(_sql)
        _params=[data[key] for key in columns]
        # print(_params)
        self.cursor.execute(_sql, tuple(_params))
        self.cnx.commit()

    def del_content(self):
        self.cursor.execute('delete from gsa_record')
        self.cnx.commit()

    def insert(self):
        self.cursor.execute('INSERT INTO `gsa_record`(gsa_id, `gsa_file`,`gsa_path`,`rs1`,`rs2`) VALUES (663,1,3,4,5)')
        self.cnx.commit()

    def query(self, table_name , data, column):
        columns = column
        _prefix = "".join(['SELECT * FROM `', table_name, '`'])
        _fields = " AND ".join(["".join(['`', column, '` = %s']) for column in columns])
        _sql = "".join([_prefix, " WHERE ","(", _fields, ")" ])
        # print(_sql)
        _params = [data[key] for key in columns]
        # print(_sql,_params)
        self.cursor.execute(_sql ,_params)
        return self.cursor.fetchall()

    def add_column(self, table_name, rs):
        # alter table gsa_record add (rs144 char(20) null,rs12 char(20) null);
        _prefix = ''.join(['alter table ',table_name, ' add '])
        columns = ','.join(' `%s`  varchar(3) null' for i in range(len(rs)) ) % tuple(rs)
        _sql = ''.join([_prefix, '(', columns, ')'])
        print(_sql)
        self.cursor.execute(_sql)
        self.cnx.commit()

    def remove_repeat(self, table):
        _sql = 'delete from ' +table + ' where id in (select id from (select id from ' +\
                                       'medicine_sample_info group by megaNo+sampleNo having count(*)>1) as x\
        and id not in (select id from (select min(id) from medicine_sample_info group by megaNo+sampleNo having count(*) > 1) as y)'
        self.cursor.execute(_sql)

    def del_all(self, table_name):
        sql = 'truncate table ' +  table_name
        self.cursor.execute(sql)
        self.cnx.commit()
        print(table_name+'  clear!')

    def get_column_name():
        columns = []
        sql = ''.join(['select * from information_schema.COLUMNS where table_name = "rs_lack_or_not"'])
        for i in (ok.select(sql)):
            columns.append(i[3])
        return columns

    def test(self,sql): # '.*\(..\...\%\)'
        print(sql)
        self.cursor.execute(sql)
        self.cnx.commit()


    def update_back(self, tbname, data, condition):
        _fields = []
        _prefix = ''.join(['UPDATE `', tbname, '`', ' SET '])
        for key in data.keys():
            strings = str(key) + "=" + "'" + str(self.cnx.converter.escape(data[key])) + "'"
            _fields.append(strings)
        #print((_fields))
        #print(_prefix)
        _sql = _prefix + ','.join(_fields) + ''.join([" WHERE ", condition])
        self.cursor.execute(_sql)
        self.cnx.commit()
        return

if __name__ == '__main__'  :
    ok = Connect('172.16.0.0', 'root', '', 'produce', '1994')
    ok.add_data('', dic) 

