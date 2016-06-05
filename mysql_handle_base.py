# -*- coding: utf-8 -*-
'''
name: 格式化拼接SQL语句 v2.2  https://github.com/xinyi-spark/MiniORM-MySQL
Author：XinYi 609610350@qq.com
Time: 2015.9.3

输入：
    mysql_db：               数据库名称
    mysql_host:              数据库所在ip
    mysql_user：             数据库登录用户名
    mysql_password：         数据库登录密码
功能：
    提供操作mysql的接口，对外界屏蔽SQL语句，使操作MySql时可以只关注要操作的数据，
    为对MySQLdb库的二次封装
接口：
    select, insert, update, delete, batch_insert, batch_update, batch_delete
    具体使用方法看源码中函数介绍
举例：
    例子在if __name__ == '__main__'中，例如：
    table_name = 'gray_list'
    fields = ['id', 'url']
    select_result = mysql_handle.select( table_name, fields., fetch_type='all')
'''

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s-[%(asctime)s][%(module)s][%(funcName)s][%(lineno)d]: %(message)s')

import sys
import re
import time
import traceback
import MySQLdb
import MySQLdb.cursors

reload(sys)
sys.setdefaultencoding('utf8')


class MySQLError(Exception):  # mysql数据库存储错误格式化输出

    def __init__(self, dep):
        self.dep = dep

    def __str__(self):
        try:
            return "Mysql Error %d: %s" % (self.dep.args[0], self.dep.args[1])
        except IndexError:
            return "%s  Mysql Error %s" % (time.ctime(), self.dep.args)


def deal_other_error():
    '''
    focus deal other error, print error info
    处理除mysql错误外的其他错误，打印出错的参数
    '''
    def _deco(func):
        def __deco(self, *args):
            try:
                return func(self, *args)
            except:
                logger.error('*args: ' + str(args))
                traceback.print_exc()
                return False
        return __deco
    return _deco


def deal_mysql_error():
    '''
    focus deal mysql error, print error info
    处理mysql错误，打印出错的sql语句
    '''
    def _deco(func):
        def __deco(self, *args):
            try:
                return func(self, *args)
            except MySQLdb.Error, e:
                logger.error(MySQLError(e))
                traceback.print_exc()
                raise
        return __deco
    return _deco


class MysqlHandleBase():

    def __init__(self, mysql_host='127.0.0.1', mysql_user='root',
                 mysql_password='', mysql_db='test'):
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_db = mysql_db

    def __del__(self):
        '''
        断开连接
        '''
        self.close_connnection()

    @deal_mysql_error()
    def connect_MySQL(self):
        '''
        连接mysql数据库
        '''
        self.db_conn = MySQLdb.connect(
            self.mysql_host, self.mysql_user, self.mysql_password,
            self.mysql_db, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
        # python MySQLdb 默认关闭了 autocommit，如果查询的是一个 innodb 表的话，
        # 一旦该表后面插入了新数据，之前的连接就会查不到新数据
        # 所以根据情况，一般情况下最好开启 autocommit
        self.db_conn.autocommit(True)
        logger.debug('connect mysql win, ip: %s' % self.mysql_host)
        return True

    @deal_mysql_error()
    def close_connnection(self):
        '''
        断开数据库连接
        '''
        try:
            self.db_conn.close()
        except:
            pass
        return True

    def sql_escape(self, string):
        '''
        普通sql语句中特殊字符处理
        '''
        string = re.sub("'", "''", string)
        return string

    def update_sql_escape(self, string):
        '''
        update sql语句中特殊字符处理
        '''
        string = self.sql_escape(string)
        string = re.sub("%", "%%", string)
        return string

    def like_sql_escape(self, string):
        '''
        LIKE语句中特殊字符处理，模块暂不支持LIKE语句
        '''
        pass

    @deal_other_error()
    def select_sql(self, table_name, fields, wheres, orders='', limits=''):
        '''
        structure select sql sentence
        fields = ['task_state', 'task_type']
        wheres = {'task_id': [43, 'd']}
        '''
        sql = "select"
        wheres_format = {}
        for field in fields:
            sql += ' ' + field + ','
        sql = sql[:-1] + ' from ' + table_name
        if wheres != {}:
            sql = sql + ' where '
            for key in wheres:
                sql += key + '=%(' + key + ')' + wheres[key][1] + ' and '
                if wheres[key][1] == 's':
                    wheres_format[key] = '\'' + \
                        self.sql_escape(str(wheres[key][0])) + '\''
                else:
                    wheres_format[key] = wheres[key][0]
            sql = sql[:-5] % wheres_format
            if orders != '':
                sql = sql + ' order by ' + orders
            if limits != '':
                sql = sql + ' limit ' + limits
        return sql

    @deal_other_error()
    def update_sql(self, table_name, fields, wheres):
        '''
        structure update sql sentence
        fields = {'task_type': [2, 'd']}
        wheres = {'task_id': [43, 'd']}
        '''
        sql = "update"
        fields_format = {}
        wheres_format = {}
        sql += ' ' + table_name + ' set'
        for field in fields:
            sql += ' ' + field + \
                '=%(' + field + ')' + fields[field][1] + ','
            if fields[field][1] == 's':
                fields_format[field] = '\'' + \
                    self.update_sql_escape(str(fields[field][0])) + '\''
            else:
                fields_format[field] = fields[field][0]
        sql = (sql[:-1] + ' where ') % fields_format
        for key in wheres:
            sql += key + '=%(' + key + ')' + wheres[key][1] + ' and '
            if wheres[key][1] == 's':
                wheres_format[key] = '\'' + \
                    self.sql_escape(str(wheres[key][0])) + '\''
            else:
                wheres_format[key] = wheres[key][0]
        sql = (sql[:-5]) % wheres_format
        return sql

    @deal_other_error()
    def insert_sql_format(self, table_name, fields):
        '''
        structure insert sql sentence
        原始sql构建，例如："insert into user(name,created) values(%s,%s)"
        fields = [('uid', 's'), ('name', 's)]
        '''
        sql = "insert into"
        values = ''
        sql += ' ' + table_name + ' ('
        for field in fields:
            sql += field[0] + ','
            values += '%' + field[1] + ','
        sql = (sql[:-1] + ') values(' + values[:-1] + ')')
        return sql

    @deal_other_error()
    def insert_sql(self, table_name, fields):
        '''
        structure insert sql sentence
        fields = {'task_id': [58, 'd'], 'task_type': [2, 'd']}
        '''
        sql = "insert into"
        values = ''
        fields_format = {}
        sql += ' ' + table_name + '('
        for field in fields:
            sql += field + ','
            values += '%(' + field + ')' + fields[field][1] + ','
            if fields[field][1] == 's':
                fields_format[field] = '\'' + \
                    self.sql_escape(str(fields[field][0])) + '\''
            else:
                fields_format[field] = fields[field][0]
        sql = (sql[:-1] + ') values(' + values[:-1] + ')')
        sql = sql % fields_format
        return sql

    @deal_other_error()
    def delete_sql(self, table_name, wheres):
        '''
        structure delete sql sentence
        fields = {'task_type': [2, 'd']}
        wheres = {'task_id': [43, 'd']}
        '''
        sql = "delete"
        wheres_format = {}
        sql += ' from ' + table_name + ' where '
        for key in wheres:
            sql += key + '=%(' + key + ')' + wheres[key][1] + ' and '
            if wheres[key][1] == 's':
                wheres_format[key] = '\'' + \
                    self.sql_escape(str(wheres[key][0])) + '\''
            else:
                wheres_format[key] = wheres[key][0]
        sql = sql[:-5] % wheres_format
        return sql

    def operate_mysql(self, sql=None, param=None, require_type='post', return_id=False, fetch_type='one'):
        '''
        执行sql语句，并进行mysql错误处理
        sql: 待执行sql语句，可为字符串或列表，需为str类型，不能为unicode等
        param: 与executemany函数配合使用，一次插入多条记录时的数据元祖
        require_type: 操作类型，分为post和get
        return_id：是否返回最后插入行的主键ID
        '''
        try:
            self.connect_MySQL()
            self.cur = self.db_conn.cursor()
            if require_type == 'get':
                self.cur.execute(sql)
                if fetch_type == 'one':
                    results = self.cur.fetchone()
                elif fetch_type == 'all':
                    results = self.cur.fetchall()
                self.cur.close()
                return results
            elif require_type == 'post':
                if not isinstance(sql, list):
                    sql = [sql]
                for once_sql in sql:
                    if param is None:
                        self.cur.execute(str(once_sql))
                    else:  # 　当批量插入时使用executemany
                        self.cur.executemany(str(once_sql), param)
                    if return_id == True:
                        first_insert_id = int(self.cur.lastrowid)  # 最后插入行的主键ID
                self.db_conn.commit()
                self.cur.close()
                if return_id == True:
                    return [True, first_insert_id]
                return True
            return False
        except MySQLdb.Error, e:
            # 1062错误：Duplicate entry XX for key 'PRIMARY，说明目标记录已插入，主键冲突。
            # 上次错误可能为2006或2013，即mysql链接断开，但指令已执行成功，
            if e.args[0] != 2006 and e.args[0] != 2013:
                logger.error('operate_mysql error, sql: %s, param: %s, require_type: %s, return_id: %s, fetch_type: %s' % (
                    sql, param, require_type, return_id, fetch_type))
                traceback.print_exc()
                return False
            # 连接MySQL服务器超时，则重新连接，如果重新连接失败，说明数据库出现其他问题，则退出程序
            re_connect_result = self.connect_MySQL()
            if re_connect_result is True:
                return self.operate_mysql(sql, param, require_type, return_id, fetch_type)
            else:
                return False
        finally:
            self.close_connnection()

    def select(self, table_name=None, fields=[], wheres={}, sql=None, fetch_type='one', orders='', limits=''):
        '''
        查询操作，有两种工作方式：
        1、sql：待执行sql语句，需为str类型，不能为unicode等
           field：（可选）对查询结果进行格式化，如：['task_name']
        2、table_name：待查询表名
           field：（必选）待查询字段，并对查询结果进行格式化
           wheres：构造查询sql语句，如：{'task_id': [1, 'd']}
        公用参数：
        fetch_type: 查询后返回数量，one表示返回一条，all表示返回所有查询结果
        orders: 排序，例如：'order_field ASC', 'order_field DESC'
        limits: 返回个数限制，例如：'5'
        '''
        if sql is None:
            sql = self.select_sql(table_name, fields, wheres, orders, limits)
            if sql is False:
                return False
        results = self.operate_mysql(
            sql, require_type='get', fetch_type=fetch_type)
        if results is None or results == ():
            return False
        return results

    def insert(self, table_name=None, fields={}, sql=None, return_id=False):
        '''
        插入操作，有两种工作方式：
        1、sql：待执行sql语句
        2、table_name：待查询表名
           field：待插入字段，如：{'task_id': [1, 'd']}
        '''
        if sql is None:
            sql = self.insert_sql(table_name, fields)
            if sql is False:
                return False
        return self.operate_mysql(sql, return_id=return_id)

    def batch_insert(self, table_name=None, fields=[], param=None, return_id=False):
        '''
        批量插入操作，通过为executemany函数提供param参数实现。
        table_name：待查询表名
        field：待插入字段说明，如：[('uid', 's'), ('name', 's)]
        param: 一次插入多条记录时的数据元祖，例如(('2',), ('3',))，
               insert_sql函数得到的sql语句为：
               "insert into user(name,created) values(%s,%s)"，此时需提供param
        '''
        sql = self.insert_sql_format(table_name, fields)
        if sql is False or param is None:
            return False
        return self.operate_mysql(sql, param=param, return_id=return_id)

    def update(self, table_name=None, fields={}, wheres={}, sql=None):
        '''
        更新操作，有两种工作方式：
        1、sql：待执行sql语句
        2、table_name：待查询表名
           field：待插入字段，如：{'task_id': [1, 'd']}
           wheres：构造查询sql语句，如：{'task_id': [1, 'd']}
        '''
        if sql is None:
            sql = self.update_sql(table_name, fields, wheres)
            if sql is False:
                return False
        return self.operate_mysql(sql)

    def batch_update(self, table_name, fields_list=[], wheres_list=[]):
        '''
        批量更新，只能通过格式化sql的方式工作
        table_name：待查询表名
        fields_list：待插入字段，如：[{'task_id': [1, 'd']}, {'task_id': [2, 'd']}]
        wheres_list：构造查询sql语句，如：[{'task_id': [1, 'd']}, {'task_id': [2, 'd']}]
        多次调用update后仅调用commit一次。
        '''
        sql_list = []
        for (fields, wheres) in zip(fields_list, wheres_list):
            sql = self.update_sql(table_name, fields, wheres)
            if sql is not False:
                sql_list.append(sql)
        return self.operate_mysql(sql_list)

    def delete(self, table_name, wheres={}, sql=None):
        '''
        删除操作，有两种工作方式：
        1、sql：待执行sql语句
        2、table_name：待查询表名
           wheres：构造查询sql语句，如：{'task_id': [1, 'd']}
        '''
        if sql is None:
            sql = self.delete_sql(table_name, wheres)
            if sql is False:
                return False
        return self.operate_mysql(sql)

    def batch_delete(self, table_name, wheres_list=[]):
        '''
        批量删除，只能通过格式化sql的方式工作
        table_name：待查询表名
        wheres_list：构造查询sql语句，如：[{'task_id': [1, 'd']}, {'task_id': [2, 'd']}]
        多次调用update后仅调用commit一次。
        '''
        sql_list = []
        for wheres in wheres_list:
            sql = self.delete_sql(table_name, wheres)
            if sql is not False:
                sql_list.append(sql)
        return self.operate_mysql(sql_list)


if __name__ == '__main__':
    mysql_handle = MysqlHandleBase(mysql_host='127.0.0.1', mysql_user='root', mysql_password='',
                                   mysql_db='')
    # 查询举例
    '''
    table_name = 'gray_list'
    fields = ['url']
    select_result = mysql_handle.select(
        table_name, fields=fields, fetch_type='all')
    print select_result
    '''
    '''sql = "SELECT * FROM followers_big ORDER BY id LIMIT 10"
    select_result = mysql_handle.select(sql=sql, fetch_type='all')
    print select_result
    '''
    '''
    table_name = 'server_live'
    engine_type = '01'
    print mysql_handle.select(table_name, fields=['engine_num'], wheres={'type': [engine_type, 's']}, fetch_type='one', orders='engine_num DESC')
    '''

    # 更新举例
    '''
    table_name = 'task_result'
    fields = {'task_state': [2, 'd']}  # wait to update fields
    wheres = {'task_id': [3, 'd'], 'start_time': ['2015-06-11 11:22:53', 's']}
    result = mysql_handle.update(table_name, fields, wheres)
    print result
    print '_______________________________________________'
    '''
    # 插入举例
    '''
    table_name = 'task_info'
    fields = {'task_id': [54, 'd'], 'task_type': [
        2, 'd'], 'task_engine': ['01-02', 's']}  # wait to insert fields
    result = mysql_handle.insert(table_name, fields)
    print result
    print '_______________________________________________'
    '''
    # 删除举例
    '''
    table_name = 'task_info'
    wheres = {'task_id': [46, 'd']}  # select condition
    result = mysql_handle.delete(
        table_name, wheres=wheres)
    print result
    print '_______________________________________________'
    '''

    # 批量插入举例
    '''
    table_name = 'followers_big'
    fields = [('url_hash', 's'), ('url', 's')]
    param = (('1111','www.baidu.com'), ('2222','www.baidu2.com'))
    # fields = [('url_hash', 's')]
    # param = (('1111',), ('2222',))
    print mysql_handle.batch_insert(table_name, fields, param)

    '''

    # 批量删除举例
    '''
    table_name = 'followers_big'
    fields_list = [{'uid': [1, 's']}, {'uid': [2, 's']}]
    wheres_list = [{'id': [154, 'd']}, {'id': [155, 'd']}]
    print mysql_handle.batch_delete(table_name, wheres_list)
    '''
