## June 2 2016 9:14 AM

# MiniMySQL-MysqlORM
Based on the MySQLdb provide formatting Mysql light-weight ORM

基于MySQLdb提供格式化操作Mysql的轻量级ORM

* * *

## function/系统功能

By joining together the SQL statement, lightweight MySQL ORM  

通过拼接SQL语句，实现轻量级MySQL ORM

## examples/使用范例

- database connection/数据库连接
```
mysql_handle = MysqlHandleBase(mysql_host='127.0.0.1', mysql_user='root', mysql_password='',mysql_db='')
```
- Query example/查询举例
```
table_name = 'gray_list'
fields = ['url']
select_result = mysql_handle.select(
    table_name, fields=fields, fetch_type='all')
print select_result
```
```
sql = "SELECT * FROM followers_big ORDER BY id LIMIT 10"
select_result = mysql_handle.select(sql=sql, fetch_type='all')
print select_result
```
```
table_name = 'server_live'
engine_type = '01'
print mysql_handle.select(table_name, fields=['engine_num'], wheres={'type': [engine_type, 's']}, fetch_type='one', orders='engine_num DESC')
```

- Update example/更新举例
```
table_name = 'task_result'
fields = {'task_state': [2, 'd']}  # wait to update fields
wheres = {'task_id': [3, 'd'], 'start_time': ['2015-06-11 11:22:53', 's']}
result = mysql_handle.update(table_name, fields, wheres)
print result
```

- Insert example/插入举例
```
table_name = 'task_info'
fields = {'task_id': [54, 'd'], 'task_type': [
    2, 'd'], 'task_engine': ['01-02', 's']}  # wait to insert fields
result = mysql_handle.insert(table_name, fields)
print result
```

- Del example/删除举例
```
table_name = 'task_info'
wheres = {'task_id': [46, 'd']}  # select condition
result = mysql_handle.delete(
    table_name, wheres=wheres)
print result
```

- Batch insert example/批量插入举例
```
table_name = 'followers_big'
fields = [('url_hash', 's'), ('url', 's')]
param = (('1111','www.baidu.com'), ('2222','www.baidu2.com'))
# fields = [('url_hash', 's')]
# param = (('1111',), ('2222',))
print mysql_handle.batch_insert(table_name, fields, param)
```

- Batch del example/批量删除举例
```
table_name = 'followers_big'
wheres_list = [{'id': [154, 'd']}, {'id': [155, 'd']}]
print mysql_handle.batch_delete(table_name, wheres_list)
```

- Update example/批量更新举例
```
table_name = 'followers_big'
fields_list = [{'uid': [1, 's']}, {'uid': [2, 's']}]
wheres_list = [{'id': [154, 'd']}, {'id': [155, 'd']}]
print mysql_handle.batch_delete(table_name, fields_list, wheres_list)
```

## setup/安装配置
```
sudo apt-get install mysql-server
apt-get install python-mysqldb
```

## contact/联系方式


609610350@qq.com
