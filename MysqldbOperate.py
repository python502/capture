#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : MysqldbOperate.py
# @Software: PyCharm
# @Desc    :
import MySQLdb
from logger import logger
MYSQL_BATCH_NUM = 20
class MysqldbOperate(object):
    '''
    classdocs
    '''

    def __init__(self,dict_mysql):
        self.conn = None
        self.cur = None
        if not dict_mysql.has_key('host') or not dict_mysql.has_key('user') or not dict_mysql.has_key('passwd')\
         or not dict_mysql.has_key('db') or not dict_mysql.has_key('port'):
            logger.error('input parameter error')
            raise ValueError
        else:
            try:
                self.conn = MySQLdb.connect(host=dict_mysql['host'], user=dict_mysql['user'], \
                                    passwd=dict_mysql['passwd'], db=dict_mysql['db'],port=dict_mysql['port'],charset='utf8')
                self.cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
            except Exception,e:
                logger.error('__init__ fail:{}'.format(e))
                raise
    
    def __del__(self):
        self.cur.close()
        self.cur = None
        self.conn.close()
        self.conn = None
        
        
    def sql_query(self,sql,num=0):
        try:
            result = ()
            if not sql:
                raise ValueError('select sql not input')
            self.cur.execute(sql)
            if num == 0:
                result = self.cur.fetchall()
            elif num == 1:
                result = self.cur.fetchone()
            else:
                result = self.cur.fetchmany()
            return result
        except Exception, e:
            logger.error('sql_query error:{}'.format(e))
            logger.error('sql_query select sql:{}'.format(sql))
            raise

    def sql_exec(self, sql, value=''):
        if not sql:
            return False
        try:
            if value:
                self.cur.execute(sql,value)
            else:
                self.cur.execute(sql)
            self.conn.commit()
            return True
        except Exception, e:
            self.conn.rollback()
            logger.error('sql_exec error:{}'.format(e))
            return False
    '''
    function:insert_batch 批量插入数据#
    @param insert_sql:string  'INSERT INTO mtable(field1, field2, field3...) VALUES' or 'REPLACE INTO mtable(field1, field2, field3...) VALUES'
    @param datas:[[],[]]
    @return '检索结果'
    '''
        
    def insert_batch(self,insert_sql,datas):
        batch_list = []
        counts = 0
        sql = ''
        try:
            for item in datas:
                batch_list.append(self.__multipleRows(item))
                try:
                    if len(batch_list) == MYSQL_BATCH_NUM:
                        sql = "%s %s " % (insert_sql, ','.join(batch_list))
                        logger.debug('sql:{}'.format(sql))
                        self.cur.execute(sql)
                        self.conn.commit()
                        batch_list = []
                        counts += MYSQL_BATCH_NUM
                except Exception,e:
                    self.conn.rollback()
                    logger.error('sql:{}'.format(sql))
                    logger.error('e:{}'.format(e))
                    continue
            if len(batch_list):
                sql = "%s %s " % (insert_sql, ','.join(batch_list))
                self.cur.execute(sql)
                self.conn.commit()
            counts += len(batch_list)
            logger.info('finished {}: {}'.format(insert_sql[0], counts))
            if counts:
                return True
            else:
                return False
        except Exception, e:
            self.conn.rollback()
            logger.error('sql:{}'.format(sql))
            logger.error('e:{}'.format(e))
            return False
    # 返回可用于multiple rows的sql拼装值

    def __multipleRows(self,params):
        try:
            ret = []
            # 根据不同值类型分别进行sql语法拼装
            for param in params:
                if param == 0:
                    ret.append(str(param))
                    continue
                if not param:
                    ret.append('""')
                    continue
                if isinstance(param, (int, long, float, bool)):
                    ret.append(str(param))
                elif isinstance(param, (str, unicode)):
                    ret.append('"' + param.encode('utf8') + '"')
                else:
                    logger.error('unsupport value: '.format(param))
            return '(' + ','.join(ret) + ')'
        except Exception,e:
            logger.error('__multipleRows error:{}'.format(e))
            raise
def main():
    DICT_MYSQL={'host':'127.0.0.1','user':'root','passwd':'111111','db':'capture','port':3306}
    omysql = MysqldbOperate(DICT_MYSQL)
    sql = 'SELECT * FROM capture.website_servicepatent'
    print omysql.sql_query(sql, 1)
def main1():
    DICT_MYSQL={'host':'127.0.0.1','user':'root','passwd':'111111','db':'capture','port':3306}
    omysql = MysqldbOperate(DICT_MYSQL)
    sql = 'INSERT INTO website_servicepatent(metastasis_info, patent_id,create_date) VALUES'
    datas=[[1,2,'2016-07-12 21:14:38'],[4,'','2016-07-12 21:14:38'],[1,2,'2016-07-12 21:14:38'],[4,5,u'\20142016-07-12 21:14:38555'],[1,'','2016-07-12 21:14:38'],[1,2,'2016-07-12 21:14:38']]
    omysql.insert_batch(sql,datas)



def main2():
    DICT_MYSQL={'host':'127.0.0.1','user':'root','passwd':'111111','db':'capture','port':3306}
    omysql = MysqldbOperate(DICT_MYSQL)
    sql = 'INSERT INTO market_varify_raw(IMAGE_URL,VARIFY_CODE) VALUES (%s,%s)'
    data = ('1','d')
    omysql.sql_exec(sql , data)
if __name__ == '__main__':
    main2()