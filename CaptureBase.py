#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 14:06
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : CaptureBase.py
# @Software: PyCharm
# @Desc    :


import urllib2
import zlib
from MysqldbOperate import MysqldbOperate
from logger import logger
from retrying import retry

DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
# DICT_MYSQL = {'host': '118.193.21.62', 'user': 'root', 'passwd': 'Avazu#2017', 'db': 'avazu_opay', 'port': 3306}
'''
classdocs
'''
class CaptureBase(object):
    def __init__(self, user_agent, proxy_ip=None):
        '''
        Constructor
        '''
        self.user_agent = user_agent
        self.proxy_ip = proxy_ip
        self.mysql = MysqldbOperate(DICT_MYSQL)
        if self.proxy_ip:
            proxy = urllib2.ProxyHandler(proxy_ip)
            opener = urllib2.build_opener(proxy)
            urllib2.install_opener(opener)


    def __del__(self):
        if self.mysql:
            del self.mysql

    '''
    function: urlopen 失败时进行retry, retry3次 间隔2秒
    @request: request
    @return: con or exception
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def __urlOpenRetry(self, request):
        # if isinstance(request, basestring):
        try:
            con = urllib2.urlopen(request, timeout=10)
            return con
        except Exception, e:
            logger.error('urlopen error retry.e: {}'.format(e))
            raise

    '''
    function: getHtml 根据url获取页面的html
    @url:  url
    @return: html
    '''
    def getHtml(self, url, header, data=None):
        if not header:
            logger.error('header is None error')
            raise ValueError()
        try:
            req = urllib2.Request(url=url, headers=header, data=data)
            con = self.__urlOpenRetry(req)
            if 200 == con.getcode():
                doc = con.read()
                if con.headers.get('Content-Encoding'):
                    doc = zlib.decompress(doc, 16+zlib.MAX_WBITS)
                con.close()
                logger.debug('getHtml: url:{} getcode is 200'.format(url))
                return doc
            else:
                logger.debug('getHtml: url:{} getcode isn\'t 200,{}'.format(url, con.getcode()))
                raise ValueError()
        except Exception,e:
            logger.error('getHtml error: {}.'.format(e))
            raise

    '''
    function: get_html 根据str header 生成 dict header
    @strsource:  str header
    @return: dict header
    '''
    def _getDict4str(self, strsource, match=':'):
        outdict = {}
        lists = strsource.split('\n')
        for list in lists:
            list = list.strip()
            if list:
                strbegin = list.find(match)
                outdict[list[:strbegin]] = list[strbegin+1:] if strbegin != len(list) else ''
        return outdict

    def _rm_duplicate(self, scr_datas, match):
        goods = {}
        repeat_num = 0
        for data in scr_datas:
            if goods.get(data[match]):
                logger.debug('find repead data: {}'.format(data[match]))
            else:
                goods[data[match]] = data
                repeat_num += 1
        return [value for value in goods.itervalues()]

    def _checkDatas(self, select_sql, sourcedatas, columns):
        insert_datas=[]
        update_datas=[]
        for sourcedata in sourcedatas:
            sql = select_sql.format(**sourcedata)
            logger.debug('select sql: {}'.format(sql))
            try:
                result = self.mysql.sql_query(sql)
                if not result:
                    insert_datas.append(sourcedata)
                else:
                    if len(result) != 1:
                        logger.error('checkHomeDatas get many lines:{}'.format(result))
                        logger.error('select_sql: {}'.format(sql))
                    for column in columns:
                        sourcedata[column.lower()] = result[0].get(column.upper())
                    update_datas.append(sourcedata)
            except Exception, e:
                logger.error('__checkDatas\'s error: {}.'.format(e))
                logger.error('__checkDatas\'s sourcedata: {}.'.format(sourcedata))
                continue
        return (insert_datas,update_datas)