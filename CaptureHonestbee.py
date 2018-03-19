#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureHonestbee.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
import os
from CaptureBase import CaptureBase
import re
import json
import time
import copy
import gevent
import thread
import ast
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from retrying import retry
from datetime import datetime
from urlparse import urljoin

lock = thread.allocate_lock()

class CaptureHonestbee(CaptureBase):
    home_url = 'https://www.honestbee.sg/en/groceries/stores/fresh-by-honestbee'
    HEADER_HOME = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Connection:keep-alive
            Host:www.honestbee.sg
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    HEADER_DEPARTMENTS = '''
            Accept:application/vnd.honestbee+json;version=2
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Connection:keep-alive
            Host:www.honestbee.sg
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'honestbee'
    DepantmentOnly = True
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureHonestbee, self).__init__(user_agent, proxy_ip)
        self.header_home = self._getDict4str(self.HEADER_HOME.format(self.user_agent))
        self.header_departments = self._getDict4str(self.HEADER_DEPARTMENTS.format(self.user_agent))
        self.resultDatas = []

    def __del__(self):
        super(CaptureHonestbee, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory_depantment(self, department):
        format_url = 'https://www.honestbee.sg/api/api/departments/{}?sort=ranking&page={}&storeId=8541'
        page_count = 48
        max_page = 2
        depantment_name = department.get('depantment_name')
        depantment_id = department.get('depantment_id')
        depantment_count = department.get('depantment_count')
        category_name = 'honestbee.sg'
        page_get = depantment_count/page_count if depantment_count % page_count == 0 else depantment_count/page_count+1
        need_page = min(max_page, page_get)
        n = 0
        try:
            for i in range(need_page):
                page_url = format_url.format(depantment_id, i+1)
                page_results = self.getGoodInfos(page_url, depantment_name, category_name)
                n+=len(page_results)
                lock.acquire()
                self.resultDatas.extend(page_results)
                lock.release()
            logger.info('dealCategory_depantment depantment:{} len goods_infos: {}'.format(depantment_name, n))
        except Exception, e:
            logger.error('dealCategory_depantment department: {} error: {}.'.format(department, e))
            raise

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory_category(self, department):
        format_url = 'https://www.honestbee.sg/api/api/departments/{}?categoryIds%5B%5D={}&sort=ranking&page={}&storeId=8541'
        page_count = 48
        max_page = 2
        depantment_name = department.get('depantment_name')
        category_name = department.get('category_name')
        category_id = department.get('category_id')
        depantment_id = department.get('depantment_id')
        category_count = department.get('category_count')
        page_get = category_count/page_count if category_count % page_count == 0 else category_count/page_count+1
        need_page=min(max_page, page_get)
        n = 0
        try:
            for i in range(need_page):
                page_url = format_url.format(depantment_id, category_id, i+1)
                page_results = self.getGoodInfos(page_url, depantment_name, category_name)
                n+=len(page_results)
                lock.acquire()
                self.resultDatas.extend(page_results)
                lock.release()
            logger.info('dealCategory_category category:{} len goods_infos: {}'.format(category_name, n))
        except Exception, e:
            logger.error('dealCategory_category department: {} error: {}.'.format(department, e))
            raise
    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getGoodInfos(self, page_url, depantment_name, category_name):
        try:
            logger.debug('pageurl: {}'.format(page_url))
            result_datas = []
            page_source = self.getHtml(page_url, self.header_departments)
            goods_infos = json.loads(page_source)['products']
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = depantment_name
                result_data['SITE'.lower()] = category_name
                result_data['STATUS'.lower()] = '01'
                result_data['Currency'.lower()] = goods_info.get('currency', 'SGD')
                result_data['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                result_data['DESCRIPTION'.lower()] = goods_info.get('description')
                result_data['RESERVE'.lower()] = goods_info.get('status')
                try:
                    result_data['PRODUCT_ID'.lower()] = goods_info['id']
                    result_data['LINK'.lower()] = urljoin(self.home_url, str(goods_info['id']))
                    result_data['MAIN_IMAGE'.lower()] = goods_info['imageUrl']
                    result_data['NAME'.lower()] = goods_info['title']
                    result_data['AMOUNT'.lower()] = float(goods_info['normalPrice'])
                    result_datas.append(result_data)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    logger.error('goods_info: {}'.format(goods_info))
                    continue
            if len(goods_infos) != len(result_datas) or not result_datas:
                logger.error('len goods_infos: {},len result_datas: {}'.format(goods_infos, result_datas))
                logger.error('result_datas: {}'.format(result_datas))
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos error:{},retry it'.format(e))
            raise

    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40

    '''
    function: 获取类别信息
    @return: [] 
    '''
    def __get_department(self):
        try:
            results = []
            page_source = self.getHtml(self.home_url, self.header_home)
            pattern = re.compile(r'window.__data=[\s\S]*?;</script>', re.S)
            page_infos = pattern.findall(page_source)[0][14:-10]

            #get depantments
            pattern_d = re.compile(r'"departments":\{"byId"[\s\S]*?\},"festiveCampaign"', re.S)
            depantments_infos = '{'+pattern_d.findall(page_infos)[0][:-18]+'}'
            depantments_infos = depantments_infos.replace('false', 'False').replace('true', 'True').replace('null', 'None')
            depantments_infos = ast.literal_eval(depantments_infos)
            depantments_infos.get('departments').get('byId')

            #get categories
            pattern_c = re.compile(r'"categories":\{"byId"[\s\S]*\}\},"checkout"', re.S)
            categories_infos = '{'+pattern_c.findall(page_infos)[0][:-11]+'}'
            categories_infos = categories_infos.replace('false', 'False').replace('true', 'True').replace('null', 'None')
            categories_infos = ast.literal_eval(categories_infos)

            depantments_infos = depantments_infos.get('departments').get('byId')
            categories_infos = categories_infos.get('categories').get('byId')
            for depantments_info in depantments_infos.itervalues():
                infos = {}
                infos['depantment_id'] = depantments_info['id']
                infos['depantment_name'] = depantments_info['name']
                infos['depantment_count'] = depantments_info['productsCount']
                if self.DepantmentOnly:
                    results.append(infos)
                    continue
                categoryIds = depantments_info['categoryIds']
                for categoryId in categoryIds:
                    categories_info = categories_infos.get(str(categoryId))
                    copy_info = copy.copy(infos)
                    copy_info['category_id'] = categories_info['id']
                    copy_info['category_name'] = categories_info['title']
                    copy_info['category_count'] = categories_info['productsCount']
                    results.append(copy_info)
            return results
        except Exception, e:
            logger.error('__get_department error: {}'.format(e))
            raise

    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    def dealCategorys(self):
        try:
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            if self.DepantmentOnly:
                funlist = [gevent.spawn(self.dealCategory_depantment, department) for department in departments]
            else:
                funlist = [gevent.spawn(self.dealCategory_category, department) for department in departments]
            gevent.joinall(funlist)
            logger.info('all categorys get data: {}'.format(len(self.resultDatas)))
            resultDatas = self._rm_duplicate(self.resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data remove duplicates: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')
            format_select = 'SELECT ID,STATUS FROM {} WHERE CHANNEL="{{channel}}" AND PRODUCT_ID="{{product_id}}" ORDER BY CREATE_TIME DESC'
            good_datas = resultDatas
            select_sql = format_select.format(self.TABLE_NAME_PRODUCT)
            table = self.TABLE_NAME_PRODUCT
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','Currency','AMOUNT','DESCRIPTION','CREATE_TIME','STATUS','RESERVE']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')



def main():
    startTime = datetime.now()
    objCaptureHonestbee = CaptureHonestbee(useragent)
    # 获取所有类别id
    # objCaptureHonestbee.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureHonestbee.dealCategorys()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

