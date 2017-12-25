#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureHipvan.py
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
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureHipvan(CaptureBase):
    home_url = 'https://www.hipvan.com/'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:www.hipvan.com
            User-Agent:{}
            '''
    Channel = 'hipvan'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureHipvan, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureHipvan, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, department):
        category = department[0]
        page_url = department[1]
        site = department[2]
        try:
            page_results = self.getGoodInfos(category, page_url, site)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(page_results)))
            return page_results
        except Exception, e:
            logger.error('dealCategory department: {} error: {}.'.format(department, e))
            return []

    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getGoodInfos(self, category, pageurl, site):
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.getHtml(pageurl, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            goods_infos = soup.find('ul', {'class':'pdt-grid__pdt-list pdt-grid__list product-grid-list'}).findAll('li')
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['SITE'.lower()] = site
                result_data['STATUS'.lower()] = '01'
                result_data['Currency'.lower()] = 'USD'
                try:
                    result_data['PRODUCT_ID'.lower()] = goods_info.attrs['data-product-variant-id']
                    data_box = goods_info.find('div', {'class':"pdt-grid__item-box pdt-grid__fade clearfix"})
                    good_link = data_box.find('a', {'class':"pdt-grid__item-link product-grid-item__link"}).attrs['href']
                    result_data['LINK'.lower()] = urljoin(self.home_url, good_link)
                    result_data['MAIN_IMAGE'.lower()] = data_box.find('div', {'class':"pdt-grid__layer-img"}).findAll('img')[0].attrs['src']
                    result_data['NAME'.lower()] = data_box.find('a', {'class':"pdt-grid__item-link product-grid-item__link"}).attrs['title']
                    try:
                        PriceInfo = data_box.find('div', {'class':"pdt-grid__current-price"}).getText().strip()
                        PriceInfo = PriceInfo[1:] if PriceInfo.startswith('$') else PriceInfo
                        good_maxDealPrice = float(PriceInfo)
                        result_data['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception:
                        result_data['AMOUNT'.lower()] = 0
                    try:
                        BeforepriceInfo = goods_info.find('div', {'class':"pdt-grid__typical-price"}).find('span').getText().strip(' ')
                        BeforepriceInfo = BeforepriceInfo[1:] if BeforepriceInfo.startswith('$') else BeforepriceInfo
                        good_maxBeforeDealPrice = float(BeforepriceInfo)
                        result_data['Before_AMOUNT'.lower()] = good_maxBeforeDealPrice
                    except Exception:
                        result_data['Before_AMOUNT'.lower()] = 0

                    result_data['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
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

    def __get_department_f(self, catelogs, categary):
        try:
            catelog_1_s = catelogs.findAll('li',{'class': 'c-nav__menu__block-item js-nav__menu__block-item'})
            results=[]
            for catelog_1 in catelog_1_s:
                kind_1 = catelog_1.find('a', {'class':'c-nav__menu__catm-link ic-bef ic-site-plus js-nav__menu__catm-link'}).getText().strip()
                site = categary+','+kind_1
                catelog_2_s = catelog_1.findAll('li', {'class': 'c-nav__menu__cats-item js-nav__menu__cats-item'})
                for catelog_2 in catelog_2_s:
                    kind_2 = catelog_2.find('a').getText().strip()
                    href = urljoin(self.home_url, catelog_2.find('a').attrs['href'])
                    results.append([kind_2, href, site])
            return results
        except Exception, e:
            logger.error('__get_department_f categary {} error: {}'.format(categary, e))
            raise

    def __get_department_n(self, catelogs, categary):
        try:
            catelog_1_s = catelogs.findAll('li',{'class': 'c-nav__menu__block-item js-nav__menu__block-item'})
            results=[]
            for catelog_1 in catelog_1_s:
                kind_1 = catelog_1.find('a', {'class':'c-nav__menu__catm-link ic-bef'}).getText().strip()
                site = categary
                href = urljoin(self.home_url, catelog_1.find('a').attrs['href'])
                results.append([kind_1, href, site])
            return results
        except Exception, e:
            logger.error('__get_department_n categary {} error: {}'.format(categary, e))
            raise

    def __get_department_s(self, catelogs, categary):
        try:
            catelog_1_s = catelogs.findAll('li',{'class': 'c-nav__menu__cats-item js-nav__menu__cats-item'})
            results=[]
            for catelog_1 in catelog_1_s:
                kind_1 = catelog_1.find('a', {'class':'c-nav__menu__cats-link js-nav__menu__cats-link'}).getText().strip()
                site = categary
                href = urljoin(self.home_url, catelog_1.find('a').attrs['href'])
                results.append([kind_1, href, site])
            return results
        except Exception, e:
            logger.error('__get_department_d categary {} error: {}'.format(categary, e))
            raise
    '''
    function: 获取类别信息
    #[{'category': 'Arts, Crafts &amp; Sewing', 'nodeId': '2617941011'}, {'category': 'Automotive & Motorcycle', 'nodeId': '15684181'}]
    @return: dict_res 
    '''
    def __get_department(self):
        try:
            results = []
            html = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.findAll('li',{'class':"c-nav__menu__drop c-nav__menu__item has-arrow js-nav__menu__item"})
            for catalog_1 in catalogs:
                # import pdb
                # pdb.set_trace()
                kind_1 = catalog_1.find('a', {'class':'c-nav__menu__catr-btn u-animate-all ic-bef ic-site-plus js-nav__menu__catr-btn'}).getText().strip()
                if kind_1 in ['Furniture', 'Homeware']:
                    result = self.__get_department_f(catalog_1, kind_1)
                elif kind_1 in ['New', 'Lighting']:
                    result = self.__get_department_n(catalog_1, kind_1)
                elif kind_1 in ['Sale']:
                    result = self.__get_department_s(catalog_1, kind_1)
                else:
                    logger.error('kind_1: {} error'.format(kind_1))
                results.extend(result)
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
            resultDatas = []
            for department in departments:
                resultData = self.dealCategory(department)
                resultDatas.extend(resultData)
            logger.info('all categorys get data: {}'.format(len(resultDatas)))
            resultDatas = self._rm_duplicate(resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data remove duplicates: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')
            format_select = 'SELECT ID,STATUS FROM {} WHERE CHANNEL="{{channel}}" AND PRODUCT_ID="{{product_id}}" ORDER BY CREATE_TIME DESC'
            good_datas = resultDatas
            select_sql = format_select.format(self.TABLE_NAME_PRODUCT)
            table = self.TABLE_NAME_PRODUCT
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')


    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        format_str = '{{{}}}'
        try:
            page_source = self.getHtml(self.home_url, self.header)
            pattern = re.compile('ORIGINAL_CAROUSEL_DATA = \[(.+?)\];', re.S)
            all_infos = pattern.findall(page_source)[0].replace('\n', '')
            pattern = re.compile('\{(.+?)\}', re.M)
            pre_load_data = pattern.findall(all_infos)
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    load_data = format_str.format(load_data)
                    data = json.loads(load_data)
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = data['href']
                    resultData['MAIN_IMAGE'.lower()] = data['image_url']
                    resultData['TITLE'.lower()] = data['title']
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('get eLement error:{}'.format(e))
                    logger.error('goodData: {}'.format(load_data))
                    continue
            result_datas = self._rm_duplicate(result_datas, 'LINK'.lower())
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(page_source))
                raise ValueError('not get valid data')

            format_select = r'SELECT ID FROM {} WHERE CHANNEL="{{channel}}" and LINK="{{link}}" ORDER BY CREATE_TIME DESC'
            good_datas = result_datas
            select_sql = format_select.format(self.TABLE_NAME_BANNER)
            table = self.TABLE_NAME_BANNER
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'TITLE', 'CREATE_TIME', 'STATUS']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureHipvan = CaptureHipvan(useragent)
    # 获取所有类别id
    # objCaptureHipvan.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureHipvan.dealCategorys()
    objCaptureHipvan.dealHomeGoods()
    # objCaptureHipvan.dealCategory(['Groceries', '/groceries/?q=sale', 19961])
    # print len(objCaptureHipvan.getGoodInfos('fdfd','https://www.hipvan.com/furniture/kids-tables-chairs?ref=nav_sub','fdfd'))
    # html = objCaptureHipvan.getHtml('https://www.hipvan.com/', objCaptureHipvan.header)
    # print html


    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

