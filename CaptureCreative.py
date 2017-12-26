#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureCreative.py
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

class CaptureCreative(CaptureBase):
    home_url = 'https://sg.creative.com/'
    best_sellers_url = 'http://sg.althea.kr/best-sellers'
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:sg.creative.com
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'creative'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureCreative, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureCreative, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, department):
        category = department[0]
        page_url = department[1]
        try:
            page_results = self.getGoodInfos(category, page_url)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(page_results)))
            return page_results
        except Exception, e:
            logger.error('category: {} error'.format(category))
            logger.error('dealCategory error: {}.'.format(e))
            return False

    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getGoodInfos(self, category, pageurl):
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.getHtmlselenium(pageurl)
            soup = BeautifulSoup(page_source, 'lxml')
            goods_infos = soup.findAll('div', {'class': 'category-cell'})
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['SITE'.lower()] = 'creative'
                result_data['STATUS'.lower()] = '01'
                result_data['Currency'.lower()] = 'USD'
                try:
                    result_data['PRODUCT_ID'.lower()] = goods_info.attrs['data-productid']
                    result_data['LINK'.lower()] = urljoin(self.home_url, goods_info.find('a').attrs['href'])
                    result_data['MAIN_IMAGE'.lower()] = 'https:'+goods_info.find('img').attrs['data-src-x1']
                    result_data['NAME'.lower()] = goods_info.find('h2',{'class':'product-name'}).getText().strip('\n').strip()
                    try:
                        result_data['AMOUNT'.lower()] = float(goods_info.attrs['data-price'])
                    except Exception:
                        result_data['AMOUNT'.lower()] = 0

                    try:
                        result_data['RESERVE'.lower()] = goods_info.find('SPAN',{'class':'new-tag'}).getText().strip('\n').strip()
                    except Exception:
                        result_data['RESERVE'.lower()] = 0

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
                if not resultData:
                    logger.error('deal one categorys error: {}'.format(department))
                    continue
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','Currency','AMOUNT','CREATE_TIME','STATUS']
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
        try:
            page_source = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            pre_load_data = soup.find('div', {'class': 'mhl-container slider'}).findAll('div')
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    good_link = load_data.find('a').attrs['href']
                    resultData['LINK'.lower()] = urljoin(self.home_url, good_link)
                    mhln = [re.match(r'mhl\d+', one).group(0) for one in load_data.attrs['class'] if re.match(r'mhl\d+',one)][0]
                    format_str = r'.%s \{[\s\S]+?\}' % (mhln)
                    pattern = re.compile(format_str, re.S)
                    image_info = pattern.findall(page_source)[0]
                    pattern = re.compile(r'background-image: url\([\s\S]+\);', re.S)
                    image_info = pattern.findall(image_info)[0][22:-2]
                    resultData['MAIN_IMAGE'.lower()] = 'https:'+image_info
                    resultData['TITLE'.lower()] = load_data.find('a').attrs['title']
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
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS', 'TITLE']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40

    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def __get_department(self):
        try:
            results = []
            html = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs_all = soup.findAll('div', {'class': 'col-xxs-12 col-xs-6 col-sm-4 col-lg-4'})
            catalogs = catalogs_all[:-1]
            other = catalogs_all[-1]
            for catalog in catalogs:
                url = urljoin(self.home_url, catalog.find('h2').find('a').attrs['href'])
                kind = catalog.find('h2').find('a').getText().strip()
                result = [kind.encode('utf-8'), url]
                results.append(result)
            other_urls = other.findAll('li')
            for other_url in other_urls:
                url = other_url.find('a').attrs['href']
                if url.find('?filters=') == -1:
                    url = urljoin(self.home_url, url)
                    kind = other_url.find('a').getText().strip()
                    results.append([kind.encode('utf-8'), url])
            return results
        except Exception, e:
            logger.error('__get_department error: {}, retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureCreative = CaptureCreative(useragent)
    # 查询并入库所有类别的商品信息
    # objCaptureCreative.get_department()
    objCaptureCreative.dealCategorys()
    # objCaptureCreative.dealHomeGoods()
    # html = objCaptureCreative.getHtml('https://sg.creative.com/p/accessories', objCaptureCreative.header)
    # print html

    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

