#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureEzbug.py
# @Software: PyCharm
# @Desc    :
import os
from CaptureBase import CaptureBase
import re
import time
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureEzbug(CaptureBase):
    home_url = 'https://ezbuy.sg/'
    white_department = [u'Women\'s Clothing', u'Men\'s Clothing', u'Toys, Mother & Kids', u'Home & Garden', u'Shoes, Bags & Accessories', \
                         u'Beauty & Health', u'Sports & Outdoors', u'Office & Stationery', u'Automotives', u'Mobiles & Tablets']
    # white_department = [u'Sports & Outdoors']
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:ezbuy.sg
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'ezbuy'

    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureEzbug, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureEzbug, self).__del__()

    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            page_num = 56#每页显示多少商品
            get_page = 2
            formaturl='https://ezbuy.sg{}&offset={}'
            for i in range(get_page):
                offset = page_num*i
                page_url = formaturl.format(department[1], offset)
                try:
                    page_results = self.getGoodInfos(category, page_url)
                    goods_infos.extend(page_results)
                except Exception, e:
                    logger.error('end do getGoodInfos error: {}'.format(e))
                    logger.error('end getGoodInfos category: {} page_url: {} error'.format(category, page_url))
                    continue
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(goods_infos)))
            return goods_infos
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
    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def getGoodInfos(self, category, pageurl):
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.getHtmlselenium(pageurl)
            soup = BeautifulSoup(page_source, 'lxml')
            goods_infos = soup.select('div .product-item')
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'category'
                resultData['STATUS'.lower()] = '01'
                try:
                    good_link = goods_info.find('div', {'class': 'info'}).find('a', {'class':"name"}).attrs['href']
                    resultData['LINK'.lower()] = urljoin(self.home_url, good_link)

                    pattern = re.compile(r'^/product/\d+.html', re.M)
                    good_id = pattern.match(good_link).group()
                    pattern = re.compile(r'\d+', re.M)
                    good_id = pattern.search(good_id).group()
                    resultData['PRODUCT_ID'.lower()] = good_id

                    good_img_big = goods_info.find('div', {'class': 'img'}).find('img').attrs['src']
                    if good_img_big.startswith('//'):
                        good_img_big = urljoin('https:', good_img_big)

                    resultData['MAIN_IMAGE'.lower()] = good_img_big
                    good_title = goods_info.find('div', {'class': 'info'}).find('a', {'class':"name"}).getText().strip('\n').strip().strip('\n')
                    good_title = CaptureEzbug.filter_emoji(good_title)
                    resultData['NAME'.lower()] = good_title
                    try:
                        PriceInfo = goods_info.find('div', {'class': 'info'}).find('span', {'class': "price"}).find('span').getText().strip()
                        PriceInfo = PriceInfo.split(' ')
                        good_maxDealPrice = float(PriceInfo[1])
                        resultData['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception, e:
                        # logger.error('good_maxDealPrice error: {}'.format(e))
                        resultData['AMOUNT'.lower()] = 0
                    resultData['Currency'.lower()] = 'SGD'
                    try:
                        BeforepriceInfo = goods_info.find('div', {'class': 'info'}).find('span', {'class': "price"}).find('s').getText().strip()
                        BeforepriceInfo = BeforepriceInfo.split(' ')
                        good_maxBeforeDealPrice = float(BeforepriceInfo[1])
                        resultData['Before_AMOUNT'.lower()] = good_maxBeforeDealPrice
                    except Exception, e:
                        # logger.error('good_maxBeforeDealPrice error: {}'.format(e))
                        resultData['Before_AMOUNT'.lower()] = 0

                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    # logger.error('goods_info: {}'.format(goods_info))
                    continue
            if len(goods_infos) != len(result_datas) or not result_datas:
                # logger.error('len goods_infos: {},len result_datas: {}'.format(goods_infos, result_datas))
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            # logger.error('getGoodInfos error:{},retry it'.format(e))
            # logger.error('category: {},pageurl：{}'.format(category, pageurl))
            # logger.error('page_source: {}'.format(page_source))
            raise

    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40

    '''
    function: 获取类别信息
    #[[,],[,]]
    @return: dict_res 
    '''
    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def __get_department(self):
        try:
            results = []
            html = self.getHtmlselenium(self.home_url)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.find('ul', {'class': 'navs', 'id': 'webNavs'}).findAll('li')
            for catalog in catalogs:
                url = catalog.find('a').attrs['href']
                kind = catalog.find('a').find('span', {'class': 'nav-text'}).getText().strip()
                if kind not in self.white_department:
                    logger.error('kind {} not in white_department'.format(kind.encode('utf-8')))
                    continue
                result = [kind.encode('utf-8'), url]
                results.append(result)
            return results
        except Exception, e:
            logger.error('__get_department error: {}, retry it'.format(e))
            raise

    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DETAIL_IMAGE','DESCRIPTION','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
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
            #format 格式化里面有大括号
            # format_str = r'{{"activityId":0,"areaName":"firstScreenArea_A",{}}}'
            # page_source = self.getHtml(self.home_url, self.header)
            # pattern = re.compile(r'\{"activityId":0,"areaName":"firstScreenArea_A",(.*?)\}', re.M)
            # pre_load_data = pattern.findall(page_source)
            # pre_load_data = [eval(format_str.format(data).replace(':true', ':True')) for data in pre_load_data]
            page_source = self.getHtmlselenium(self.home_url)
            soup = BeautifulSoup(page_source, 'lxml')
            # pre_load_data = soup.find('div', {'class': 'pcBetterSwipe'}).findAll('a', {'target': '_blank'})
            pre_load_data = soup.find('div', {'class': 'bannerWithHoverButtonWrapper'}).findAll('a', {'target': '_blank'})
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = urljoin(self.home_url, load_data.attrs['href'])
                    resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, load_data.find('img').attrs['src'])
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    #应该是遇到了一张图片对应多个url商品链接的情况
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
            replace_insert_columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS']
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

    objCaptureEzbug = CaptureEzbug(useragent)
    # 获取所有类别id
    # objCaptureEzbug.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureEzbug.dealCategorys()
    # objCaptureEzbug.dealCategory(['Women\'s Clothing', '/category/5?ezspm=1.20000006.3.0.5'])
    # print objCaptureEzbug.getGoodInfos('Women\'s Clothing', 'https://ezbuy.sg/category/5?ezspm=1.10000000.3.0.5&offset=56')
    # # 查询并入库首页推荐商品信息
    objCaptureEzbug.dealHomeGoods()
    # html = objCaptureEzbug.getHtmlselenium('https://ezbuy.sg/product/18978258.html?categoryid=10&ezspm=1.20000006.22.0.0')
    # print objCaptureEzbug.getHtmlselenium('https://ezbuy.sg/category/5?ezspm=1.10000000.3.0.5&offset=0')
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

