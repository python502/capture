#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureAngelflorist.py
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

class CaptureAngelflorist(CaptureBase):
    home_url = 'https://www.angelflorist.com/'
    white_department = ['Hand Bouquet', 'Table Arrangement', 'Wine Hampers', 'New Born Baby', 'Grand Opening Stand', \
                         'fruits baskets', 'Wellness Hampers', 'Condolence Stand', 'Perfume & Flowers', 'Everlasting Bloom',\
                        'Orchids Plants', 'Wedding Flowers', 'Anniversary', 'Birthday']

    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Connection:keep-alive
            Host:www.angelflorist.com
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'angelflorist'

    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureAngelflorist, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureAngelflorist, self).__del__()


    def getFirstUrlInfos(self, url):
        try:
            results = []
            html = self.getHtml(url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            paginations = soup.findAll('font', {'class': 'pagination'})[1].findAll('a')[:-1]
            for pagination in paginations:
                url = pagination.attrs['href']
                url = urljoin(self.home_url, url)
                results.append(url)
            return results
        except Exception:
            return []

    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        first_url = department[1]
        url_pools = [first_url]
        try:
            # page_num = 16#每页显示多少商品
            get_page = 5
            url_results = self.getFirstUrlInfos(first_url)
            if url_results:
                url_pools.extend(url_results)
            len_page = len(url_pools)
            need_page = min(len_page, get_page)
            for i in range(need_page):
                page_url = url_pools[i]
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
            html = self.getHtml(pageurl, self.header)
            soup = BeautifulSoup(html, 'lxml')
            goods_infos = soup.select('div .thumbnail.item')
            result_datas = []
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'category'
                resultData['STATUS'.lower()] = '01'
                try:
                    good_link = goods_info.find('a').attrs['href']
                    resultData['LINK'.lower()] = urljoin(self.home_url, good_link)
                    resultData['PRODUCT_ID'.lower()] = good_link.split('/')[-2]
                    img_link = goods_info.find('img').attrs['src']
                    resultData['MAIN_IMAGE'.lower()] = urljoin(self.home_url, img_link)
                    resultData['NAME'.lower()] = goods_info.find('img').attrs['alt'].strip('\n').strip().strip('\n')
                    PriceInfo = goods_info.find('p', {'class': 'item-price'}).getText().strip()
                    resultData['Currency'.lower()] = 'SGD' if PriceInfo.startswith('S$') else 'USD'
                    pattern = re.compile(r'\d+\.\d*', re.M)
                    resultData['AMOUNT'.lower()] = float(pattern.findall(PriceInfo)[0])
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    logger.error('goods_info: {}'.format(goods_info))
                    continue
            if len(goods_infos) != len(result_datas) or not result_datas:
                # logger.error('len goods_infos: {},len result_datas: {}'.format(goods_infos, result_datas))
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos error:{},retry it'.format(e))
            logger.error('category: {},pageurl：{}'.format(category, pageurl))
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
            html = self.getHtml(self.home_url,self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.findAll('div', {'class': 'item-block'})
            show_catalogs = catalogs[:-1]
            list_catalogs = catalogs[-1]
            for show_catalog in show_catalogs:
                url = show_catalog.find('a').attrs['href']
                url = urljoin(self.home_url, url)
                kind = show_catalog.find('span', {'class': 'block-title-left'}).getText().strip()
                if kind not in self.white_department:
                    logger.error('kind {} not in white_department'.format(kind.encode('utf-8')))
                    continue
                result = [kind.encode('utf-8'), url]
                results.append(result)
            list_catalogs = list_catalogs.findAll('td')
            for list_catalog in list_catalogs:
                url = list_catalog.find('a').attrs['href']
                url = urljoin(self.home_url, url)
                kind = list_catalog.find('a').getText().strip('\n').strip()
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
            pre_load_data = soup.find('ul', {'class': 'bxslider'}).findAll('li')
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = urljoin(self.home_url, load_data.find('a').attrs['href'])
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

    objCaptureAngelflorist = CaptureAngelflorist(useragent)
    # 获取所有类别id
    # objCaptureAngelflorist.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureAngelflorist.dealCategorys()

    objCaptureAngelflorist.dealHomeGoods()

    # print objCaptureAngelflorist.getHtml(objCaptureAngelflorist.home_url,objCaptureAngelflorist.header)
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

