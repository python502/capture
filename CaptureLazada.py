#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureLazada.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
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

class CaptureLazada(CaptureBase):
    department_url = 'https://www.lazada.sg/catalog/?q=sale'
    home_url = 'https://www.lazada.sg'
    white_department = [ u'Mobiles & Tablets', u'Health & Beauty', u'Toys & Games', u'Furniture & D\xe9cor', u'Sports & Outdoors',\
                         u'Watches Sunglasses Jewellery', u'Fashion', u'Tools, DIY & Outdoor', u'Kitchen & Dining', u'Motors',\
                         u'TV, Audio / Video, Gaming & Wearables', u'Computers & Laptops', u'Bags and Travel', u'Stationery & Craft',\
                         u'Cameras',u'Mother & Baby', u'Pet Supplies', u'Home Appliances', u'Bedding & Bath', u'Media, Music & Books',
                         u'Laundry & Cleaning', u'Groceries']
    # white_department = [ u'Toys & Games']
    HEADER = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Connection:keep-alive
            Host:www.lazada.sg
            User-Agent:{}
            '''
    Channel = 'lazada'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureLazada, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureLazada, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            page_num = 60#设置每页显示多少商品
            get_page = 2
            formaturl='https://www.lazada.sg{}&itemperpage={}&page={}'
            if department[2] % 60 == 0:
                total_page = department[2] / 60
            else:
                total_page = department[2]/60+1
            end_page = min(total_page, get_page)+1
            for i in range(1, end_page):
                page_url = formaturl.format(department[1], page_num, i)
                page_results = self.getGoodInfos(category, page_url)
                goods_infos.extend(page_results)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(goods_infos)))
            return goods_infos
        except Exception, e:
            logger.error('categoryurl: {} error'.format(category))
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
            page_source = self.getHtml(pageurl, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            goods_infos = soup.select('div .c-product-card.c-product-list__item.c-product-card_view_grid')
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['SITE'.lower()] = 'lazada.sg'
                result_data['STATUS'.lower()] = '01'
                try:
                    good_id = goods_info.attrs['data-sku-simple']
                    result_data['PRODUCT_ID'.lower()] = good_id
                    good_link = goods_info.find('div', {'class':"c-product-card__img-placeholder"}).find('a', {'class':"c-product-card__img-placeholder-inner"}).attrs['href']
                    result_data['LINK'.lower()] = urljoin(self.home_url, good_link)

                    good_img_big = goods_info.find('div', {'class':"c-product-card__img-placeholder"}).find('a', {'class':"c-product-card__img-placeholder-inner"}).\
                        find('span').attrs['data-js-component-params']
                    pattern = re.compile(r'"src": ".*"', re.S)
                    good_img_big = pattern.findall(good_img_big)[0].strip()
                    result_data['MAIN_IMAGE'.lower()] = good_img_big[8:-1]
                    good_title = goods_info.find('div', {'class':"c-product-card__description"}).find('a', {'class':"c-product-card__name"}).getText().strip('\n').strip().strip('\n')
                    result_data['NAME'.lower()] = good_title.strip('\\')

                    try:
                        good_dealcnt = goods_info.find('div', {'class':"c-product-card__description"}).find('div',{'class':'c-product-card__review-num'}).getText().strip()
                        pattern = re.compile(r'\d+', re.M)
                        good_dealcnt = int(pattern.findall(good_dealcnt)[0])
                        result_data['DISPLAY_COUNT'.lower()] = good_dealcnt
                    except Exception, e:
                        # logger.error('good_dealcnt error: {}'.format(e))
                        result_data['DISPLAY_COUNT'.lower()] = 0

                    try:
                        PriceInfo = goods_info.find('div', {'class':"c-product-card__price-block"}).find('span', {'class':"c-product-card__price-final"}).getText().strip('\n').strip().strip('\n')
                        PriceInfo = PriceInfo.split(' ')
                        currency = PriceInfo[0]
                        good_maxDealPrice = float(PriceInfo[1].replace(',',''))
                        result_data['Currency'.lower()] = currency
                        result_data['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception, e:
                        # logger.error('good_maxDealPrice error: {}'.format(e))
                        result_data['Currency'.lower()] = 'SGD'
                        result_data['AMOUNT'.lower()] = 0

                    try:
                        BeforepriceInfo = goods_info.find('div', {'class':"c-product-card__price-block"}).find('div', {'class':"c-product-card__old-price"}).getText().strip()
                        BeforepriceInfo = BeforepriceInfo.split(' ')
                        good_maxBeforeDealPrice = float(BeforepriceInfo[1].replace(',',''))
                        result_data['Before_AMOUNT'.lower()] = good_maxBeforeDealPrice
                    except Exception, e:
                        # logger.error('good_maxBeforeDealPrice error: {}'.format(e))
                        result_data['Before_AMOUNT'.lower()] = 0

                    result_data['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    try:
                        good_description = goods_info.find('div', {'class':"c-product-card__price-block"}).find('div', {'class':"c-product-card-location__title"}).getText().strip('\n').strip().strip('\n')
                        result_data['DESCRIPTION'.lower()] = good_description.strip('\\')
                    except Exception, e:
                        # logger.error('good_description error: {}'.format(e))
                        result_data['DESCRIPTION'.lower()] = u'Singapore'

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
    #[{'category': 'Arts, Crafts &amp; Sewing', 'nodeId': '2617941011'}, {'category': 'Automotive & Motorcycle', 'nodeId': '15684181'}]
    @return: dict_res 
    '''
    def __get_department(self):
        try:
            results = []
            html = self.getHtml(self.department_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.find('ul',{'class':"c-catalog-nav__list"}).find_all('li')
            for catalog in catalogs:
                url = catalog.find('a').attrs['href']
                kind = catalog.find('a').getText().strip('\n').strip()
                if kind not in self.white_department:
                    logger.error('kind {} not in white_department'.format(kind.encode('utf-8')))
                    continue
                total = int(catalog.find('span').getText().strip('\n').strip().encode('utf-8')[1:-1])
                result = [kind.encode('utf-8'), url, total]
                results.append(result)
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DESCRIPTION','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','DISPLAY_COUNT','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureLazada = CaptureLazada(useragent)
    # 获取所有类别id
    objCaptureLazada.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureLazada.dealCategorys()
    # objCaptureLazada.dealCategory(['Groceries', '/groceries/?q=sale', 19961])
    # print objCaptureLazada.getGoodInfos('fdfd','https://www.lazada.sg/shop-fashion/?q=sale&itemperpage=60&page=0')
    # print objCaptureLazada.getHtml('https://www.lazada.sg/',objCaptureLazada.header)

    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

