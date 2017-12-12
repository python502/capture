#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureShopee.py
# @Software: PyCharm
# @Desc    :
import os
from CaptureBase import CaptureBase
import re
import time
import json
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver

class CaptureShopee(CaptureBase):
    department_url = 'https://shopee.sg/api/v1/category_list/'
    home_url = 'https://shopee.sg/'
    white_department = ['Men\'s Wear', 'Women\'s Apparel', 'Mobile & Gadgets', 'Health & Beauty', 'Food & Beverages', \
                         'Toys, Kids & Babies', 'Home Appliances', 'Home & Living', 'Men\'s Shoes', 'Women\'s Shoes' \
                          'Watches','Accessories', 'Computers & Peripherals', 'Bags', 'Games & Hobbies', 'Design & Crafts' \
                          'Sports & Outdoors', 'Pet Accessories', 'Miscellaneous', 'Tickets & Vouchers']
    # white_department = ['Men\'s Wear']
    HEADER = '''
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            cache-control:max-age=0
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'shopee'
    add_sub = False

    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureShopee, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureShopee, self).__del__()

    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            get_page = 2
            formaturl='{}?page={}'
            for i in range(get_page):
                page_url = formaturl.format(department[1], i)
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
    @retry(stop_max_attempt_number=3, wait_fixed=3000)
    def getGoodInfos(self, category, pageurl):
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.getHtmlselenium(pageurl)
            soup = BeautifulSoup(page_source, 'lxml')
            goods_headers = soup.findAll('script', {'type': 'application/ld+json'})
            resultHerder = {}
            for goods_header in goods_headers:
                good = json.loads(goods_header.contents[0].encode('utf-8'))
                resultHerder[good.get('url')] = good.get('image')
            goods_infos = soup.select('div .shopee-search-result-view__item-card')
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'category'
                resultData['STATUS'.lower()] = '01'
                try:
                    good_link = urljoin(self.home_url, goods_info.find('a', {'class': 'shopee-item-card--link'}).attrs['href'])
                    resultData['LINK'.lower()] = good_link

                    good_img = resultHerder.get(good_link)
                    if not good_img:
                        logger.error('good_link: {} not get image error'.format(good_link))
                        continue
                    resultData['MAIN_IMAGE'.lower()] = good_img

                    pattern = re.compile(r'-i\.\d+\.\d+', re.M)
                    good_id = pattern.findall(good_link)[0]
                    resultData['PRODUCT_ID'.lower()] = good_id.split('.')[-1]

                    good_title = goods_info.find('div', {'class': 'shopee-item-card__text-name'}).getText().strip('\n').strip(' ').strip('\n')
                    good_title = CaptureShopee.filter_emoji(good_title)
                    resultData['NAME'.lower()] = good_title

                    try:
                        BeforePriceInfo = goods_info.find('div', {'class': 'shopee-item-card__original-price'}).getText().strip('$')
                        good_maxBeforeDealPrice = float(BeforePriceInfo)
                        resultData['Before_AMOUNT'.lower()] = good_maxBeforeDealPrice
                    except Exception, e:
                        # logger.error('good_maxDealPrice error: {}'.format(e))
                        resultData['Before_AMOUNT'.lower()] = 0

                    try:
                        PriceInfo = goods_info.find('div', {'class': 'shopee-item-card__current-price'}).getText().strip('$')
                        good_maxDealPrice = float(PriceInfo)
                        resultData['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception, e:
                        # logger.error('good_maxDealPrice error: {}'.format(e))
                        resultData['AMOUNT'.lower()] = 0

                    resultData['Currency'.lower()] = 'SGD'
                    try:
                        good_dealcnt = goods_info.find('div', {'class':"shopee-item-card__btn-like__text"}).getText().strip(' ')
                        resultData['DISPLAY_COUNT'.lower()] = int(good_dealcnt)
                    except Exception, e:
                        # logger.error('good_dealcnt error: {}'.format(e))
                        resultData['DISPLAY_COUNT'.lower()] = 0

                    try:
                        goods_info.find('div', {'class':"shopee-horizontal-badge shopee-preferred-seller-badge"}).getText().strip(' ')
                        resultData['COMMEND_FLAG'.lower()] = 1
                    except Exception, e:
                        # logger.error('good_dealcnt error: {}'.format(e))
                        resultData['COMMEND_FLAG'.lower()] = 0
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
            logger.error('category: {},pageurl：{}'.format(category, pageurl))
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
        format_main = 'https://shopee.sg/{}-cat.{}'
        format_sub = 'https://shopee.sg/{}-cat.{}.{}'
        try:
            results = []
            page_source = self.getHtml(self.department_url, self.header)
            page_infos = json.loads(page_source)
            for page_info in page_infos:
                #main
                category = page_info[u'main'][u'display_name'].encode('utf-8')
                cat_id = page_info[u'main'][u'catid']
                if category not in self.white_department:
                    continue
                if category.find(' & ') != -1:
                    tmp = category.replace(' & ', '-')
                else:
                    tmp = category.replace(' ', '-')
                url = format_main.format(tmp, cat_id)
                results.append([category, url])
                #sub
                if self.add_sub:
                    sub_categorys = page_info[u'sub']
                    for sub_category in sub_categorys:
                        category = sub_category[u'display_name'].encode('utf-8')
                        sub_cat_id = sub_category[u'catid']
                        if category.find(' & ') != -1:
                            tmp = category.replace(' & ', '-')
                        else:
                            tmp = category.replace(' ', '-')
                        url = format_sub.format(tmp, cat_id, sub_cat_id)
                        results.append([category, url])
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','COMMEND_FLAG', 'Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
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
            page_source = self.getHtmlselenium(self.home_url, 'chrome', 120)
            soup = BeautifulSoup(page_source, 'lxml')
            pre_load_data = soup.find('div', {'class': 'image-carousel__item-list-wrapper'}).findAll('li', {'class': 'image-carousel__item'})
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = urljoin(self.home_url, load_data.find('a', {'class': 'home-banners__banner-image'}).attrs['href'])
                    image_info = load_data.find('div', {'class': 'lazy-image__image'}).attrs['style']
                    pattern = re.compile('\(.*?\)')
                    pre_load_data = pattern.findall(image_info)[0]
                    resultData['MAIN_IMAGE'.lower()] = pre_load_data[2:-2]
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
            replace_insert_columns = ['CHANNEL', 'LINK', 'TITLE', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS']
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

    objCaptureShopee = CaptureShopee(useragent)
    # 获取所有类别id
    # objCaptureShopee.get_department()
    # 查询并入库所有类别的商品信息
    # objCaptureShopee.dealCategorys()
    # # 查询并入库首页推荐商品信息
    objCaptureShopee.dealHomeGoods()
    # print objCaptureShopee.getHtml('https://shopee.sg/Men\'s-Shoes-cat.168',objCaptureShopee.header)
    # print objCaptureShopee.getHtmlselenium('https://shopee.sg')
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

