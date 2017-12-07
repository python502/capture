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
import random
import os
from CaptureBase import CaptureBase
from pytesser import *
import re
import json
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import configuration
from selenium import webdriver
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

TABLE_NAME_GOODS = 'market_product_raw'
TABLE_NAME_HOME = 'market_banner_raw'
TABLE_NAME_VERIFY = 'market_verify_raw'
'''
classdocs
'''
class CaptureLazada(CaptureBase):
    department_url = 'https://www.lazada.sg/catalog/?q=sale'
    home_url = 'https://www.lazada.sg'
    phantomjs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'phantomjs.exe')
    white_department = [ u'Mobiles & Tablets', u'Health & Beauty', u'Toys & Games', u'Furniture & D\xe9cor', u'Sports & Outdoors',\
                         u'Watches Sunglasses Jewellery', u'Fashion', u'Tools, DIY & Outdoor', u'Kitchen & Dining', u'Motors',\
                         u'TV, Audio / Video, Gaming & Wearables', u'Computers & Laptops', u'Bags and Travel', u'Stationery & Craft',\
                         u'Cameras',u'Mother & Baby', u'Pet Supplies', u'Home Appliances', u'Bedding & Bath', u'Media, Music & Books',
                         u'Laundry & Cleaning', u'Groceries']
    # white_department = [ u'Mobiles & Tablets']
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
        '''
        Constructor
        '''
        super(CaptureLazada, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))
    def __del__(self):
        super(CaptureLazada, self).__del__()

    '''
    function: 查询并将商品入库
    @category: category
    @categoryurl: category url
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            page_num = 60#设置每页显示多少商品
            get_page = 2
            formaturl='https://www.lazada.sg{}&itemperpage={}&page={}'
            if department[2]%60 == 0:
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
        driver = None
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.getHtml(pageurl, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            # goods_infos = soup.find('div',{'class':'catalog__main__content'})

            goods_infos = soup.select('div .c-product-card.c-product-list__item.c-product-card_view_grid')
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'lazada.sg'
                resultData['STATUS'.lower()] = '01'
                try:
                    good_id = goods_info.attrs['data-sku-simple']
                    resultData['PRODUCT_ID'.lower()] = good_id
                    good_link = goods_info.find('div', {'class':"c-product-card__img-placeholder"}).find('a', {'class':"c-product-card__img-placeholder-inner"}).attrs['href']
                    resultData['LINK'.lower()] = urljoin(self.home_url, good_link)

                    good_img_big = goods_info.find('div', {'class':"c-product-card__img-placeholder"}).find('a', {'class':"c-product-card__img-placeholder-inner"}).\
                        find('span').attrs['data-js-component-params']
                    pattern = re.compile(r'"src": ".*"', re.S)
                    good_img_big = pattern.findall(good_img_big)[0].strip()
                    resultData['MAIN_IMAGE'.lower()] = good_img_big[8:-1]
                    good_title = goods_info.find('div', {'class':"c-product-card__description"}).find('a', {'class':"c-product-card__name"}).getText().strip('\n').strip(' ').strip('\n')
                    resultData['NAME'.lower()] = good_title

                    try:
                        good_dealcnt = goods_info.find('div', {'class':"c-product-card__description"}).find('div',{'class':'c-product-card__review-num'}).getText().strip(' ')
                        pattern = re.compile(r'\d+', re.M)
                        good_dealcnt = int(pattern.findall(good_dealcnt)[0])
                        resultData['DISPLAY_COUNT'.lower()] = good_dealcnt
                    except Exception, e:
                        # logger.error('good_dealcnt error: {}'.format(e))
                        resultData['DISPLAY_COUNT'.lower()] = 0

                    try:
                        PriceInfo = goods_info.find('div', {'class':"c-product-card__price-block"}).find('span', {'class':"c-product-card__price-final"}).getText().strip('\n').strip(' ').strip('\n')
                        PriceInfo = PriceInfo.split(' ')
                        currency = PriceInfo[0]
                        good_maxDealPrice = float(PriceInfo[1].replace(',',''))

                        resultData['Currency'.lower()] = currency
                        resultData['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception, e:
                        # logger.error('good_maxDealPrice error: {}'.format(e))
                        resultData['Currency'.lower()] = 'SGD'
                        resultData['AMOUNT'.lower()] = 0

                    try:
                        BeforepriceInfo = goods_info.find('div', {'class':"c-product-card__price-block"}).find('div', {'class':"c-product-card__old-price"}).getText().strip(' ')
                        BeforepriceInfo = BeforepriceInfo.split(' ')
                        good_maxBeforeDealPrice = float(BeforepriceInfo[1].replace(',',''))
                        resultData['Before_AMOUNT'.lower()] = good_maxBeforeDealPrice
                    except Exception, e:
                        # logger.error('good_maxBeforeDealPrice error: {}'.format(e))
                        resultData['Before_AMOUNT'.lower()] = 0

                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    try:
                        good_description = goods_info.find('div', {'class':"c-product-card__price-block"}).find('div', {'class':"c-product-card-location__title"}).getText().strip('\n').strip(' ').strip('\n')
                        resultData['DESCRIPTION'.lower()] = good_description
                    except Exception, e:
                        # logger.error('good_description error: {}'.format(e))
                        resultData['DESCRIPTION'.lower()] = u'Singapore'

                    result_datas.append(resultData)
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
    function: 格式化数据并将商品入库 存在不同detailId 对应相同商品的情况
    @source_datas: 原始数据
    @return: True or False
    '''
    '''
    function: 存储分类商品信息
    @good_datas： 商品信息s
    @return: True or False or raise
    '''
    def saveCategoryGoods(self, good_datas):
        try:
            result_insert, result_update = True, True
            table = TABLE_NAME_GOODS
            columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DETAIL_IMAGE','DESCRIPTION','Currency','AMOUNT','Before_AMOUNT','CREATE_TIME','DISPLAY_COUNT','STATUS']
            if not good_datas:
                logger.error('not get datas to save')
                return False
            # select_sql = 'SELECT ID,STATUS FROM market_product_raw WHERE CHANNEL="{channel}" and KIND="{kind}" AND PRODUCT_ID="{product_id}" ORDER BY CREATE_TIME DESC '
            select_sql = 'SELECT ID,STATUS FROM market_product_raw WHERE CHANNEL="{channel}" AND PRODUCT_ID="{product_id}" ORDER BY CREATE_TIME DESC '
            (insert_datas, update_datas) = self._checkDatas(select_sql, good_datas, ['ID', 'STATUS'])
            if insert_datas:
                operate_type = 'insert'
                l = len(insert_datas)
                logger.info('len insert_datas: {}'.format(l))
                result_insert = self.mysql.insert_batch(operate_type, table, columns, insert_datas)
                logger.info('result_insert: {}'.format(result_insert))
            if update_datas:
                operate_type = 'replace'
                l = len(update_datas)
                logger.info('len update_datas: {}'.format(l))
                columns.insert(0, 'ID')
                result_update = self.mysql.insert_batch(operate_type, table, columns, update_datas)
                logger.info('result_update: {}'.format(result_update))
            return result_insert and result_update
        except Exception, e:
            logger.error('saveCategoryGoods error: {}.'.format(e))
            return False

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
                kind = catalog.find('a').getText().strip('\n').strip(' ')
                if kind not in self.white_department:
                    logger.error('kind {} not in white_department'.format(kind.encode('utf-8')))
                    continue
                total = int(catalog.find('span').getText().strip('\n').strip(' ').encode('utf-8')[1:-1])
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
            return self.saveCategoryGoods(resultDatas)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')

    def get_verify_code(self, img_url):

        html = self.getHtml(img_url, self.header_varify)
        # img_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anazon_verify.jpg')
        img_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.basename(img_url))
        with open(img_name, 'wb') as f:
            f.write(html)
        im = Image.open(img_name)
        verify_code = image_to_string(im).strip()
        os.remove(img_name)
        return verify_code

    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        driver = None
        result_datas = []
        try:
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = self.user_agent
            driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
            #加载页面的超时时间
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)

            verify_image_url = ''
            verify_code = ''
            while 1:
                driver.get(self.home_url)
                #判断是否被反爬虫 要去输入验证码
                pattern = re.compile(r'<h4>Type the characters you see in this image:</h4>', re.M)
                need_verify = True if pattern.search(driver.page_source) else False
                if need_verify:
                    #要求输入验证码，获取验证码图片url
                    logger.info('Need check verify code !')
                    verify_image_url = driver.find_element_by_xpath('/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]/img').get_attribute('src').strip()
                    logger.info('verify image url: {}'.format(verify_image_url))
                    result = configuration.get_value('VERIFY_CODE', os.path.basename(verify_image_url))
                    if result:
                        verify_code = result
                    else:
                        verify_code = self.get_verify_code(verify_image_url)
                        if len(verify_code) != 6 or not re.match('^[A-Z]+$', verify_code):
                            # continue
                            logger.error('auto check verify code:{} error'.format(verify_code))
                            logger.info('Need to manually enter the verification code')
                            verify_code = ''
                            while not verify_code:
                                verify_code = raw_input("please input verify code:")
                                logger.info('The verification code you entered: {}'.format(verify_code))
                    input_code = driver.find_element_by_xpath('//*[@id="captchacharacters"]')
                    input_code.send_keys(verify_code)

                    submit = driver.find_element_by_xpath("/html/body/div/div[1]/div[3]/div/div/form/div[2]/div/span/span/button")
                    submit.submit()
                else:
                    if verify_image_url and verify_code:
                        configuration.set_value('VERIFY_CODE', os.path.basename(verify_image_url), verify_code)
                    break
            #设置定位元素时的超时时间
            driver.implicitly_wait(10)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            pattern = re.compile(r'<li class="a-carousel-card" role="listitem" aria-setsize="\d+" aria-posinset=', re.M)
            aria_set_size_str = pattern.findall(driver.page_source)[0]
            pattern = re.compile(r'\d+', re.M)
            aria_set_size = pattern.findall(aria_set_size_str)[0]
            for i in range(1, int(aria_set_size)+1):
                try:
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    find_raw = {'class': "a-carousel-card", 'role': "listitem",\
                                'aria-setsize': aria_set_size, 'aria-posinset': str(i)}
                    good_data = soup.findAll('li', find_raw)[0]
                    href = good_data.findAll('span')[0].findAll('a')[0].attrs['href']

                    if href.startswith('/b/') or href.startswith('b/') or href.startswith('/b?') or href.startswith('b?'):
                        pattern = re.compile('&pf_rd_p=[-a-z0-9]+')
                        href = pattern.sub('', href)
                        pattern = re.compile('&pf_rd_r=[A-Z0-9]+')
                        href = pattern.sub('', href)
                        resultData['LINK'.lower()] = urljoin(self.home_url, href)
                    else:
                        resultData['LINK'.lower()] = urljoin(self.home_url, href.split('?')[0])
                    resultData['TITLE'.lower()] = good_data.findAll('span')[0].findAll('img')[0].attrs['alt']
                    # #不要小图
                    # resultData['MIN_IMAGE'.lower()] = good_data.findAll('span')[0].findAll('img')[0].attrs['src']
                    resultData['MAIN_IMAGE'.lower()] = good_data.findAll('span')[0].findAll('img')[0].attrs['data-a-hires']
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))

                    result_datas.append(resultData)
                except Exception, e:
                    #应该是遇到了一张图片对应多个url商品链接的情况
                    logger.error('get eLement error:{}'.format(e))
                    logger.error('goodData: {}'.format(good_data))
                    continue
            if len(result_datas) == 0:
                logger.error('driver.page_source: {}'.format(driver.page_source))
                raise ValueError('not get valid data')
            select_sql = 'SELECT ID,STATUS FROM market_banner_raw WHERE CHANNEL="{channel}" and LINK="{link}" ORDER BY CREATE_TIME DESC '
            (insert_datas, update_datas) = self._checkDatas(select_sql, result_datas, ['ID', 'STATUS'])
            # #不要小图
            # column = 'CHANNEL, LINK, TITLE, MIN_IMAGE, MAIN_IMAGE, CREATE_TIME'
            columns = ['CHANNEL', 'LINK', 'TITLE', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS']
            table = TABLE_NAME_HOME
            result_insert, result_update = True, True
            if insert_datas:
                operate_type = 'insert'
                length = len(insert_datas)
                logger.info('len insert_datas: {}'.format(length))
                result_insert = self.mysql.insert_batch(operate_type, table, columns, insert_datas)
                logger.info('result_insert: {}'.format(result_insert))
            if update_datas:
                operate_type = 'replace'
                length = len(update_datas)
                logger.info('len update_datas: {}'.format(length))
                columns.insert(0, 'ID')
                result_update = self.mysql.insert_batch(operate_type, table, columns, update_datas)
                logger.info('result_update: {}'.format(result_update))
            return result_insert and result_update
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise
        finally:
            if driver:
                driver.quit()

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureLazada = CaptureLazada(useragent)
    # 获取所有类别id
    # objCaptureLazada.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureLazada.dealCategorys()
    # objCaptureLazada.dealCategory(['Groceries', '/groceries/?q=sale', 19961])
    # # 查询并入库首页推荐商品信息
    # print objCaptureLazada.getGoodInfos('fdfd','https://www.lazada.sg/shop-fashion/?q=sale&itemperpage=60&page=0')
    # objCaptureAmazon.dealHomeGoods()
    # objCaptureLazada.getHtml()
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

