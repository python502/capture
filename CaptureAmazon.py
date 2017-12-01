#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureAmazon.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
import random
import urllib2
import os

from pytesser import *
import zlib
import re
import cookielib
import json
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from CrawlingProxy import CrawlingProxy,useragent
from MysqldbOperate import MysqldbOperate
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

# DICT_MYSQL = {'host': '118.193.21.62', 'user': 'root', 'passwd': 'Avazu#2017', 'db': 'avazu_opay', 'port': 3306}
DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
TABLE_NAME_GOODS = 'market_product_raw'
TABLE_NAME_HOME = 'market_banner_raw'
TABLE_NAME_VERIFY = 'market_verify_raw'
'''
classdocs
'''
class CaptureAmazon(object):
    # [{'category': 'Baby', 'nodeId': '165796011'}, {'category': 'Beauty', 'nodeId': '3760911'}, {'category': 'Books', 'nodeId': '283155'}, {'category': 'Electronics', 'nodeId': '172282'}, {'category': 'Furniture', 'nodeId': '1063306'}, {'category': 'Grocery', 'nodeId': '16310101'}, {'category': 'Handmade', 'nodeId': '11260432011'}, {'category': 'Home', 'nodeId': '1055398'}, {'category': 'Kindle', 'nodeId': '133140011'}, {'category': 'Kitchen', 'nodeId': '284507'}, {'category': 'Magazines', 'nodeId': '599858'}, {'category': 'Music', 'nodeId': '5174'}, {'category': 'Software', 'nodeId': '229534'}, {'category': 'Video', 'nodeId': '404272'}, {'category': 'Wine', 'nodeId': '2983386011'}]
    department_url = 'https://www.amazon.com/gp/goldbox/'
    home_url = 'https://www.amazon.com/'
    phantomjs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'phantomjs.exe')
    white_department = [ u'Arts, Crafts &amp; Sewing', u'Automotive & Motorcycle', u'Baby', u'Baby Clothing & Accessories', u'Beauty',\
                         u'Boys’ Fashion', u'Camera & Photo', u'Cell Phones & Accessories', u'Computers & Accessories', u'Costumes & Accessories',\
                         u'DIY & Tools', u'Electronics', u'Furniture', u'Girls’ Fashion', u'Grocery',\
                         u'Kitchen', u'Luggage Travel Gear', 'Men\'s Shoes', 'Men\'s Watches', u'Men’s Clothing', u'Men’s Fashion',\
                         u'Office Electronics & Supplies', u'Patio, Lawn & Garden', u'Power & Hand Tools', u'Sports & Outdoors', u'Toys &amp; Games', 'Women\'s Shoes', \
                         'Women\'s Watches', u'Women’s Clothing', u'Women’s Fashion', u'Women’s Jewelry']
    # white_department = [ u'Arts, Crafts &amp; Sewing', u'Automotive & Motorcycle']
    HEADER = '''
            Accept:application/json, text/javascript, */*; q=0.01
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9
            Connection:keep-alive
            Content-Type:application/x-www-form-urlencoded
            Host:www.amazon.com
            Origin:https://www.amazon.com
            User-Agent:{}
            X-Requested-With:XMLHttpRequest
            '''
    Channel = 'amazon'
    def __init__(self, user_agent, proxy_ip=None):
        '''
        Constructor
        '''
        # Cookie and Referer is'not necessary

        self.user_agent = user_agent
        src_header = self.HEADER.format(self.user_agent)
        self.header = self.__getDict4str(src_header)
        self.mysql = MysqldbOperate(DICT_MYSQL)
        #获得一个cookieJar实例
        self.cj = cookielib.CookieJar()
        #cookieJar作为参数，获得一个opener的实例
        opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cj))
        urllib2.install_opener(opener)

        proxy = urllib2.ProxyHandler(proxy_ip)
        opener = urllib2.build_opener(proxy)
        urllib2.install_opener(opener)
    def __del__(self):
        if self.mysql:
            del self.mysql
    '''
    function: getHtml 根据url获取页面的html
    @url:  url
    @return: html
    '''
    def getHtml(self, url, header=None):
        HEADER = header if header else self.header
        try:
            req = urllib2.Request(url=url, headers=HEADER)
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
    def __getDict4str(self, strsource):
        outdict = {}
        lists = strsource.split('\n')
        for list in lists:
            list = list.strip()
            if list:
                strbegin = list.find(':')
                outdict[list[:strbegin]] = list[strbegin+1:] if strbegin != len(list) else ''
        return outdict


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
    function: 查询并将商品入库
    @category: category
    @categoryurl: category url
    @return: True or False
    '''
    def dealCategory(self, category, categoryurl):
        try:
            numMAX = 100 #总共爬取多少记录，可支持最大值是300
            numSet = 100 #每次爬取多少记录，可支持最大值是100
            html = self.getHtml(categoryurl)
            # data = '{"requestMetadata":{"marketplaceID":"ATVPDKIKX0DER","clientID":"goldbox_mobile_pc","sessionID":"139-3159259-1881936"},"dealTargets":[{"dealID":"4577b235"},{"dealID":"52668fbe"},{"dealID":"724ca365"},{"dealID":"e6ba5e93"}],"responseSize":"ALL","itemResponseSize":"DEFAULT_WITH_PREEMPTIVE_LEAKING","widgetContext":{"pageType":"GoldBox","subPageType":"main","deviceType":"pc","refRID":"TW3AYWM2XCBXJBHK98AR","widgetID":"38e1504b-c126-4f80-b9f9-efce015af061","slotName":"slot-4"}}'
            pattern = re.compile(r'"amznMerchantID" : "\w+"', re.M)
            amznMerchantID = pattern.findall(html)[0]
            amznMerchantID = amznMerchantID.split(':')[1].strip().strip('"')

            pattern = re.compile(r'"sessionId" : "[-\w]+"', re.M)
            sessionId = pattern.findall(html)[0].split(':')[1].strip().strip('"')

            pattern = re.compile(r'var ue_id=\'\w+\'', re.M)
            refRID = pattern.findall(html)[0].split('=')[1].strip().strip('\'')

            pattern = re.compile(r'"widgetID" : "[-\w]+"', re.M)
            widgetID = pattern.findall(html)[0].split(':')[1].strip().strip('"')

            pattern = re.compile(r'"slotName" : "slot-\d*"', re.M)
            slotNames = pattern.findall(html)  # refRID
            slotNames = [slot.split(':')[1].strip().strip('"') for slot in slotNames]

            pattern = re.compile(r'"sortedDealIDs" : \[\s+(?!"sortedDealIDs" : \[)(.*)"dealDetails"', re.S)
            dealIDs = pattern.findall(html)[0]  # refRID
            pattern = re.compile(r'\w+', re.S)
            dealIDs = pattern.findall(dealIDs)  # refRID
            dealIDs = [{"dealID": dealid} for dealid in dealIDs]
            dealIDs = dealIDs[:numMAX]
            logger.info('len all category: {} dealIDs：{}'.format(category, len(dealIDs)))
            resultDatas = []
            while dealIDs:
                newDealIDs = dealIDs[:numSet]
                dealIDs = dealIDs[numSet:]
                categoryGoodInfos = self.getCategoryGoods(amznMerchantID, sessionId, newDealIDs, refRID, widgetID, slotNames)
                if categoryGoodInfos and categoryGoodInfos.get('dealDetails'):
                    if len(newDealIDs) != len(categoryGoodInfos['dealDetails']):
                        logger.error('lenDealIDs:{}, lenResdealDetails:{}'.format(len(newDealIDs), len(categoryGoodInfos['dealDetails'])))
                    des_datas = self.format_data(category, categoryGoodInfos['dealDetails'])
                    resultDatas.extend(des_datas)
            logger.info('len all BEST_DEAL category: {} dealIDs：{}'.format(category, len(des_datas)))
            return resultDatas
        except Exception, e:
            logger.error('categoryurl: {} error'.format(categoryurl))
            logger.error('dealCategory error: {}.'.format(e))
            return False


    '''
    function: 格式化数据
    @category: category 分类名
    @sourceDatas：从网页中post得到的原始数据 类型为字典
    @return: [{},{}]
    '''
    def format_data(self, category, sourceDatas):
        resultDatas = []
        Not_BEST_DEAL = 0
        for sourceData in sourceDatas.values():
            try:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                # resultData['KIND'.lower()] = category.encode('utf8') if isinstance(category, (str, unicode)) else category
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'goldbox'
                resultData['STATUS'.lower()] = '01'
                if 'BEST_DEAL' != sourceData.get('type'):
                    logger.debug('sourceData:{} type isn\'t "BEST_DEAL"'.format(sourceData))
                    Not_BEST_DEAL+=1
                    continue
                if not sourceData.get('impressionAsin'):
                    logger.error('sourceData:{} not find impressionAsin'.format(sourceData))
                    continue
                else:
                    resultData['PRODUCT_ID'.lower()] = sourceData.get('impressionAsin').strip()

                if not sourceData.get('egressUrl') and not sourceData.get('ingressUrl'):
                    logger.error('sourceData:{} not find egressUrl and ingressUrl'.format(sourceData))
                    continue
                else:
                    resultData['LINK'.lower()] = sourceData.get('egressUrl').strip() if sourceData.get('egressUrl').strip() else sourceData.get('ingressUrl').strip()

                if not sourceData.get('primaryImage'):
                    logger.error('sourceData:{} not find primaryImage'.format(sourceData))
                    continue
                else:
                    resultData['MAIN_IMAGE'.lower()] = sourceData.get('primaryImage').strip()


                resultData['NAME'.lower()] = sourceData.get('title').strip().replace('"',r'\"')
                resultData['DESCRIPTION'.lower()] = sourceData.get('description').strip().replace('"', r'\"')
                resultData['Currency'.lower()] = sourceData.get('currencyCode').strip() if sourceData.get('currencyCode') else 'USD'
                # 原数据float
                resultData['AMOUNT'.lower()] = sourceData.get('maxDealPrice') if sourceData.get('maxDealPrice') else 0
                resultData['HIGH_AMOUNT'.lower()] = sourceData.get('minDealPrice') if sourceData.get('minDealPrice') else 0
                resultData['Before_AMOUNT'.lower()] = sourceData.get('maxListPrice') if sourceData.get('maxListPrice') else 0
                resultData['Before_HIGH_AMOUNT'.lower()] = sourceData.get('minListPrice') if sourceData.get('minListPrice') else 0
                # 原数据bool
                resultData['COMMEND_FLAG'.lower()] = '1' if sourceData.get('isFeatured') else '0'
                resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                # 原数据int
                resultData['DISPLAY_COUNT'.lower()] = sourceData.get('totalReviews') if sourceData.get('totalReviews') else 0

                resultData['RESERVE'.lower()] = sourceData.get('dealID').strip()
                # resultData = [time.strftime('%Y-%m-%d',time.localtime(time.time()))]  #日期
                # resultData = [time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))]  #日期+时间
                resultDatas.append(resultData)
            except Exception, e:
                logger.error('format_data error: {}.'.format(e))
                logger.error('resultData: {}'.format(resultData))
        if Not_BEST_DEAL == len(sourceDatas):
            logger.error('This time all the goods are not BEST_DEAL')
        return resultDatas

    '''
    function: 格式化数据并将商品入库 存在不同detailId 对应相同商品的情况
    @source_datas: 原始数据
    @return: True or False
    '''
    def saveCategoryGoods(self, source_datas):
        try:
            result_insert, result_update = True, True
            logger.debug('source_datas: {}'.format(source_datas))
            # select_sql = 'SELECT ID,STATUS FROM market_product_raw WHERE CHANNEL="{channel}" and KIND="{kind}" AND PRODUCT_ID="{product_id}" AND RESERVE="{reserve}" ORDER BY CREATE_TIME DESC '
            select_sql = 'SELECT ID,STATUS FROM market_product_raw WHERE CHANNEL="{channel}" AND PRODUCT_ID="{product_id}" ORDER BY CREATE_TIME DESC '
            (insert_datas, update_datas) = self.__checkDatas(select_sql, source_datas)
            columns = ['CHANNEL', 'KIND', 'SITE', 'PRODUCT_ID', 'LINK', 'MAIN_IMAGE', 'NAME', 'DESCRIPTION', 'Currency',\
                       'AMOUNT', 'HIGH_AMOUNT', 'Before_AMOUNT', 'Before_HIGH_AMOUNT', 'COMMEND_FLAG', 'CREATE_TIME', 'DISPLAY_COUNT', 'RESERVE', 'STATUS']
            table = TABLE_NAME_GOODS
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
    '''
    function: 获取商品信息 post请求
    @source_datas: 原始数据
    @return: dict_res or None
    '''
    def getCategoryGoods(self, amznMerchantID, sessionId, newDealIDs, refRID, widgetID, slotNames):
        try:
            slotName = random.choice(slotNames)
            time_now = int(time.time() * 1000)
            URL = 'https://www.amazon.com/xa/dealcontent/v2/GetDeals?nocache={}'.format(time_now)
            HEADER = self.header
            data = '{"requestMetadata":{"marketplaceID":"' + amznMerchantID + '","clientID":"goldbox_mobile_pc","sessionID":"' + sessionId + '"},"dealTargets":' + str(newDealIDs).replace("'", '"') + \
                   ',"responseSize":"ALL","itemResponseSize":"DEFAULT_WITH_PREEMPTIVE_LEAKING","widgetContext":{"pageType":"GoldBox","subPageType":"main","deviceType":"pc","refRID":"' + refRID + '","widgetID":"' + widgetID + '","slotName":"' + slotName + '"}}'
            req = urllib2.Request(url=URL, headers=HEADER, data=data)
            con = self.__urlOpenRetry(req)
            if 200 == con.getcode():
                doc = con.read()
                response = zlib.decompress(doc, 16 + zlib.MAX_WBITS)
                dict_res = json.loads(response)
                logger.debug('dict_res: {}'.format(dict_res))
                con.close()
                return dict_res
            else:
                logger.error('getCategoryGoods:{} getcode isn\'t 200,{}'.format(newDealIDs, con.getcode()))
                return None
        except Exception, e:
            logger.error('getCategoryGoods:{} error: {}.'.format(newDealIDs, e))
            return None

  #通过url获取所需信息 包括 price
    def getGoodsInfos(self, goodsurl):
        try:
            html = self.get_html(goodsurl)
            soup = BeautifulSoup(html, 'lxml')
            try:
                price = soup.findAll(('div', {'class': "a-section a-spacing-none snsPriceBlock"}))[0].findAll('span', {'class': "a-size-large a-color-price"})[0].getText().strip('\n').strip(' ')
                return price
            except Exception, e:
                if e.message != 'list index out of range':
                    return
                else:
                    print 'retry get price'
            list_price = soup.findAll(('tr', {'id': "priceblock_ourprice_row"}))
            for list in list_price:
                try:
                    price = list.findAll('td', {'class': "a-span12"})[0].findAll('span',{'class': "a-size-medium a-color-price"})[0].getText().strip('\n').strip(' ')
                    break
                except Exception,e:
                    if e.message != 'list index out of range':
                        return
                    else:
                        continue
                        print 'retry get price'
            return price
        except Exception,e:
            print  e
            return None

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
            html = self.getHtml(self.department_url)
            pattern = re.compile(r'{\s*\"nodeId\" : \"\d+\",\s*\"category\" : \"[-,\';&\w\s\’]+\"\s*}', re.M)
            departments = pattern.findall(html)
            return [eval(i) for i in departments if eval(i)['category'].decode("utf-8") in self.white_department]
        except Exception, e:
            logger.error('__get_department error: {}'.format(e))
            raise

    def __rm_duplicate(self, scr_datas, match):
        goods = {}
        repeat_num = 0
        for data in scr_datas:
            if goods.get(data[match]):
                logger.debug('find repead data: {}'.format(data[match]))
            else:
                goods[data[match]] = data
                repeat_num += 1
        return [value for value in goods.itervalues()]
    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    def dealCategorys(self):
        try:
            formatUrl = 'https://www.amazon.com/gp/goldbox/?gb_f_GB-SUPPLE=enforcedCategories:{},sortOrder:BY_SCORE,dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL'
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            Urls = [[department.get('category'), formatUrl.format(department.get('nodeId'))] for department in departments]
            resultDatas = []
            for url in Urls:
                resultData = self.dealCategory(url[0], url[1])
                resultDatas.extend(resultData)
            logger.info('all categorys get data: {}'.format(len(resultDatas)))
            resultDatas = self.__rm_duplicate(resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data is repeated: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')
            return self.saveCategoryGoods(resultDatas)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')

    def __checkDatas(self, select_sql, sourcedatas):
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
                    sourcedata['ID'.lower()] = result[0].get('ID')
                    sourcedata['STATUS'.lower()] = result[0].get('STATUS', '01')
                    update_datas.append(sourcedata)
            except Exception, e:
                logger.error('__checkDatas\'s error: {}.'.format(e))
                logger.error('__checkDatas\'s sourcedata: {}.'.format(sourcedata))
                continue
        return (insert_datas,update_datas)

    def get_verify_code(self, img_url):
        HEADER = '''
                Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
                Accept-Encoding:gzip, deflate, br
                Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
                Connection:keep-alive
                Host:images-na.ssl-images-amazon.com
                User-Agent:{}
                '''
        header = self.__getDict4str(HEADER.format(useragent))
        html = self.getHtml(img_url, header)
        # img_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anazon_verify.jpg')
        img_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),os.path.basename(img_url))
        with open(img_name, 'wb') as f:
            f.write(html)
        im = Image.open(img_name)
        verify_code = image_to_string(im).strip()
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
                    select_sql = 'select VERIFY_CODE from {} where IMAGE_URL="{}"'.format(TABLE_NAME_VERIFY, verify_image_url)
                    result = self.mysql.sql_query(select_sql)
                    if result:
                        verify_code = result[0].get('VERIFY_CODE')
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
                    try:
                        if verify_image_url and verify_code:
                            insert_sql = 'insert into {} (IMAGE_URL, VERIFY_CODE) VALUES ("{}","{}")'. \
                                format(TABLE_NAME_VERIFY, verify_image_url, verify_code)
                            logger.debug('insert_sql: {}'.format(insert_sql))
                            self.mysql.sql_exec(insert_sql)
                    except Exception,e:
                        logger.error('insert verify code error: {}'.format(e))
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
            (insert_datas, update_datas) = self.__checkDatas(select_sql, result_datas)
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

    objCaptureAmazon = CaptureAmazon(useragent)
    # objCaptureAmazon.get_verify_code('https://images-na.ssl-images-amazon.com/captcha/usvmgloq/Captcha_jxqunydgna.jpg')
    # 获取所有类别id
    # objCaptureAmazon.get_department()
    # 查询并入库所有类别的商品信息

    objCaptureAmazon.dealCategorys()
    # 查询并入库首页推荐商品信息
    objCaptureAmazon.dealHomeGoods()
    # 查询dealID的商品信息
    # objCaptureAmazon.getCategoryGoods('ATVPDKIKX0DER','134-6378449-3791711', [{"dealID": "862d423c"}], 'HKWF4KQNXA8YEVZHWF5Y', '30c09623-33cf-4469-be4c-3e8293ae0ee9', ['slot-3', 'slot-4', 'slot-6'])
    # 查询并入库特别类别的商品信息
    # objCaptureAmazon.dealCategory('Kitchen', 'https://www.amazon.com/gp/goldbox/?gb_f_GB-SUPPLE=enforcedCategories:284507,sortOrder:BY_SCORE,dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL')
    # 获取具体网址的html信息
    # objCaptureAmazon.getHtml('https://images-na.ssl-images-amazon.com/captcha/usvmgloq/Captcha_jxqunydgna.jpg')
    # objCaptureAmazon.getHtml('https://www.amazon.com/gp/goldbox/?gb_f_GB-SUPPLE=enforcedCategories:2625373011,sortOrder:BY_SCORE,dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL')
    # 入库商品信息
    # objCaptureAmazon.saveCategoryGoods([{'status': '01', 'kind': '\xe5\xa5\xb3\xe8\xa3\x85\xe7\xb2\xbe\xe5\x93\x81', 'main_image': 'https://g-search1.alicdn.com/img/bao/uploaded/i4/imgextra/i2/96654238/TB2sKS2afal9eJjSZFzXXaITVXa_!!0-saturn_solar.jpg', 'product_id': '560327725588', 'description': u'\u534a\u9ad8\u9886\u9488\u7ec7\u6253\u5e95\u886b\u957f\u8896\u5957\u5934\u6bdb\u8863\u5973\u79cb\u51ac\u88c5\u65b0\u6b3e\u97e9\u7248\u5bbd\u677e\u767e\u642d\u77ed\u6b3e\u7ebf\u8863', 'site': 's.taobao', 'currency': 'CNY', 'amount': 49.8, 'create_time': '20171130205215', 'link': 'https://detail.tmall.com/item.htm?id=560327725588', 'detail_image': 'https://g-search1.alicdn.com/img/bao/uploaded/i4/imgextra/i2/96654238/TB2sKS2afal9eJjSZFzXXaITVXa_!!0-saturn_solar.jpg_180x180.jpg', 'display_count': 39201, 'channel': 'taobao', 'name': u'\u534a\u9ad8\u9886\u957f\u8896\u9488\u7ec7\u6253\u5e95\u886b\u5bbd\u677e\u6bdb\u8863\u5957\u5934\u77ed\u6b3e\u767e\u642d'}])
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

