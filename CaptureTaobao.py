#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureTaobao.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
import random
import urllib2
import os
import urllib
import zlib
import re
import cookielib
import json
import time
from urllib import urlencode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from CrawlingProxy import CrawlingProxy,useragent
from MysqldbOperate import MysqldbOperate
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin

DICT_MYSQL = {'host': '127.0.0.1', 'user': 'root', 'passwd': '111111', 'db': 'capture', 'port': 3306}
TABLE_NAME_GOODS = 'market_product_raw'
TABLE_NAME_HOME = 'market_banner_raw'
TABLE_NAME_VERIFY = 'market_verify_raw'
'''
classdocs
'''
class CaptureTaobao(object):
    phantomjs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'phantomjs.exe')
    home_url = 'https://world.taobao.com/'
    good_url = 'https://detail.tmall.com/item.htm?id={}'
    http_url = 'https:{}'
    User_Agent = '''
            scheme:https
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:en-US;q=0.8,en;q=0.7
            cache-control:max-age=0
            upgrade-insecure-requests:1
            User-Agent:{}
            '''
    Channel = 'taobao'

    def __init__(self, user_agent, proxy_ip=None):
        '''
        Constructor
        '''
        # Cookie and Referer is'not necessary
        self.result_url = []
        self.user_agent = user_agent
        self.insert = 0
        self.update = 0
        self.get_page = 3
        src_header = self.User_Agent.format(self.user_agent)
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

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getHtmlselenium(self, url):
        driver = None
        try:
            # # 全请求头设定
            # # 使用copy()防止修改原代码定义dict
            # cap = DesiredCapabilities.PHANTOMJS.copy()
            # for key, value in self.header.items():
            #     cap['phantomjs.page.customHeaders.{}'.format(key)] = value

            driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
            #加载页面的超时时间
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)
            driver.get(url)
            page = driver.page_source.encode('utf-8') if isinstance(driver.page_source, (str, unicode)) else driver.page_source
            logger.debug('driver.page_source: {}'.format(page))
            return page
        except Exception, e:
            logger.error('getHtmlselenium error:{},retry it'.format(e))
            raise
        finally:
            if driver:
                driver.quit()
    '''
    function: getHtml 根据url获取页面的html
    @url:  url
    @return: html
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getHtml(self, url):
        HEADER = self.header
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



    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40

    '''
    function: 获取类别信息,方法内将汉字转换成urlencode编码
    #[[u'\u5973\u88c5\u7cbe\u54c1', u'//s.taobao.com/search?q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247'],]
    @return: list 
    '''
    def __get_department(self):
        try:
            format_url = 'https:{}&sort=renqi-desc'
            result_departments = []
            html = self.getHtml(self.home_url)
            pattern = re.compile('<script class="J_ContextData" type="text/template">(.*?)</script>', re.S)
            infos = pattern.findall(html)
            infos = json.loads(infos[0].strip())['category']
            for info in infos:
                logger.debug('info: {}'.format(info))
                result_department = []
                result_department.append(info['title'])
                pattern = re.compile(u'[^\u4E00-\u9FA5]')
                filtered_str = pattern.sub(r' ', info['href']).strip()
                if filtered_str:
                    tmp = {'name': filtered_str.encode('utf-8')}
                    tmp_str = urllib.urlencode(tmp)[5:]
                    pattern = re.compile(u'[\u4E00-\u9FA5]+')
                    url_after = pattern.sub(tmp_str, info['href']).strip()
                    result_department.append(format_url.format(url_after.encode('utf-8')))
                else:
                    result_department.append(format_url.format(info['href'].encode('utf-8')))
                result_departments.append(result_department)
            logger.debug('__get_department: {}'.format(result_departments))
            return result_departments
        except Exception, e:
            logger.error('__get_department error: {}'.format(e))
            logger.error('html: {}'.format(html))
            raise

    '''
    function: 获取所有分类的商品信息
    @
    @return: None
    '''
    def dealCategorys(self):
        try:
            results = []
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            for department in departments:
                try:
                    result = self.dealCategory(department[0], department[1])
                    results.append([department[0], result])
                except Exception, e:
                    logger.error('end do dealCategory error: {}'.format(e))
                    logger.error('end category {} error'.format(department[0].encode('utf-8')))
                    logger.error('end department {}'.format(department))
                    results.append([department[0], False])
                    continue
            logger.info('insert: {}, update: {}'.format(self.insert, self.update))
            logger.info('end of crawler result:{}'.format(results))
            # return self.saveCategoryGoods(results)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))


    def __checkHomeDatas(self, select_sql, sourcedatas):
        insert_datas=[]
        update_datas=[]
        for sourcedata in sourcedatas:
            sql = select_sql.format(sourcedata[0], sourcedata[1])
            logger.debug('select sql: {}'.format(sql))
            try:
                result = self.mysql.sql_query(sql)
                if not result:
                    insert_datas.append(sourcedata)
                else:
                    if len(result) != 1:
                        logger.error('checkHomeDatas get many lines:{}'.format(result))
                        logger.error('select_sql: {}'.format(sql))
                    sourcedata.insert(0, result[0].get('ID'))
                    update_datas.append(sourcedata)
            except Exception, e:
                logger.error('checkHomeDatas\'s error: {}.'.format(e))
                logger.error('checkHomeDatas\'s sourcedata: {}.'.format(sourcedata))
                continue
        return (insert_datas,update_datas)

    '''
    function: 分类原始数据。区分insert 还是 update
    @select_sql: select sql
    @sourcedatas: 原始数据
    @return: (insert_datas, update_datas)
    '''
    def __checkCategoryDatas(self, select_sql, sourcedatas):
        insert_datas=[]
        update_datas=[]
        for sourcedata in sourcedatas:
            sql = select_sql.format(sourcedata[0], sourcedata[1].encode('utf8') if isinstance(sourcedata[1], (str, unicode)) else sourcedata[1], sourcedata[4])
            logger.debug('select sql: {}'.format(sql))
            try:
                result = self.mysql.sql_query(sql)
                if not result:
                    insert_datas.append(sourcedata)
                else:
                    if len(result) != 1:
                        logger.error('checkCategoryDatas get many lines:{}'.format(result))
                        logger.error('select_sql: {}'.format(sql))
                    sourcedata.insert(0, result[0].get('ID'))
                    update_datas.append(sourcedata)
            except Exception, e:
                logger.error('checkCategoryDatas\'s error: {}.'.format(e))
                logger.error('checkCategoryDatas\'s sourcedata: {}.'.format(sourcedata))
                continue
        return (insert_datas, update_datas)

    '''
    function: 存储分类商品信息
    @good_datas： 商品信息s
    @return: True or False or raise
    '''
    def saveCategoryGoods(self, good_datas):
        try:
            if not good_datas:
                logger.error('not get datas to save')
                return False
            select_sql = 'SELECT ID FROM market_product_raw WHERE CHANNEL="{}" and KIND="{}" AND NUMBER="{}" ORDER BY CREATE_TIME DESC '
            (insert_datas, update_datas) = self.__checkCategoryDatas(select_sql, good_datas)
            if insert_datas:
                l = len(insert_datas)
                self.insert += l
                logger.info('len insert_datas: {}'.format(l))
                insert_sql = 'INSERT INTO {}(CHANNEL,KIND,SITE,STATUS,Number,LINK,MAIN_IMAGE,NAME,DETAIL_IMAGE,DESCRIPTION,Currency,AMOUNT,CREATE_TIME,DISPLAY_COUNT) VALUES'.format(
                    TABLE_NAME_GOODS)
                result_insert = self.mysql.insert_batch(insert_sql, insert_datas)
                logger.info('result_insert: {}'.format(result_insert))
            if update_datas:
                l = len(update_datas)
                self.update += l
                logger.info('len update_datas: {}'.format(l))
                update_sql = 'REPLACE INTO {}(ID,CHANNEL,KIND,SITE,STATUS,Number,LINK,MAIN_IMAGE,NAME,DETAIL_IMAGE,DESCRIPTION,Currency,AMOUNT,CREATE_TIME,DISPLAY_COUNT) VALUES'.format(
                    TABLE_NAME_GOODS)
                result_update = self.mysql.insert_batch(update_sql, update_datas)
                logger.info('result_update: {}'.format(result_update))
            return True
        except Exception, e:
            logger.error('saveCategoryGoods error: {}.'.format(e))
            return False

    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getGoodInfos(self, category, pageurl, num):
        driver = None
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = self.user_agent
            dcap["phantomjs.page.settings.loadImages"] = False#禁止加载图片
            driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
            #加载页面的超时时间
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            driver.get(pageurl)
            driver.implicitly_wait(10)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            goods_infos = soup.select('div .item.J_MouserOnverReq')
            len_goods = len(goods_infos)
            # if num and len_goods != num:
            #     #会出现第一页只能加载36条记录的情况
            #     raise ValueError('get goods_infos{} not equal hope num{}'.format(len_goods, num))
            #只取前44条记录，后4条有可能和下一页重复
            if len_goods == 48:
                goods_infos = goods_infos[:44]
            for goods_info in goods_infos:
                resultData = [self.Channel, category, 's.taobao', '01']
                try:
                    good_id = goods_info.find_all('a', {'class':"pic-link J_ClickStat J_ItemPicA"})[0].attrs['data-nid']
                    resultData.append(good_id)
                    good_url = self.good_url.format(good_id)
                    resultData.append(good_url)
                    good_img_big = goods_info.find_all('img', {'class':"J_ItemPic img"})[0].attrs['data-src']
                    resultData.append(self.http_url.format(good_img_big))
                    good_title = goods_info.find_all('img', {'class':"J_ItemPic img"})[0].attrs['alt']
                    resultData.append(good_title)
                    try:
                        good_img_small = goods_info.find_all('img', {'class': "J_ItemPic img"})[0].get('src') if goods_info.find_all('img', {'class': "J_ItemPic img"})[0].get('src') else goods_info.find_all('img', {'class': "J_ItemPic img"})[0].get('data-ks-lazyload')
                        resultData.append(self.http_url.format(good_img_small))
                    except Exception, e:
                        logger.error('good_img_small error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                        resultData.append('')
                    try:
                        good_description = goods_info.find_all('a', {'class':"J_ClickStat"})[1].getText().strip().strip('\n')
                        resultData.append(good_description)
                    except Exception, e:
                        logger.error('good_description error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                        resultData.append('')
                    try:
                        good_maxDealPrice = goods_info.find_all('div', {'class':"price g_price g_price-highlight"})[0].getText().strip('\n').strip('')
                        currency = 'CNY'if good_maxDealPrice.encode('utf-8').startswith('¥') else 'USD'
                        pattern = re.compile(r'\d+.\d+', re.M)
                        good_maxDealPrice = float(pattern.findall(good_maxDealPrice)[0])
                        resultData.append(currency)
                        resultData.append(good_maxDealPrice)
                    except Exception, e:
                        logger.error('good_maxDealPrice error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                        resultData.append('USD')
                        resultData.append(0)

                    resultData.append(time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))
                    try:
                        good_dealcnt = goods_info.find_all('div', {'class':"deal-cnt"})[0].getText().strip('\n').strip('')
                        pattern = re.compile(r'^\d+', re.M)
                        good_dealcnt = int(pattern.findall(good_dealcnt)[0])
                        resultData.append(good_dealcnt)
                    except Exception, e:
                        logger.error('good_dealcnt error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                        resultData.append(0)
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    logger.error('goods_info: {}'.format(goods_info))
                    continue
            if len(goods_infos) != len(result_datas) or not result_datas:
                logger.error('len goods_infos: {},len result_datas: {}'.format(goods_infos, result_datas))
                logger.error('result_datas: {}'.format(result_datas))
                raise ValueError
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos error:{},retry it'.format(e))
            raise
        finally:
            if driver:
                driver.quit()
    '''
    function: 获取并存储单类商品信息
    @category： 分类名
    @firsturl： 分类首页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealCategory(self, category, firsturl):
        goods_infos = []
        try:
            page_infos = self.getPageInfos(firsturl)
            total_page = page_infos['totalPage']
            page_size = page_infos['pageSize']
            for i in range(min(total_page, self.get_page)):
                if i == 0:
                    num = 48
                elif i == total_page-1:
                    num = 0
                else:
                    num = page_size
                page_url = '{}&s={}'.format(firsturl, page_size*i)
                page_results = self.getGoodInfos(category, page_url, num)
                goods_infos.extend(page_results)
            # #去重复  测试发现可以不用
            # goods ={}
            # for info in goods_infos:
            #     if goods.get(info[4]):
            #         logger.error('get duplicate data')
            #         logger.error('info: {}'.format(info))
            #         logger.error('info stored: {}'.format(goods.get(info[4])))
            #     else:
            #         goods[info[4]] = info
            # goods_infos = [value for value in goods.itervalues()]
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category.encode('utf-8'), len(goods_infos)))
            return self.saveCategoryGoods(goods_infos) if goods_infos else False
        except Exception, e:
            logger.error('dealCategory {} error:{}'.format(category.encode('utf-8'), e))
            raise


    '''
    function: 获取分类商品信息(pageSize,totalPage,currentPage,totalCount)
    @return: dict or None
    '''
    def getPageInfos(self, good_url):
        html = self.getHtml(good_url)
        pattern = re.compile(r'"pager":\{"pageSize":\d+,"totalPage":\d+,"currentPage":\d+,"totalCount":\d+\}', re.M)
        infos = json.loads(pattern.findall(html)[0][8:])
        #{u'currentPage': 1, u'totalPage': 100, u'pageSize': 44, u'totalCount': 7718071}
        return infos


    '''
    function: 查询并存储首页滑动栏信息
    @return: True or Raise
    '''
    def dealHomeGoods(self):
        try:
            goods = {}
            html = self.getHtmlselenium(self.home_url)
            soup = BeautifulSoup(html, 'lxml')
            recommends = soup.find('div', {'id': "banner-slider", 'class': "image-slider"}).find_all('a',{'class':"tab-pannel slider-panel item"})
            for recommend in recommends:
                resultData = [self.Channel]
                resultData.append(recommend.attrs['href'])
                try:
                    img = recommend.find('img').attrs['data-ks-lazyload']
                except Exception, e:
                    logger.error('get img error:{}'.format(e))
                    img = recommend.find('img').attrs['src']
                if not img.startswith('https'):
                    img = self.http_url.format(img)
                resultData.append(img)
                resultData.append(time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())))
                good_name = recommend.attrs['data-color']
                goods[good_name] = resultData
            resultDatas = [value for value in goods.itervalues()]
            if len(resultDatas) == 0:
                logger.error('driver.page_source: {}'.format(html))
                raise ValueError('not get valid data')
            select_sql = 'SELECT ID FROM market_banner_raw WHERE CHANNEL="{}" and LINK="{}" ORDER BY CREATE_TIME DESC '
            (insert_datas, update_datas) = self.__checkHomeDatas(select_sql, resultDatas)
            column = 'CHANNEL, LINK, MAIN_IMAGE, CREATE_TIME'
            if insert_datas:
                length = len(insert_datas)
                self.insert += length
                logger.info('len insert_datas: {}'.format(length))
                insert_sql = 'INSERT INTO {}({}) VALUES'.format(TABLE_NAME_HOME, column)
                result_insert = self.mysql.insert_batch(insert_sql, insert_datas)
                logger.info('result_insert: {}'.format(result_insert))
            if update_datas:
                length = len(update_datas)
                self.update += length
                logger.info('len update_datas: {}'.format(length))
                update_sql = 'REPLACE INTO {}(ID,{}) VALUES'.format(TABLE_NAME_HOME, column)
                result_update = self.mysql.insert_batch(update_sql, update_datas)
                logger.info('result_update: {}'.format(result_update))
            return True
        except Exception, e:
            logger.error('dealHomeGoods error:{}'.format( e))
            raise

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureTaobao = CaptureTaobao(useragent)
    # 获取所有类别url
    # objCaptureTaobao.get_department()
    # objCaptureTaobao.getHtmlselenium('https://world.taobao.com/')

    #查询并入库所有类别的商品信息
    objCaptureTaobao.dealCategorys()

    # 查询并入库首页推荐商品信息
    objCaptureTaobao.dealHomeGoods()

    #查询商品总信息 例如总页数 总条数
    # objCaptureTaobao.getPageInfos(u'https://s.taobao.com/search?spm=a21wu.241046-cn.6977698868.5.2816e72eg2pkH9&q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247')

    #查询特别类别的商品信息 插入或跟新数据库
    #48个
    # objCaptureTaobao.dealCategory(u'\u5973\u88c5\u7cbe\u54c1', u'https://s.taobao.com/search?spm=a21wu.241046-cn.6977698868.5.2816e72eg2pkH9&q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247')
    # 36个
    # objCaptureTaobao.dealCategory(u'\u6c7d\u8f66/\u5a31\u4e50', u'https://s.taobao.com/search?q=%E6%B1%BD%E8%BD%A6%E7%94%A8%E5%93%81&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_149506538351411_1797247&sort=renqi-desc')
    # 获取具体网址的html信息
    # print objCaptureTaobao.getHtml(u'https://s.taobao.com/search?spm=a21wu.241046-cn.6977698868.5.2816e72eg2pkH9&q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247')
    print objCaptureTaobao.getHtml('https://world.taobao.com/')
    # 入库商品信息
    # objCaptureTaobao.saveCategoryGoods([['taobao', u'\u5973\u88c5\u7cbe\u54c1', 's.taobao', '01', '544980174926', 'https://detail.tmall.com/item.htm?id=544980174926', u'\u6625\u79cb\u65b0\u6b3e\u4e00\u5b57\u9886\u9488\u7ec7\u886b\u957f\u8896\u77ed\u6b3e\u7d27\u8eab\u6bdb\u8863\u5973', 'https://detail.tmall.com/item.htm?id=//g-search1.alicdn.com/img/bao/uploaded/i4/imgextra/i4/1748305007546288692/TB25MFUemFjpuFjSszhXXaBuVXa_!!0-saturn_solar.jpg_180x180.jpg', 'https://detail.tmall.com/item.htm?id=//g-search1.alicdn.com/img/bao/uploaded/i4/imgextra/i4/1748305007546288692/TB25MFUemFjpuFjSszhXXaBuVXa_!!0-saturn_solar.jpg_180x180.jpg', u'\u6625\u79cb\u65b0\u6b3e\u4e00\u5b57\u9886\u9488\u7ec7\u886b\u97e9\u7248\u5973\u88c5\u4fee\u8eab\u6253\u5e95\u886b\u957f\u8896\u77ed\u6b3e\u7d27\u8eab\u6bdb\u8863\u5973\u5957\u5934', 'CNY', 59.0, time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())),5037]])
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()


