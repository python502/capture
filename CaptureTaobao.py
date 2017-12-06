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
import os
import urllib
import re
import json
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from CaptureBase import CaptureBase

TABLE_NAME_GOODS = 'market_product_raw'
TABLE_NAME_HOME = 'market_banner_raw'
'''
classdocs
'''
class CaptureTaobao(CaptureBase):
    phantomjs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'phantomjs.exe')
    home_url = 'https://world.taobao.com/'
    good_url = 'https://detail.tmall.com/item.htm?id={}'
    http_url = 'https:{}'
    HEADER = '''
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
        super(CaptureTaobao, self).__init__(user_agent, proxy_ip)
        self.get_page = 3
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureTaobao, self).__del__()

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
            html = self.getHtml(self.home_url, self.header)
            pattern = re.compile('<script class="J_ContextData" type="text/template">(.*?)</script>', re.S)
            infos = pattern.findall(html)
            infos = json.loads(infos[0].strip())['category']
            for info in infos:
                logger.debug('info: {}'.format(info))
                result_department = []
                result_department.append(info['title'].encode('utf-8'))
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
            resultDatas = []
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            for department in departments:
                try:
                    result = self.dealCategory(department[0], department[1])
                    resultDatas.extend(result)
                except Exception, e:
                    logger.error('end do dealCategory error: {}'.format(e))
                    logger.error('end category {} error'.format(department[0]))
                    logger.error('end department {}'.format(department))
                    continue
            logger.info('all categorys get data: {}'.format(len(resultDatas)))
            resultDatas = self._rm_duplicate(resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data is repeated: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')
            return self.saveCategoryGoods(resultDatas)
            logger.info('end of crawler result:{}'.format(results))
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))


    '''
    function: 存储分类商品信息
    @good_datas： 商品信息s
    @return: True or False or raise
    '''
    def saveCategoryGoods(self, good_datas):
        try:
            result_insert, result_update = True, True
            table = TABLE_NAME_GOODS
            columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','DETAIL_IMAGE','DESCRIPTION','Currency','AMOUNT','CREATE_TIME','DISPLAY_COUNT','STATUS']
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
            # if len_goods == 48:
            #     goods_infos = goods_infos[:44]
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 's.taobao'
                resultData['STATUS'.lower()] = '01'
                try:
                    good_id = goods_info.find_all('a', {'class':"pic-link J_ClickStat J_ItemPicA"})[0].attrs['data-nid']
                    resultData['PRODUCT_ID'.lower()] = good_id
                    good_url = self.good_url.format(good_id)
                    resultData['LINK'.lower()] = good_url
                    good_img_big = goods_info.find_all('img', {'class': "J_ItemPic img"})[0].attrs['data-src']
                    resultData['MAIN_IMAGE'.lower()] = self.http_url.format(good_img_big)
                    good_title = goods_info.find_all('img', {'class': "J_ItemPic img"})[0].attrs['alt']
                    resultData['NAME'.lower()] = good_title
                    try:
                        good_img_small = goods_info.find_all('img', {'class': "J_ItemPic img"})[0].get('src') if goods_info.find_all('img', {'class': "J_ItemPic img"})[0].get('src') else goods_info.find_all('img', {'class': "J_ItemPic img"})[0].get('data-ks-lazyload')
                        resultData['DETAIL_IMAGE'.lower()] = self.http_url.format(good_img_small)
                    except Exception, e:
                        logger.error('good_img_small error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                    try:
                        good_description = goods_info.find_all('a', {'class':"J_ClickStat"})[1].getText().strip().strip('\n')
                        resultData['DESCRIPTION'.lower()] = good_description
                    except Exception, e:
                        logger.error('good_description error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))

                    try:
                        good_maxDealPrice = goods_info.find_all('div', {'class':"price g_price g_price-highlight"})[0].getText().strip('\n').strip('')
                        currency = 'CNY'if good_maxDealPrice.encode('utf-8').startswith('¥') else 'USD'
                        pattern = re.compile(r'\d+.\d+', re.M)
                        good_maxDealPrice = float(pattern.findall(good_maxDealPrice)[0])

                        resultData['Currency'.lower()] = currency
                        resultData['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception, e:
                        logger.error('good_maxDealPrice error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                        resultData['Currency'.lower()] = 'USD'
                        resultData['AMOUNT'.lower()] = 0

                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))

                    try:
                        good_dealcnt = goods_info.find_all('div', {'class':"deal-cnt"})[0].getText().strip('\n').strip('')
                        pattern = re.compile(r'^\d+', re.M)
                        good_dealcnt = int(pattern.findall(good_dealcnt)[0])
                        resultData['DISPLAY_COUNT'.lower()] = good_dealcnt
                    except Exception, e:
                        logger.error('good_dealcnt error: {}'.format(e))
                        logger.error('goods_info: {}'.format(goods_info))
                        resultData['DISPLAY_COUNT'.lower()] = 0
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
                logger.info('getGoodInfos category:{} page{} begin'.format(category, i+1))
                page_results = self.getGoodInfos(category, page_url, num)
                goods_infos.extend(page_results)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(goods_infos)))
            return goods_infos
        except Exception, e:
            logger.error('dealCategory {} error:{}'.format(category, e))
            raise


    '''
    function: 获取分类商品信息(pageSize,totalPage,currentPage,totalCount)
    @return: dict or None
    '''
    def getPageInfos(self, good_url):
        html = self.getHtml(good_url, self.header)
        pattern = re.compile(r'"pager":\{"pageSize":\d+,"totalPage":\d+,"currentPage":\d+,"totalCount":\d+\}', re.M)
        infos = json.loads(pattern.findall(html)[0][8:])
        #{u'currentPage': 1, u'totalPage': 100, u'pageSize': 44, u'totalCount': 7718071}
        return infos


    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or Raise
    '''
    def dealHomeGoods(self):
        try:
            goods = {}
            html = self.getHtmlselenium(self.home_url)
            soup = BeautifulSoup(html, 'lxml')
            recommends = soup.find('div', {'id': "banner-slider", 'class': "image-slider"}).find_all('a',{'class':"tab-pannel slider-panel item"})
            for recommend in recommends:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['STATUS'.lower()] = '01'
                resultData['LINK'.lower()] = recommend.attrs['href']
                try:
                    img = recommend.find('img').attrs['data-ks-lazyload']
                except Exception, e:
                    logger.error('get img error:{}'.format(e))
                    img = recommend.find('img').attrs['src']
                if not img.startswith('https'):
                    img = self.http_url.format(img)
                resultData['MAIN_IMAGE'.lower()] = img
                resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                good_name = recommend.attrs['data-color']
                goods[good_name] = resultData
            resultDatas = [value for value in goods.itervalues()]
            if len(resultDatas) == 0:
                logger.error('driver.page_source: {}'.format(html))
                raise ValueError('not get valid data')
            select_sql = 'SELECT ID,STATUS FROM market_banner_raw WHERE CHANNEL="{channel}" and LINK="{link}" ORDER BY CREATE_TIME DESC '
            (insert_datas, update_datas) = self._checkDatas(select_sql, resultDatas, ['ID', 'STATUS'])
            columns = ['CHANNEL', 'LINK', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS']
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
    # objCaptureTaobao.dealHomeGoods()

    #查询商品总信息 例如总页数 总条数
    # objCaptureTaobao.getPageInfos(u'https://s.taobao.com/search?spm=a21wu.241046-cn.6977698868.5.2816e72eg2pkH9&q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247')

    #查询特别类别的商品信息 插入或跟新数据库
    #48个
    # objCaptureTaobao.dealCategory(u'\u5973\u88c5\u7cbe\u54c1', u'https://s.taobao.com/search?spm=a21wu.241046-cn.6977698868.5.2816e72eg2pkH9&q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247')
    # 36个
    # objCaptureTaobao.dealCategory(u'\u6c7d\u8f66/\u5a31\u4e50', u'https://s.taobao.com/search?q=%E6%B1%BD%E8%BD%A6%E7%94%A8%E5%93%81&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_149506538351411_1797247&sort=renqi-desc')
    # 获取具体网址的html信息
    # print objCaptureTaobao.getHtml(u'https://s.taobao.com/search?spm=a21wu.241046-cn.6977698868.5.2816e72eg2pkH9&q=%E5%A5%B3%E8%A3%85&acm=lb-zebra-241046-2058600.1003.4.1797247&scm=1003.4.lb-zebra-241046-2058600.OTHER_14950676920071_1797247')
    # print objCaptureTaobao.getHtml('https://world.taobao.com/')
    # 入库商品信息
    # objCaptureTaobao.saveCategoryGoods([{'status': '01', 'kind': '\xe5\xa5\xb3\xe8\xa3\x85\xe7\xb2\xbe\xe5\x93\x81', 'main_image': 'https://g-search1.alicdn.com/img/bao/uploaded/i4/imgextra/i2/96654238/TB2sKS2afal9eJjSZFzXXaITVXa_!!0-saturn_solar.jpg', 'product_id': '560327725588', 'description': u'\u534a\u9ad8\u9886\u9488\u7ec7\u6253\u5e95\u886b\u957f\u8896\u5957\u5934\u6bdb\u8863\u5973\u79cb\u51ac\u88c5\u65b0\u6b3e\u97e9\u7248\u5bbd\u677e\u767e\u642d\u77ed\u6b3e\u7ebf\u8863', 'site': 's.taobao', 'currency': 'CNY', 'amount': 49.8, 'create_time': '20171130205215', 'link': 'https://detail.tmall.com/item.htm?id=560327725588', 'detail_image': 'https://g-search1.alicdn.com/img/bao/uploaded/i4/imgextra/i2/96654238/TB2sKS2afal9eJjSZFzXXaITVXa_!!0-saturn_solar.jpg_180x180.jpg', 'display_count': 39201, 'channel': 'taobao', 'name': u'\u534a\u9ad8\u9886\u957f\u8896\u9488\u7ec7\u6253\u5e95\u886b\u5bbd\u677e\u6bdb\u8863\u5957\u5934\u77ed\u6b3e\u767e\u642d'}])
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()


