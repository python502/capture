#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureHotels.py
# @Software: PyCharm
# @Desc    :
import os
from CaptureBase import CaptureBase
import re
import time
import json
import requests

from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime

class CaptureHotels(CaptureBase):
    #中国香港
    # home_url = 'https://www.hotels.com/'
    #新加坡#美国
    home_url = 'https://www.hotels.com/?locale=c.._U0pp5u1Sjg.&pos=c..6-R09f_2ERhFDT1a1lxE3Q..'
    destination_popular = 'https://www.hotels.com/landing/web/component/linkblocks/destination-popular-hotels'
    white_department = ['Hong Kong Hotels', 'Tokyo Hotels', 'Taipei Hotels', 'Singapore Hotels', 'Osaka Hotels', \
                         'Seoul Hotels', 'Paris Hotels', 'London Hotels', 'Bangkok Hotels', 'Macau Hotels', \
                          'Phuket Hotels','New York Hotels', 'Shanghai Hotels', 'Sydney Hotels', 'Kyoto Hotels', 'Los Angeles Hotels', \
                          'Bali Hotels','Las Vegas Hotels', 'Okinawa (and vicinity) Hotels', 'Kuala Lumpur Hotels', \
                          'Beijing Hotels', 'San Francisco Hotels', 'Rome Hotels', 'Milan Hotels']
    # white_department = ['Macau Hotels']
    HEADER = '''
        Host: www.hotels.com
        Connection: keep-alive
        Content-Length: 119
        Accept: application/json
        X-Requested-With: XMLHttpRequest
        User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36
        Content-Type: application/json; charset=UTF-8
        Accept-Encoding: gzip, deflate, br
        Accept-Language: zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            '''
    Channel = 'Hotels'
    uuid = ''
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureHotels, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureHotels, self).__del__()

    '''
    function: 获取类别信息
    @top_type: 0 Top Destinations  other Top Countries  此暂时不需要
    #[[,],[,]]
    @return: dict_res 
    '''
    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def __get_department(self, top_type=0):
        try:
            results = []
            page_source = self.getHtmlselenium(self.home_url)
            pattern = re.compile(r'data-uuid=\"[-\w]+\"', re.M)
            self.uuid = pattern.findall(page_source)[0][11:-1]
            soup = BeautifulSoup(page_source, 'lxml')
            if top_type == 0:
                logger.debug('get Top Destinations infos')
                page_infos = soup.find('ul', {'class': 'widget-toggle-bd', 'id':'widget-toggle-i0-e0'}).findAll('li')
            else:
                page_infos = soup.find('ul', {'class': 'widget-toggle-bd', 'id':'widget-toggle-i0-e1'}).findAll('li')
            for page_info in page_infos:
                url = page_info.find('a').attrs['href']
                category = page_info.find('a').attrs['title']
                if category not in self.white_department:
                    logger.debug('category: {} not in white_department'.format(category))
                    continue
                results.append([category, url])
            return results
        except Exception, e:
            logger.error('__get_department error: {}, retry it'.format(e))
            raise

    def get_department(self):
        print '*'*40
        logger.info('get_department: {}'.format(self.__get_department()))
        print '*'*40

    '''
    function: 获取并存储首页滚动栏的推荐信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            page_source = self.getHtmlselenium(self.home_url)
            soup = BeautifulSoup(page_source, 'lxml')
            pre_load_data = soup.find('div', {'class': 'widget-carousel-enabled'}).find('div', {'class': 'resp-row resp-section'}).findAll('div',{'role':"option"})
            print len(pre_load_data)
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = load_data.find('a').attrs['href']
                    resultData['MAIN_IMAGE'.lower()] = load_data.find('img').attrs['data-src']
                    resultData['TITLE'.lower()] = load_data.find('span', {'class': 'teaser-title'}).getText().strip()
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','DESCRIPTION','MAIN_IMAGE','NAME','RESERVE', 'DISPLAY_COUNT', 'Currency','CREATE_TIME','STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')

    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department):
        category = department[0]
        url = department[1]
        try:
            page_results = self.getHotelInfos(category, url)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(page_results)))
            return page_results
        except Exception, e:
            logger.error('category: {} error'.format(category))
            logger.error('dealCategory error: {}.'.format(e))
            return False

    '''
    function: 使用PHANTOMJS浏览器获取js执行后的html
    @url:  url
    @return: html
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def __getHtmlselenium(self, url, timeout=30):
        from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
        from selenium import webdriver
        driver = None
        try:
            driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
            #加载页面的超时时间
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            driver.get(url)
            startTime = datetime.now()
            endTime = datetime.now()
            while ((endTime-startTime).seconds < timeout):
                try:
                    driver.find_element_by_xpath('//*[@id="citylanding-core"]/div[2]/div[2]/div[3]/div/div/div/div/script')
                    break
                except Exception, e:
                    time.sleep(1)
                endTime = datetime.now()
            driver.implicitly_wait(10)
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
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=3000)
    def getHotelInfos(self, category, pageurl):
        try:
            logger.debug('pageurl: {}'.format(pageurl))
            result_datas = []
            page_source = self.__getHtmlselenium(pageurl)
            soup = BeautifulSoup(page_source, 'lxml')
            hotel_infos = json.loads(soup.find('div', {'class': 'popular-hotels'}).find('script').getText().strip())
            # # 国内没有被墙
            # data = {"uuid": self.uuid, "context": pageurl}
            # hotel_infos = json.loads(self.getHtml(self.destination_popular, self.header, data=json.dumps(data)))
            resultDatas = {}
            for hotel_info in hotel_infos:
                if hotel_info['@type'] == 'Review':
                    link = hotel_info['itemReviewed']['sameAs']
                    if not resultDatas.get(link):
                        resultDatas[link] = {}
                    hotel_description = CaptureHotels.filter_emoji(hotel_info['reviewBody'])
                    resultDatas[link]['DESCRIPTION'.lower()] = hotel_description
                else:
                    link = hotel_info[u'@id']
                    if not resultDatas.get(link):
                        resultDatas[link] = {}
                        resultDatas[link]['CHANNEL'.lower()] = self.Channel
                        resultDatas[link]['KIND'.lower()] = category
                        resultDatas[link]['SITE'.lower()] = 'Top Destinations'
                        resultDatas[link]['STATUS'.lower()] = '01'
                    resultDatas[link]['LINK'.lower()] = hotel_info[u'@id']
                    resultDatas[link]['MAIN_IMAGE'.lower()] = hotel_info['image']
                    pattern = re.compile(r'\d+', re.M)
                    host_id = pattern.findall(hotel_info[u'@id'])[0]
                    resultDatas[link]['PRODUCT_ID'.lower()] = host_id
                    resultDatas[link]['NAME'.lower()] = hotel_info['name']
                    resultDatas[link]['Currency'.lower()] = 'USD'
                    resultDatas[link]['DISPLAY_COUNT'.lower()] = hotel_info['aggregateRating']['reviewCount']
                    resultDatas[link]['RESERVE'.lower()] = str(hotel_info['aggregateRating']['ratingValue'])
                    resultDatas[link]['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))

                result_datas = [value for value in resultDatas.itervalues()]
            if not result_datas:
                raise ValueError('getHotelInfos get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getHotelInfos error:{},retry it'.format(e))
            logger.error('getHotelInfos category: {}, pageurl：{}'.format(category, pageurl))
            raise

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureHotels = CaptureHotels(useragent)
    # objCaptureHotels.get_department()
    objCaptureHotels.dealCategorys()
    # objCaptureHotels.getHotelInfos('Hong Kong Hotels', 'https://www.hotels.com/de606379/hotels-hong-kong-hong-kong/')
    # # 查询并入库首页推荐商品信息
    objCaptureHotels.dealHomeGoods()
    import urllib
    # data = {"uuid":"9603a59b-569d-4d5a-8afe-a5ef40e8d5d6","context":"https://www.hotels.com/de726784/hotels-tokyo-japan/"}
    # print objCaptureHotels.getHtml('https://www.hotels.com/landing/web/component/linkblocks/destination-popular-hotels', data)
    endTime = datetime.now()

    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

