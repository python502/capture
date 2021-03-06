#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureIshopchangi.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from CaptureBase import CaptureBase, TimeoutException
import time
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime
from urlparse import urljoin
import json
import urllib
import re
class CaptureIshopchangi(CaptureBase):
    home_url = 'https://www.ishopchangi.com/'
    img_url = 'https://www.ishopchangi.com/IMG/crmbanner/'
    banner_url = 'https://www.ishopchangi.com/AppEngine/cmsservice/GetContents.ashx?languageCode=en-US&contentPath=cagecom%2FBanners%2FDesktop%2Fen-US'
    product_url = 'https://www.ishopchangi.com/WSHub/wsProduct.asmx/GetProductGroupByFilterEx'
    HEADER = '''
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            cache-control:max-age=0
            upgrade-insecure-requests:1
            User-Agent:{}
            '''
    BANNER_HEADER = '''
            accept:application/json, text/javascript, */*; q=0.01
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            content-type:application/json
            referer:https://www.ishopchangi.com/
            user-agent:{}
            x-requested-with:XMLHttpRequest
            '''
    page_num = u'100'
    page_need = 1
    Channel = 'ishopchangi'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureIshopchangi, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))
        self.banner_header = self._getDict4str(self.BANNER_HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureIshopchangi, self).__del__()

    '''
    function: 查询并将商品入库
    @department: list
    @return: True or False
    '''
    def dealCategory(self, department):
        category = department[0]
        page_url = department[1]
        pattern = re.compile(r'\d+$', re.M)
        category_id = pattern.findall(page_url)[0]
        format_url = '{}?p={}'
        goods_infos = []
        infos = {}
        try:
            for i in xrange(1, self.page_need+1):
                page_url_tmp = format_url.format(page_url, i)
                infos['referer'] = page_url_tmp
                infos['current_page'] = str(i)
                infos['page_num'] = self.page_num
                infos['category_id'] = category_id
                page_results = self.getGoodInfos(category, infos)
                if not page_results or len(page_results) != int(self.page_num):
                    break
                goods_infos.extend(page_results)
            logger.info('dealCategory category:{} len goods_infos: {}'.format(category, len(goods_infos)))
            return goods_infos
        except Exception, e:
            logger.error('category: {} error'.format(category))
            logger.error('dealCategory error: {}.'.format(e))
            return False

    '''
    function: 获取单页商品信息
    @category： 分类名
    @firsturl： 商品页信息
    @return: True or False or raise
    '''
    # @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getGoodInfos(self, category, infos):
        try:
            logger.debug('pageurl: {}'.format(infos['referer']))
            result_datas = []
            header = '''
            accept:application/json, text/javascript, */*; q=0.01
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            content-type:application/json
            origin:https://www.ishopchangi.com
            referer:{}
            user-agent:{}
            x-requested-with:XMLHttpRequest'''
            header = self._getDict4str(header.format(infos['referer'], self.user_agent))
            data = {"categories":infos['category_id'],"brand":"","minPrice":0,"maxPrice":5000,"flightType":"departure|arrival|delivery|","filterBy":"","filterPageSize":infos['page_num'],"currentPageindex":infos['current_page'],"langType":"en-US","sortBy":"popularity","tagName":"","deliveryAvailable":""}
            data = json.dumps(data)
            response = self.getHtml(self.product_url, header, data)
            goods_infos = json.loads(json.loads(response)['d'])['Items']
            for goods_info in goods_infos:
                result_data = {}
                result_data['CHANNEL'.lower()] = self.Channel
                result_data['KIND'.lower()] = category
                result_data['STATUS'.lower()] = '01'
                result_data['Currency'.lower()] = 'SGD'
                try:
                    result_data['PRODUCT_ID'.lower()] = goods_info.get(u'ProductGroupAutoID')
                    format_link = 'https://www.ishopchangi.com/product/{}-{}-{}'
                    result_data['LINK'.lower()] = format_link.format(goods_info.get(u'ProductGroupTitleSlug'), goods_info.get(u'ProductGroupAutoID'), goods_info.get(u'LP_ConcessionaireAutoID'))
                    result_data['MAIN_IMAGE'.lower()] = goods_info.get(u'GroupImage')
                    result_data['NAME'.lower()] = goods_info.get(u'LP_BrandName')
                    result_data['AMOUNT'.lower()] = goods_info.get(u'Org_Disc_Price')
                    result_data['SITE'.lower()] = goods_info.get(u'CategoryDisplayName')
                    result_data['Before_AMOUNT'.lower()] = goods_info.get(u'Org_Price')
                    #0  soldout    1   notsoldout
                    result_data['RESERVE'.lower()] = 0 if goods_info.get(u'IsSoldOut') else 1
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
            logger.info('getGoodInfos category:{} len goods_infos: {}'.format(category, len(result_datas)))
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','Currency','AMOUNT','CREATE_TIME','STATUS','Before_AMOUNT','RESERVE']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealCategorys error: {}'.format(e))
        finally:
            logger.info('dealCategorys end')


    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def __getHtmlselenium(self, url, checkstr, header=False, timeout=30):
        from selenium import webdriver
        error_url = 'The product you are looking for cannot be found on our site.'
        driver = None
        try:
            if not header:
                driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
            else:
                driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
                driver.add_cookie({u'domain': u'.ishopchangi.com', u'name': u'productLoadPageSize', u'value': self.page_num,
                         u'path': u'/', u'httponly': False, u'secure': False})
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            driver.get(url)
            startTime = datetime.now()
            endTime = datetime.now()
            while ((endTime-startTime).seconds < timeout):
                try:
                    driver.find_element_by_xpath(checkstr)
                    break
                except Exception, e:
                    time.sleep(1)
                endTime = datetime.now()
            else:
                if header and driver.page_source.find(error_url) != -1:
                    return None
                raise TimeoutException('ishopchangi __getHtmlselenium timeout')
            driver.implicitly_wait(10)
            page = driver.page_source.encode('utf-8') if isinstance(driver.page_source, (str, unicode)) else driver.page_source
            logger.debug('driver.page_source: {}'.format(page))
            return page
        except Exception, e:
            logger.error('__getHtmlselenium error:{},retry it'.format(e))
            raise
        finally:
            if driver:
                driver.quit()
    '''
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            response = self.getHtml(self.banner_url, self.banner_header)
            pre_load_data = json.loads(response)['contents']
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['TITLE'.lower()] = load_data['title']
                    desc = load_data['desc']
                    if desc.startswith('https://'):
                        resultData['LINK'.lower()] = desc
                    elif desc.startswith('<'):
                        soup = BeautifulSoup(desc, 'lxml')
                        # if soup.find('a',{'class':"in-cell-link"}):
                        #     resultData['LINK'.lower()] = soup.find('a',{'class':"in-cell-link"}).attrs['href']
                        # else:
                        resultData['LINK'.lower()] = soup.find('span').getText().strip('\n').strip().strip('\n')
                    else:
                        logger.error('error banner desc :{}'.format(desc))
                        break
                    resultData['MAIN_IMAGE'.lower()] = urljoin(self.img_url, load_data['contentBanner'])
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('get eLement error:{}'.format(e))
                    logger.error('goodData: {}'.format(load_data))
                    continue
            result_datas = self._rm_duplicate(result_datas, 'LINK'.lower())
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(response))
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

    # @retry(stop_max_attempt_number=5, wait_fixed=3000)
    # def __get_department(self):
    #     try:
    #         results = []
    #         page_source = self.__getHtmlselenium(self.home_url, '//*[@id="navigationstrip"]')
    #         soup = BeautifulSoup(page_source, 'lxml')
    #         catalogs = soup.find('div', {'id': 'navigationstrip'}).findAll('div', {'class': 'holder notouch'})
    #         for catalog in catalogs:
    #             href = catalog.find('a').attrs['href'].replace('productpage', 'productlistings')
    #             url = urljoin(self.home_url, href)
    #             kind = catalog.find('a').getText().strip()
    #             kind = CaptureIshopchangi.filter_emoji(kind)
    #             result = [kind.encode('utf-8'), url]
    #             results.append(result)
    #         return results
    #     except Exception, e:
    #         logger.error('__get_department error: {}, retry it'.format(e))
    #         raise

    @retry(stop_max_attempt_number=5, wait_fixed=3000)
    def __get_department(self):
        try:
            results = []
            page_source = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            catalogs = soup.find('div', {'id': 'navigationstrip'}).findAll('div', {'class': 'holder notouch'})
            for catalog in catalogs:
                href = catalog.find('a').attrs['href'].replace('productpage', 'productlistings')
                url = urljoin(self.home_url, href)
                kind = catalog.find('a').getText().strip()
                kind = CaptureIshopchangi.filter_emoji(kind)
                result = [kind.encode('utf-8'), url]
                results.append(result)
            return results
        except Exception, e:
            logger.error('__get_department error: {}, retry it'.format(e))
            raise

def main():
    startTime = datetime.now()
    # objCrawlingProxy = CrawlingProxy()
    # proxy = objCrawlingProxy.getRandomProxy()
    # objCaptureAmazon = CaptureAmazon(useragent,proxy)

    objCaptureIshopchangi = CaptureIshopchangi(useragent)
    # 查询并入库所有类别的商品信息
    objCaptureIshopchangi.get_department()
    objCaptureIshopchangi.dealCategorys()
    # objCaptureIshopchangi.dealHomeGoods()
    # html = objCaptureIshopchangi.getHtmlselenium(objCaptureIshopchangi.home_url,sleep_time=30)
    # print html

    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

