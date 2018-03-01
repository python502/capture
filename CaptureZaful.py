#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureZaful.py
# @Software: PyCharm
# @Desc    :
from CaptureBase import CaptureBase
import time
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from bs4 import BeautifulSoup
from retrying import retry
from datetime import datetime

class CaptureZaful(CaptureBase):
    home_url = 'https://www.zaful.com/'
    # white_department = [u'NEW', u'Women', u'Dresses', u'Tops', u'Swimwear', \
    #                      u'Men', u'Sports', u'Accessories', u'SALE', u'Z-Me']
    white_department = [u'Women', u'Dresses', u'Tops', u'Swimwear', \
                         u'Men', u'Sports', u'Accessories', u'SALE']
    HEADER = '''
            accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Cache-Control:max-age=0
            Upgrade-Insecure-Requests:1
            User-Agent:{}
            '''
    Channel = 'zaful'

    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureZaful, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureZaful, self).__del__()

    '''
    function: 查询并将商品入库
    @department: 存储需要信息的列表
    @return: True or False
    '''
    def dealCategory(self, department):
        goods_infos = []
        category = department[0]
        try:
            page_num = 120#每页显示多少商品
            get_page = 1
            formaturl='{}g_{}.html?pz={}'
            for i in range(1, get_page+1):
                page_url = formaturl.format(department[1], i, page_num)
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
            page_source = self.getHtml(pageurl, self.header)
            soup = BeautifulSoup(page_source, 'lxml')
            if category == 'SALE':
                goods_infos = soup.find('section', {'class': 'mianBox fr'}).find('div', {'class': 'proList clearfix mt30'}).findAll(
                    'li')
            else:
                goods_infos = soup.find('section',{'class':'mianBox fr'}).find('ul',{'class':'clearfix'}).findAll('li')
            i = 0
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'category'
                resultData['STATUS'.lower()] = '01'
                i+=1
                try:
                    if category == 'SALE':
                        resultData['LINK'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('a', {'class': "pic js_exposure"}).attrs['href']
                        resultData['MAIN_IMAGE'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('a',{'class': "pic js_exposure"}).find('img').attrs['data-original']
                        resultData['NAME'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('a', {'class': "pic js_exposure"}).find('img').attrs['alt']
                        resultData['PRODUCT_ID'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('span', {'class': "like-icon js-addToFav "}).attrs['data-gid']
                    else:
                        resultData['LINK'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('a', {'class':"pic logsss_event_cl"}).attrs['href']
                        resultData['MAIN_IMAGE'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('a',{'class': "pic logsss_event_cl"}).find('img').attrs['data-original']
                        resultData['NAME'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('a', {'class': "pic logsss_event_cl"}).attrs['title']
                        resultData['PRODUCT_ID'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('span', {'class': "like-icon js-addToFav logsss_event_cl "}).attrs['data-gid']

                    try:
                        PriceInfo = goods_info.find('p', {'class': 'goods-price f14'}).find('span', {'class': "shop-price"}).find('strong', {'class': "my_shop_price"}).attrs['data-orgp']
                        good_maxDealPrice = float(PriceInfo)
                        resultData['AMOUNT'.lower()] = good_maxDealPrice
                    except Exception, e:
                        resultData['AMOUNT'.lower()] = 0
                    try:
                        resultData['Currency'.lower()] = goods_info.find('p', {'class': 'goods-price f14'}).find('span', {'class': "shop-price"}).find('strong', {'class': "bizhong"}).getText().strp()
                    except Exception, e:
                        resultData['Currency'.lower()] = 'USD'

                    try:
                        BeforepriceInfo = goods_info.find('p', {'class': 'goods-price f14'}).find('del', {'class': "market-price"}).find('strong', {'class': "my_shop_price"}).attrs['data-orgp']
                        good_maxBeforeDealPrice = float(BeforepriceInfo)
                        resultData['Before_AMOUNT'.lower()] = good_maxBeforeDealPrice
                    except Exception, e:
                        resultData['Before_AMOUNT'.lower()] = 0

                    try:
                        resultData['RESERVE'.lower()] = goods_info.find('div', {'class': 'img-hover-wrap'}).find('span', {'class':"copy-text popular"}).getText().strip()
                    except Exception, e:
                        resultData['RESERVE'.lower()] = ''
                    resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    result_datas.append(resultData)
                except Exception, e:
                    logger.error('error: {}'.format(e))
                    logger.error('goods_info: {}'.format(i))
                    continue

            # if len(goods_infos) != len(result_datas) or not result_datas:
            #     logger.error('len goods_infos: {},len result_datas: {}'.format(goods_infos, result_datas))
            #     raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            # logger.error('getGoodInfos error:{},retry it'.format(e))
            # logger.error('category: {},pageurl：{}'.format(category, pageurl))
            logger.error('page_source: {}'.format(page_source))
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
            html = self.getHtml(self.home_url, self.header)
            soup = BeautifulSoup(html, 'lxml')
            catalogs = soup.find('ul', {'class': 'f14 navWrap'}).children
            for catalog in catalogs:
                if catalog.name != 'li':
                    continue
                url = catalog.find('a').attrs['href']
                kind = catalog.find('a').find('span').getText().strip()
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


def main():
    startTime = datetime.now()

    objCaptureZaful = CaptureZaful(useragent)
    # 获取所有类别id
    # objCaptureZaful.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureZaful.dealCategorys()
    # objCaptureZaful.getGoodInfos('SALE', 'https://www.zaful.com/sale/?pz=120' )

    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

