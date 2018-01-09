#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureShopee.py
# @Software: PyCharm
# @Desc    :
from CaptureBase import CaptureBase
import time
import json
from CrawlingProxy import CrawlingProxy,useragent
from logger import logger
from retrying import retry
from datetime import datetime
from urlparse import urljoin

class CaptureShopee(CaptureBase):
    banners_url = 'https://shopee.sg/api/banner/get_list?type=activity'
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
        category_url = department[2]
        try:
            page_url = 'https://shopee.sg/api/v1/search_items/?by=pop&order=desc&newest=0&limit=100&categoryids={}'.format(department[1])
            page_results = self.getGoodInfos(category, category_url, page_url)
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
    @firsturl： 商品页url
    @return: True or False or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=3000)
    def getGoodInfos(self, category, category_url, page_url):
        headers = {"cookie": "csrftoken=se23P6QTCViDCMbZuVNgZXs2rqohW4ZA",
                   "referer": category_url,
                   "x-csrftoken": "se23P6QTCViDCMbZuVNgZXs2rqohW4ZA"}
        result_datas = []
        format_url = '{}-i.{}.{}'
        image_url = 'https://cfshopeesg-a.akamaihd.net/file/{}'
        try:
            iterms_infos = self.getHtml(page_url, self.header)
            iterms_infos = json.loads(iterms_infos)['items']
            page_url = 'https://shopee.sg/api/v1/items/'
            data = {"item_shop_ids": iterms_infos}
            iterms_infos = self.getHtml(page_url, headers, data=json.dumps(data))
            goods_infos = json.loads(iterms_infos)
            for goods_info in goods_infos:
                resultData = {}
                resultData['CHANNEL'.lower()] = self.Channel
                resultData['KIND'.lower()] = category
                resultData['SITE'.lower()] = 'category'
                resultData['STATUS'.lower()] = '01'
                good_title = goods_info.get('name')
                good_title = self.filter_emoji(good_title)
                good_title = good_title.encode('utf-8')
                resultData['NAME'.lower()] = good_title
                shopid = goods_info.get('shopid')
                itemid = goods_info.get('itemid')
                link_name = good_title.replace(' ', '-').replace('100%', '100%25').replace(':', '').replace('™', '%E2%84%A2').replace('15%', '15')
                link_after = format_url.format(link_name, shopid, itemid)
                good_link = urljoin(self.home_url, link_after)
                resultData['LINK'.lower()] = good_link
                # try:
                #     self.getHtml(good_link, {'user-agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'})
                # except Exception:
                #     print good_link

                resultData['PRODUCT_ID'.lower()] = itemid
                resultData['MAIN_IMAGE'.lower()] = image_url.format(goods_info.get('image'))

                BeforePriceInfo = goods_info.get('price_before_discount', 0)
                resultData['Before_AMOUNT'.lower()] = self.format_price(BeforePriceInfo, 5)

                PriceInfo = goods_info.get('price', 0)
                resultData['AMOUNT'.lower()] = self.format_price(PriceInfo, 5)

                resultData['Currency'.lower()] = goods_info.get('currency')
                resultData['DISPLAY_COUNT'.lower()] = goods_info.get('show_discount')
                resultData['COMMEND_FLAG'.lower()] = '1' if goods_info.get('is_shopee_verified') else '0'
                resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                result_datas.append(resultData)
            if not result_datas:
                raise ValueError('get result_datas error')
            return result_datas
        except Exception, e:
            logger.error('getGoodInfos error:{}'.format(e))
            logger.error('category: {},category_url: {}'.format(category, category_url))
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
            format_main = 'https://shopee.sg/{}-cat.{}'
            format_sub = 'https://shopee.sg/{}-cat.{}.{}'
            results_f = []
            results_s = []
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
                results_f.append([category, cat_id, url])
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
                        results_s.append([category, sub_cat_id, url])
            return results_s if self.add_sub else results_f
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
            replace_insert_columns = ['CHANNEL','KIND','SITE','PRODUCT_ID','LINK','MAIN_IMAGE','NAME','COMMEND_FLAG', 'Currency', 'AMOUNT','Before_AMOUNT','CREATE_TIME','STATUS']
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
            page_source = self.getHtml(self.banners_url, self.header)
            pre_load_data = json.loads(page_source)['banners']
            for load_data in pre_load_data:
                try:
                    logger.debug('load_data: {}'.format(load_data))
                    resultData = {}
                    resultData['CHANNEL'.lower()] = self.Channel
                    resultData['STATUS'.lower()] = '01'
                    resultData['LINK'.lower()] = load_data.get('navigate_params').get('url')
                    resultData['MAIN_IMAGE'.lower()] = load_data.get('banner_image')
                    resultData['TITLE'.lower()] = load_data.get('navigate_params').get('navbar').get('title')
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
    objCaptureShopee = CaptureShopee(useragent)
    # 获取所有类别id
    # objCaptureShopee.get_department()
    # 查询并入库所有类别的商品信息
    objCaptureShopee.dealCategorys()
    # # 查询并入库首页推荐商品信息
    # objCaptureShopee.dealHomeGoods()
    # print objCaptureShopee.getGoodInfos('aaaa','https://shopee.sg/Mobile-Gadgets-cat.8?page=0')
    # print objCaptureShopee.getHtml('https://shopee.sg/api/banner/get_list?type=activity',objCaptureShopee.header)
    # print objCaptureShopee.getHtmlselenium('https://shopee.sg')
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

