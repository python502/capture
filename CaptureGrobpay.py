#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureGrobpay.py
# @Software: PyCharm
# @Desc    :
'''
Created on 2016年6月4日

@author: Administrator
'''
from CaptureBase import CaptureBase
import urllib
import random
from CrawlingProxy import UserAgents, useragent
import json
import time
from logger import logger
from datetime import datetime
KEY = 'AIzaSyANWO - 6gC75Bf_PPzBtPPuU2T5jUY7p51M'
Location2coordinate = {
    'Raffles Place Singapore':{'latitude':'1.2840999','longitude':'103.8513269'},
                       'Tanjong Pagar Singapore': {'latitude': '1.2764031', 'longitude': '103.84685850000005'},
                       'Tiong Bahru Road Singapore': {'latitude': '1.2865334', 'longitude': '103.8247887'},
                       'Mount Faber Road Singapore': {'latitude': '1.2712405', 'longitude': '103.81967559999998'},
                       'Buona Vista Singapore': {'latitude': '1.3073194', 'longitude': '103.78994220000004'},
                       'Clarke Quay Singapore': {'latitude': '1.291001', 'longitude': '103.84449889999996'},
                       'Bugis Junction Singapore': {'latitude': '1.2996001', 'longitude': '103.85512819999997'},
                       'Little India Singapore': {'latitude': '1.3065597', 'longitude': '103.85181899999998'},
                       'Orchard Road Singapore': {'latitude': '1.3017996', 'longitude': '103.83779709999999'},
                       'Bukit Timah Road Singapore': {'latitude': '1.3017996', 'longitude': '103.83779709999999'},
                       'Novena Singapore': {'latitude': '1.3208572', 'longitude': '103.84243190000007'},
                       'Toa Payoh Singapore': {'latitude': '1.3343035', 'longitude': '103.85632650000002'},
                       'MacPherson Road Singapore': {'latitude': '1.3311534', 'longitude': '103.87878460000002'},
                       'Geylang Singapore': {'latitude': '1.3200544', 'longitude': '103.89177459999996'},
                       'Joo Chiat Road Singapore': {'latitude': '1.3099091', 'longitude': '103.90202690000001'},
                       'Bedok Singapore': {'latitude': '1.3236038', 'longitude': '103.92734050000001'},
                       'Changi Road Singapore': {'latitude': '1.3183354', 'longitude': '103.90886380000006'},
                       'Tampines Singapore': {'latitude': '1.3495907', 'longitude': '103.9567879'},
                       'Punggol Singapore': {'latitude': '1.3984457', 'longitude': '103.9072046'},
                       'Ang Mo Kio Avenue 10 Singapore': {'latitude': '1.3669806', 'longitude': '103.85660860000007'},
                       'Upper Bukit Timah Road Singapore': {'latitude': '1.3576368', 'longitude': '103.76806080000006'},
                       'Boon Lay Way Singapore': {'latitude': '1.3443216', 'longitude': '103.72708920000002'},
                       'Choa Chu Kang Singapore': {'latitude': '1.3839803', 'longitude': '103.74696110000002'},
                       'Kranji Road Singapore': {'latitude': '1.4308475', 'longitude': '103.75577610000005'},
                       'Woodlands Singapore': {'latitude': '1.4381922', 'longitude': '103.78895970000008'},
                       'Upper Thomson Road Singapore': {'latitude': '1.3728883', 'longitude': '103.82851129999995'},
                       'Sembawang Singapore': {'latitude': '1.4491107', 'longitude': '103.81849540000007'},
                       'Yio Chu Kang Road Singapore': {'latitude': '1.391422', 'longitude': '103.8562167'},
                       'Marina Bay Sands Bayfront Avenue Singapore': {'latitude': '1.2845442', 'longitude': '103.85958979999998'},
                       'Paya Lebar Road Singapore': {'latitude': '1.3240203','longitude': '103.89081759999999'},
                       'Pasir Ris Singapore': {'latitude': '1.3720937', 'longitude': '103.94737280000004'},
                       'Dairy Farm Road Singapore': {'latitude': '1.3648296', 'longitude': '103.77386520000005'},
                       }

class CaptureGrobpay(CaptureBase):
    get_merchant_url = 'https://www.grab.com/sg/wp-admin/admin-ajax.php'
    HEADER = '''
            accept:application/json, text/javascript, */*; q=0.01
            accept-encoding:gzip, deflate, br
            accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            content-type:application/x-www-form-urlencoded; charset=UTF-8
            origin:https://www.grab.com
            x-requested-with:XMLHttpRequest
            User-Agent:{}
            '''
    TABLE_NAME_GROBPAY = 'grobpay_business_info'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureGrobpay, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))

    def __del__(self):
        super(CaptureGrobpay, self).__del__()

    def _rm_duplicate(self, scr_datas, match):
        goods = {}
        repeat_num = 0
        for data in scr_datas:
            if goods.get(':'.join([data[match[0]], data[match[1]]])):
                logger.debug('find repead data: {}'.format(data))
                logger.debug('save_data: {}'.format(goods.get(':'.join([data[match[0]], data[match[1]]]))))
                repeat_num += 1
            else:
                goods[':'.join([data[match[0]], data[match[1]]])] = data
        logger.info('repead data count: {}'.format(repeat_num))
        return [value for value in goods.itervalues()]

    def get_merchant_infos(self, location_information, area_name):

        location_information['action'] = 'search_grabpay_merchant'
        data = urllib.urlencode(location_information)
        infos = self.getHtml(self.get_merchant_url, self.header, data)
        marchants = json.loads(infos)
        for marchant in marchants:
            marchant['area_name'] = area_name
            marchant['name'] = marchant.get('name').replace('"', '')
            marchant['create_time'] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        logger.info('get_merchant_infos area_name {} get merchant {}'.format(area_name, len(marchants)))
        return marchants

    #
    # def get_location_info(self, location_name):
    #     # url_format = 'https://maps.googleapis.com/maps/api/place/textsearch/json?{}'
    #     #自动匹配
    #     # https://maps.googleapis.com/maps/api/place/autocomplete/json?input=Bugis+Junction&location=1.344436%2C103.842773&key=AIzaSyANWO+-+6gC75Bf_PPzBtPPuU2T5jUY7p51M&types=geocode
    #     url_format = 'https://maps.googleapis.com/maps/api/place/autocomplete/json?{}'
    #     locations = []
    #     pagetoken = ''
    #     while 1:
    #         # input = {'query': location_name+' in Singapore',
    #         #          'key': KEY,
    #         #          'pagetoken': pagetoken}
    #         input = {'input': location_name+' in Singapore',
    #                  'key': KEY,
    #                  'location': '1.344436,103.842773',
    #                  'types':'geocode'}
    #         header = '''
    #         accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
    #         accept-encoding:gzip, deflate, br
    #         accept-language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
    #         cache-control:max-age=0
    #         upgrade-insecure-requests:1
    #         user-agent:{}
    #         '''
    #         useragent = random.choice(UserAgents)
    #         header = self._getDict4str(header.format(useragent))
    #         str_input = urllib.urlencode(input)
    #         url =url_format.format(str_input)
    #         page_source = self.getHtml(url, header)
    #         page_source = json.loads(page_source)
    #         status = page_source['status']
    #         if status != u'OK':
    #             logger.error('location_name:{} error status {}'.format(location_name, status))
    #             logger.error('url {}'.format(url))
    #             break
    #         location_infos = page_source['results']
    #         for location_info in location_infos:
    #             input_info = []
    #             location = {}
    #             name = location_info['name'].strip()
    #             lat = location_info[u'geometry'][u'location'][u'lat']
    #             lng = location_info[u'geometry'][u'location'][u'lng']
    #             input_info.append(name.encode('utf-8'))
    #             location['latitude'] = str(lat)
    #             location['longitude'] = str(lng)
    #             input_info.append(location)
    #             locations.append(input_info)
    #         pagetoken = page_source.get('next_page_token','').strip()
    #
    #     return locations
#半自动
    def get_location_info(self, location_name):
        return [[location_name, Location2coordinate[location_name]]]
    def dealMerchant(self, location_name):
        try:
            merchant_info = []
            location_info = self.get_location_info(location_name)
            for info in location_info:
                merchant_info.extend(self.get_merchant_infos(info[1], info[0]))
            logger.info('all location_name{} get data: {}'.format(location_name, len(merchant_info)))
            return merchant_info
        except Exception, e:
            logger.error('dealMerchant location_name: {} error: {}.'.format(location_name, e))
            return []

    def dealMerchants(self,location_names):
        try:
            resultDatas = []
            for location_name in location_names:
                resultDatas.extend(self.dealMerchant(location_name))
            logger.info('all location_names get data: {}'.format(len(resultDatas)))
            resultDatas = self._rm_duplicate(resultDatas, ['name', 'address'])
            logger.info('After the data remove duplicates: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealMerchants get no resultDatas ')
            for resultData in resultDatas:
                resultData['address'] = resultData.get('address').encode('utf-8')
            format_select = 'SELECT ID FROM {} WHERE address="{{address}}" AND name="{{name}}"ORDER BY CREATE_TIME DESC'
            good_datas = resultDatas
            select_sql = format_select.format(self.TABLE_NAME_GROBPAY)
            table = self.TABLE_NAME_GROBPAY
            replace_insert_columns = ['area_name','address','dist','facebook','instagram','logo','to_lat','to_long','website','name','offer_page_url','create_time']
            select_columns = ['ID']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('dealMerchants error: {}'.format(e))
        finally:
            logger.info('dealMerchants end')
    def get_diff_nums(self, location_names):
        diff = []
        for location_name in location_names:
            sql_1 = 'select count(*) from {} where area_name="{}"'.format(self.TABLE_NAME_GROBPAY, location_name)
            sql_2 = 'select num from {} where area_name="{}"'.format('grobpay_num_info', location_name)
            result_1 = self.mysql.sql_query(sql_1)
            result_1 = result_1[0].get('count(*)', 0) if result_1 else 0
            result_2 = self.mysql.sql_query(sql_2)
            result_2 = result_2[0].get('num', 0) if result_2 else 0
            if result_1 != result_2:
                diff.append({'area_name': location_name,
                            'before_num': result_2,
                            'after_num': result_1,
                             'diff_count': result_1-result_2})
        logger.info('diff area: {}'.format(sorted(diff, key=lambda asd: (asd['diff_count'], asd['area_name'])\
                                                  , reverse=True)))

    def update_nums(self, location_names):
        info = []
        for location_name in location_names:
            sql_1 = 'select count(*) from {} where area_name="{}"'.format(self.TABLE_NAME_GROBPAY, location_name)
            result_1 = self.mysql.sql_query(sql_1)
            result_1 = result_1[0].get('count(*)', 0) if result_1 else 0
            info.append({'area_name': location_name,
                            'num': result_1})
        logger.info('info area: {}'.format(info))
        operate_type = 'replace'
        table = 'grobpay_num_info'
        replace_insert_columns = ['area_name','num']
        update_datas = info
        result_update = self.mysql.insert_batch(operate_type, table, replace_insert_columns, update_datas)
        logger.info('saveDatas result_update: {}'.format(result_update))

    def _saveDatas(self, good_datas, table, select_sql, replace_insert_columns, select_columns):
        try:
            result_insert, result_update = True, True
            if not good_datas:
                logger.error('saveDatas not get datas')
                return False
            (insert_datas, update_datas) = self._checkDatas(select_sql, good_datas, select_columns)
            if insert_datas:
                operate_type = 'insert'
                l = len(insert_datas)
                logger.info('len insert_datas: {}'.format(l))
                result_insert = self.mysql.insert_batch(operate_type, table, replace_insert_columns, insert_datas)
                logger.info('insert_datas: {}'.format(insert_datas))
                logger.info('saveDatas insert_datas: {}'.format(result_insert))
            if update_datas:
                # operate_type = 'replace'
                l = len(update_datas)
                logger.info('len update_datas: {}'.format(l))
                # replace_insert_columns.insert(0, 'ID')
                result_update = True
                # result_update = self.mysql.insert_batch(operate_type, table, replace_insert_columns, update_datas)
                # logger.info('saveDatas result_update: {}'.format(result_update))
            return result_insert and result_update
        except Exception, e:
            logger.error('saveDatas error: {}.'.format(e))
            return False
def main():
    startTime = datetime.now()
    objCaptureGrobpay = CaptureGrobpay(useragent)
    location_names = Location2coordinate.keys()
    objCaptureGrobpay.dealMerchants(location_names)
    objCaptureGrobpay.get_diff_nums(location_names)
    # objCaptureGrobpay.update_nums(location_names)
    # location_information = {'latitude':'1.3525845',
    #                         'longitude':'103.83521159999998'}
    # objCaptureGrobpay.get_merchant_infos(location_information, 'ffffffff')
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

