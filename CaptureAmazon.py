#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureAmazon.py
# @Software: PyCharm
# @Desc    :
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

class CaptureAmazon(CaptureBase):
    department_url = 'https://www.amazon.com/gp/goldbox/'
    home_url = 'https://www.amazon.com/'
    white_department = [ u'Arts, Crafts &amp; Sewing', u'Automotive & Motorcycle', u'Baby', u'Baby Clothing & Accessories', u'Beauty',\
                         u'Boys’ Fashion', u'Camera & Photo', u'Cell Phones & Accessories', u'Computers & Accessories', u'Costumes & Accessories',\
                         u'DIY & Tools', u'Electronics', u'Furniture', u'Girls’ Fashion', u'Grocery',\
                         u'Kitchen', u'Luggage Travel Gear', 'Men\'s Shoes', 'Men\'s Watches', u'Men’s Clothing', u'Men’s Fashion',\
                         u'Office Electronics & Supplies', u'Patio, Lawn & Garden', u'Power & Hand Tools', u'Sports & Outdoors', u'Toys &amp; Games', 'Women\'s Shoes', \
                         'Women\'s Watches', u'Women’s Clothing', u'Women’s Fashion', u'Women’s Jewelry']
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

    HEADER_VARIFY = '''
            Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
            Accept-Encoding:gzip, deflate, br
            Accept-Language:zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
            Connection:keep-alive
            Host:images-na.ssl-images-amazon.com
            User-Agent:{}
            '''
    Channel = 'amazon'
    def __init__(self, user_agent, proxy_ip=None):
        super(CaptureAmazon, self).__init__(user_agent, proxy_ip)
        self.header = self._getDict4str(self.HEADER.format(self.user_agent))
        self.header_varify = self._getDict4str(self.HEADER_VARIFY.format(self.user_agent))

    def __del__(self):
        super(CaptureAmazon, self).__del__()

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
            html = self.getHtml(categoryurl, self.header)
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
                resultData['Currency'.lower()] = sourceData.get('currencyCode') or 'USD'
                # 原数据float
                resultData['AMOUNT'.lower()] = sourceData.get('minDealPrice') or 0
                resultData['HIGH_AMOUNT'.lower()] = sourceData.get('maxDealPrice') or 0
                # if resultData['HIGH_AMOUNT'.lower()] > resultData['AMOUNT'.lower()]:
                #     continue
                resultData['Before_AMOUNT'.lower()] = sourceData.get('minListPrice') or 0
                resultData['Before_HIGH_AMOUNT'.lower()] = sourceData.get('maxListPrice') or 0
                # 原数据bool
                resultData['COMMEND_FLAG'.lower()] = '1' if sourceData.get('isFeatured') else '0'
                resultData['CREATE_TIME'.lower()] = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                # 原数据int
                resultData['DISPLAY_COUNT'.lower()] = sourceData.get('totalReviews') or 0

                resultData['RESERVE'.lower()] = sourceData.get('dealID').strip()
                # resultData = [time.strftime('%Y-%m-%d',time.localtime(time.time()))]  #日期
                resultDatas.append(resultData)
            except Exception, e:
                logger.error('format_data error: {}.'.format(e))
                logger.error('resultData: {}'.format(resultData))
        if Not_BEST_DEAL == len(sourceDatas):
            logger.error('This time all the goods are not BEST_DEAL')
        return resultDatas
    '''
    function: 获取商品信息 post请求
    @source_datas: 原始数据
    @return: dict_res or None
    '''
    def getCategoryGoods(self, amznMerchantID, sessionId, newDealIDs, refRID, widgetID, slotNames):
        try:
            slotName = random.choice(slotNames)
            time_now = int(time.time() * 1000)
            url = 'https://www.amazon.com/xa/dealcontent/v2/GetDeals?nocache={}'.format(time_now)
            data = '{"requestMetadata":{"marketplaceID":"' + amznMerchantID + '","clientID":"goldbox_mobile_pc","sessionID":"' + sessionId + '"},"dealTargets":' + str(newDealIDs).replace("'", '"') + \
                   ',"responseSize":"ALL","itemResponseSize":"DEFAULT_WITH_PREEMPTIVE_LEAKING","widgetContext":{"pageType":"GoldBox","subPageType":"main","deviceType":"pc","refRID":"' + refRID + '","widgetID":"' + widgetID + '","slotName":"' + slotName + '"}}'
            response = self.getHtml(url, self.header, data)
            dict_res = json.loads(response)
            return dict_res
        except Exception, e:
            logger.error('getCategoryGoods:{} error: {}.'.format(newDealIDs, e))
            return None

  #通过url获取所需信息 包括 price
    def getGoodsInfos(self, goodsurl):
        try:
            html = self.get_html(goodsurl, header=self.header)
            soup = BeautifulSoup(html, 'lxml')
            try:
                price = soup.findAll(('div', {'class': "a-section a-spacing-none snsPriceBlock"}))[0].findAll('span', {'class': "a-size-large a-color-price"})[0].getText().strip('\n').strip()
                return price
            except Exception, e:
                if e.message != 'list index out of range':
                    return
                else:
                    print 'retry get price'
            list_price = soup.findAll(('tr', {'id': "priceblock_ourprice_row"}))
            for list in list_price:
                try:
                    price = list.findAll('td', {'class': "a-span12"})[0].findAll('span',{'class': "a-size-medium a-color-price"})[0].getText().strip('\n').strip()
                    break
                except Exception, e:
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
            html = self.getHtml(self.department_url, self.header)
            pattern = re.compile(r'{\s*\"nodeId\" : \"\d+\",\s*\"category\" : \"[-,\';&\w\s\’]+\"\s*}', re.M)
            departments = pattern.findall(html)
            return [eval(i) for i in departments if eval(i)['category'].decode("utf-8") in self.white_department]
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
            formatUrl = 'https://www.amazon.com/gp/goldbox/?gb_f_GB-SUPPLE=enforcedCategories:{},sortOrder:BY_SCORE,dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL'
            departments = self.__get_department()
            logger.debug('departments: {}'.format(departments))
            Urls = [[department.get('category'), formatUrl.format(department.get('nodeId'))] for department in departments]
            resultDatas = []
            for url in Urls:
                resultData = self.dealCategory(url[0], url[1])
                resultDatas.extend(resultData)
            logger.info('all categorys get data: {}'.format(len(resultDatas)))
            resultDatas = self._rm_duplicate(resultDatas, 'PRODUCT_ID'.lower())
            logger.info('After the data remove duplicates: {}'.format(len(resultDatas)))
            if not resultDatas:
                raise ValueError('dealCategorys get no resultDatas ')

            format_select =  'SELECT ID,STATUS FROM {} WHERE CHANNEL="{{channel}}" AND PRODUCT_ID="{{product_id}}" ORDER BY CREATE_TIME DESC'
            good_datas = resultDatas
            select_sql = format_select.format(self.TABLE_NAME_PRODUCT)
            table = self.TABLE_NAME_PRODUCT
            replace_insert_columns = ['CHANNEL', 'KIND', 'SITE', 'PRODUCT_ID', 'LINK', 'MAIN_IMAGE', 'NAME', 'DESCRIPTION', 'Currency',\
                       'AMOUNT', 'HIGH_AMOUNT', 'Before_AMOUNT', 'Before_HIGH_AMOUNT', 'COMMEND_FLAG', 'CREATE_TIME', 'DISPLAY_COUNT', 'RESERVE', 'STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
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

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def getHtmlselenium(self, url):
        driver = None
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
                driver.get(url)
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
    function: 获取并存储首页滚动栏的商品信息
    @return: True or raise
    '''
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def dealHomeGoods(self):
        result_datas = []
        try:
            page_source = self.getHtmlselenium(self.home_url)
            soup = BeautifulSoup(page_source, 'lxml')
            pattern = re.compile(r'<li class="a-carousel-card" role="listitem" aria-setsize="\d+" aria-posinset=', re.M)
            aria_set_size_str = pattern.findall(page_source)[0]
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
                    # logger.error('get eLement error:{}'.format(e))
                    # logger.error('goodData: {}'.format(good_data))
                    continue
            if len(result_datas) == 0:
                logger.error('page_source: {}'.format(page_source))
                raise ValueError('not get valid data')

            format_select = 'SELECT ID,STATUS FROM {} WHERE CHANNEL="{{channel}}" and LINK="{{link}}" ORDER BY CREATE_TIME DESC'
            good_datas = result_datas
            select_sql = format_select.format(self.TABLE_NAME_BANNER)
            table = self.TABLE_NAME_BANNER
            replace_insert_columns = ['CHANNEL', 'LINK', 'TITLE', 'MAIN_IMAGE', 'CREATE_TIME', 'STATUS']
            select_columns = ['ID', 'STATUS']
            return self._saveDatas(good_datas, table, select_sql, replace_insert_columns, select_columns)
        except Exception, e:
            logger.error('Get home goods infos error:{},retry it'.format(e))
            raise

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
    # # 查询并入库首页推荐商品信息
    objCaptureAmazon.dealHomeGoods()
    # 查询dealID的商品信息
    # objCaptureAmazon.getCategoryGoods('ATVPDKIKX0DER','134-6378449-3791711', [{"dealID": "862d423c"}], 'HKWF4KQNXA8YEVZHWF5Y', '30c09623-33cf-4469-be4c-3e8293ae0ee9', ['slot-3', 'slot-4', 'slot-6'])
    # 查询并入库特别类别的商品信息
    # objCaptureAmazon.dealCategory('Kitchen', 'https://www.amazon.com/gp/goldbox/?gb_f_GB-SUPPLE=enforcedCategories:284507,sortOrder:BY_SCORE,dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL')
    # 获取具体网址的html信息
    # objCaptureAmazon.getHtml('https://images-na.ssl-images-amazon.com/captcha/usvmgloq/Captcha_jxqunydgna.jpg')
    # objCaptureAmazon.getHtml('https://www.amazon.com/gp/goldbox/?gb_f_GB-SUPPLE=enforcedCategories:2625373011,sortOrder:BY_SCORE,dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL', objCaptureAmazon.header)
    endTime = datetime.now()
    print 'seconds', (endTime - startTime).seconds
if __name__ == '__main__':
    main()

