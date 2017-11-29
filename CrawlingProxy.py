#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureProxy.py
# @Software: PyCharm
# @Desc    :
import urllib2
import threadpool
import random
from bs4 import BeautifulSoup
from logger import logger
import os

USER_AGENT = '''
    Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36
    Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11
    Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16
    Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36
    Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36

    Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2
    Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50

    Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0
    Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10
    Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1

    Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0
    Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)

    Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)
    Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)
    '''
def getList4str(strsource):
    outlist = []
    lists = strsource.split('\n')
    for list in lists:
        list = list.strip()
        if list:
            outlist.append(list)
    return outlist

UserAgents = getList4str(USER_AGENT)
useragent = random.choice(UserAgents)

class CrawlingFinish(SyntaxWarning):
    pass


class CrawlingProxy(object):
    def __init__(self):
        '''
        Constructor
        '''
        self.file_proxy = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'proxy.txt')
        self.proxy_num = 2
        self.pool_num = 1
        self.result = []

    def generateProxy(self, page=50):
        with open(self.file_proxy, 'w') as of:
            page = min(page, 50)
            page = max(page, 2)
            for page in range(1, page):
                try:
                    url = 'http://www.xicidaili.com/nn/{}'.format(page)
                    user_agent = "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"
                    request = urllib2.Request(url)
                    request.add_header("User-Agent", user_agent)
                    content = urllib2.urlopen(request, timeout=10)
                    soup = BeautifulSoup(content, 'lxml')
                    trs = soup.find('table', {"id": "ip_list"}).findAll('tr')
                    for tr in trs[1:]:
                        tds = tr.findAll('td')
                        ip = tds[1].text.strip()
                        port = tds[2].text.strip()
                        protocol = tds[5].text.strip()
                        if protocol == 'HTTP' or protocol == 'HTTPS':
                            of.write('{}={}:{}s\n'.format(protocol, ip, port))
                            logger.debug('{}://{}:{}'.format(protocol, ip, port))
                except Exception,e:
                    logger.error('url:{},e:{}'.format(url, e))
                    continue

    def getValidProxy(self, line):
        url = "https://www.amazon.com"
        protocol, proxy = line.split('=')
        try:
            proxy_host = protocol.lower()+'://'+proxy
            proxy_tmp = {protocol.lower(): proxy_host}
            proxy = urllib2.ProxyHandler(proxy_tmp)
            opener = urllib2.build_opener(proxy)
            res = opener.open(url).read()
            logger.debug('pass {},res {}'.format(proxy_tmp, res))
            return proxy_tmp
        except Exception,e:
            logger.debug('error {},e {}'.format(proxy_tmp, e))
            return None
    def saveResult(self, requests, result):
        logger.debug("the result is {} {}".format(requests.requestID,result))
        if result:
            self.result.append(result)
        if len(self.result) == self.proxy_num:
            logger.debug('Have finish {} proxy'.format(self.proxy_num))
            raise CrawlingFinish()
    def getValidProxys(self):
        try:
            inFile = open(self.file_proxy, 'r')
            lines = []
            while True:
                line = inFile.readline().strip()
                if len(line) == 0:
                    break
                lines.append(line)
            if not lines:
                return
            num_pool = min(len(lines),self.pool_num)
            pool = threadpool.ThreadPool(num_pool)
            requests = threadpool.makeRequests(self.getValidProxy, lines, self.saveResult)
            [pool.putRequest(req) for req in requests]
            pool.wait()
            return self.result
        except CrawlingFinish:
            return self.result
        except Exception,e:
            logger.error('e {}'.format(e))
            return None
    def getRandomProxy(self):
        proxys = self.getValidProxys()
        if proxys:
            proxy = random.choice(proxys)
            logger.info('get avaliable proxy: {}'.format(proxy))
            return proxy
        else:
            logger.error('no avaliable proxy')




# def main():
#     objCrawlingProxy = CrawlingProxy()
#     objCrawlingProxy.generateProxy()
#
def main2():
    objCrawlingProxy = CrawlingProxy()
    objCrawlingProxy.generateProxy()
    print objCrawlingProxy.getValidProxys()
    result = objCrawlingProxy.getRandomProxy()
    print result
if __name__ == '__main__':
    main2()