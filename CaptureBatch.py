#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 20:37
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    :
# @File    : CaptureBatch.py
# @Software: PyCharm
# @Desc    :
from CrawlingProxy import UserAgents
from CaptureAlthea import CaptureAlthea
from CaptureAmazon import CaptureAmazon
from CaptureAngelflorist import CaptureAngelflorist
from CaptureAsos import CaptureAsos
from CaptureCreative import CaptureCreative
from CaptureEzbug import CaptureEzbug
from CaptureHervelvetvase import CaptureHervelvetvase
from CaptureHipvan import CaptureHipvan
from CaptureHonestbee import CaptureHonestbee
from CaptureHotels import CaptureHotels
from CaptureIshopchangi import CaptureIshopchangi
from CaptureLazada import CaptureLazada
from CaptureRedmart import CaptureRedmart
from CaptureShopee import CaptureShopee
from CaptureSingaporeair import CaptureSingaporeair
from CaptureTaobao import CaptureTaobao
from CaptureUniqlo import CaptureUniqlo
from CaptureVexicot import CaptureVexicot
from CaptureZalora import CaptureZalora
from logger import logger
from datetime import datetime
import random
import multiprocessing

#capture_type 1, home;2, goods;3, all
class InfoConfig(object):
    def __init__(self, company, capture_type):
        self.company = company
        self.capture_type = capture_type

nodes = {
    'Althea': InfoConfig(CaptureAlthea(random.choice(UserAgents)), 3),
    'Amazon': InfoConfig(CaptureAmazon(random.choice(UserAgents)), 3),
    'Angelflorist': InfoConfig(CaptureAngelflorist(random.choice(UserAgents)), 3),
    'Asos': InfoConfig(CaptureAsos(random.choice(UserAgents)), 3),
    'Creative': InfoConfig(CaptureCreative(random.choice(UserAgents)), 3),
    'Ezbug': InfoConfig(CaptureEzbug(random.choice(UserAgents)), 3),
    'Hervelvetvase': InfoConfig(CaptureHervelvetvase(random.choice(UserAgents)), 3),
    'Hipvan': InfoConfig(CaptureHipvan(random.choice(UserAgents)), 3),
    'Honestbee': InfoConfig(CaptureHonestbee(random.choice(UserAgents)), 3),
    'Hotels': InfoConfig(CaptureHotels(random.choice(UserAgents)), 3),
    'Ishopchangi': InfoConfig(CaptureIshopchangi(random.choice(UserAgents)), 3),
    'Lazada': InfoConfig(CaptureLazada(random.choice(UserAgents)), 3),
    'Redmart': InfoConfig(CaptureRedmart(random.choice(UserAgents)), 3),
    'Shopee': InfoConfig(CaptureShopee(random.choice(UserAgents)), 3),
    'Singaporeair': InfoConfig(CaptureSingaporeair(random.choice(UserAgents)), 3),
    'Taobao': InfoConfig(CaptureTaobao(random.choice(UserAgents)), 3),
    'Uniqlo': InfoConfig(CaptureUniqlo(random.choice(UserAgents)), 3),
    'Vexicot': InfoConfig(CaptureVexicot(random.choice(UserAgents)), 3),
    'Zalora': InfoConfig(CaptureZalora(random.choice(UserAgents)), 3),
}

lock = multiprocessing.Lock()
result_end = []

def exec_task(company_name, input_type=None):
    try:
        logger.info('exec_task {} begin'.format(company_name))
        capture_type = input_type if input_type else nodes.get(company_name).capture_type
        capture_obj = nodes.get(company_name).company
        if capture_type == 1:
            ret = capture_obj.dealHomeGoods()
        elif capture_type == 2:
            ret = capture_obj.dealCategorys()
        else:
            ret = capture_obj.dealHomeGoods() and capture_obj.dealCategorys()
        logger.info('exec_task {} end'.format(company_name))
        return (company_name, ret)
    except Exception, ex:
        logger.error('exec_task {} error: {}'.format(company_name, ex))
        return (company_name, False)
def save_result(result):
    lock.acquire()
    logger.info('save_result: {}'.format(result))
    result_end.append(result)
    lock.release()

def runner_capture():
    try:
        logger.info('runner_capture is begin.')
        pool = multiprocessing.Pool(processes=5)
        for key in nodes.iterkeys():
            pool.apply_async(exec_task, (key,), callback=save_result)  # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
        pool.close()
        pool.join()  # 调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
        logger.info('runner_capture all end.')
        logger.info('result:{}'.format(sorted(result_end, key=lambda asd: (asd[1], asd[0]))))
    except Exception, e:
        logger.error('runner_capture error: {}'.format(e))

def runner_capture_single(company_name, input_type=None):
    try:
        logger.info('Capture is begin.')
        result = exec_task(company_name, input_type)
        logger.info('Capture is end.')
        logger.info('result:{}'.format(result))
    except Exception, e:
        logger.error('runner_capture_single error: {}'.format(e))

if __name__ == '__main__':
    startTime = datetime.now()
    runner_capture()
    # runner_capture_single('Ezbug')
    endTime = datetime.now()
    logger.info('seconds {}'.format((endTime - startTime).seconds))