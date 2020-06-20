# coding:utf-8

#########################################################################################################
# File:     ZRTZ_QUEUE_EFENCE_DEVICE_DATA.py
#
# Purpose:  电子围栏直接向rabbitmq数据队列发送消息
#
# Owner:    xiangying@whzrtz.com
#
# Location: ../test/tools
#
# Release/Update information:
# - 06/05/2019, xiangying - First release
#
#########################################################################################################

import pika
from time import sleep, time
from random import randrange
import random
from re import sub as sys_sub, match


def get_net(imsi):
    """ 解析网络制式 """
    if match(r'^4600[02478].*', imsi):
        return 1
    elif match(r'^4600[169].*', imsi):
        return 2
    else:
        return 0


while 1:
    try:
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='192.168.2.254',
            credentials=credentials,
            port=5672))
        channel = connection.channel()
        break
    except Exception as e:
        print(e)
        sleep(1)
        continue

# channel.queue_declare(queue='ZRTZ_QUEUE_EFENCE_CTRL_w1-d105')
i = 1
while 1:
    timestamp = int(time())
    imsi = randrange(460000000000000, 460099999999999)
    list1 = ['中国移动', '中国联通']
    mnc = random.choice(list1)

    # ZRT_LTE IMSI 随机生成imsi
    # message = '{"devid":"w1-d075","subid":257,"devtype":"ZRT_LTE","msgtype":"IMSI_REPORT",' \
    #           '"data":{"imsi":%s,"imei":"","time":%s,"net":%d}}' % (timestamp, get_net(imsi), imsi)

    message = '{"collecttime":%s,"deviceid":00000,"ext":0,"imei":"","imsi":"%s","mnc":"%s","ownarea":"河北 唐山","seqnum":0,"teleBrand":"","teleSevenNum":"1513151"}'% (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), imsi, mnc)

    # ZRT_LTE IMSI 生成固定的imsi
    #message = '{"devid":"w1-d987","subid":257,"devtype":"ZRT_LTE","msgtype":"IMSI_REPORT",' \
    #          '"data":{"imsi":"460061161572173","imei":"","time":%s,"net":4}}' % timestamp

    # zrt 2G IMSI
    # message = "{'msgtype': 'IMSI_REPORT', 'data': {'time': %s, 'net': 1, " \
    #           "'imei': '867246029542420', 'buffer_id': 0, 'imsi': '460037599917137'}, " \
    #           "'subtype': 'gsm', 'fetch': 'False', 'devtype': 'PORTABLE', 'devid': 'PE0005', " \
    #           "'subid': 'gsm'}" % timestamp

    # zrt 2G quhao
    # message = "{'data':{'time':%s,'fetch_type':0,'net':%d,'imsi':%s,'imei':'','buffer_id':128}," \
    #           "'msgtype':'IMSI_REPORT','subid':'gsm','devid':'PE0108','subtype':'gsm','fetch':'True'," \
    #           "'devtype':'QUHAO'}" % (timestamp, get_net(imsi), imsi)

    # zrt 2G STATUS
    # message = "{'subid': 'gsm', 'msgtype': 'STATUS', 'devtype': 'PORTABLE', 'devid': 'PE0005', " \
    #           "'subtype': 'gsm', 'data': {'time': %s, 'channel_0': 2, 'channel_1': 2}, " \
    #           "'fetch': 'False'}" % timestamp

    channel.basic_publish(exchange='ZRTZ_EXCHANGE_EFENCE',
                          routing_key='ZRTZ_QUEUE_EFENCE_DEVICE_DATA',
                          # routing_key='EFENCE_QUHAO_DATA',
                          body=message)

    print('{}:{}'.format(i, message))
    sleep(1)
    i += 1
# connection.close()
