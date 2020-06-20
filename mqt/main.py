# /usr/bin/env python3
# coding=utf-8
import configparser
import json
import logging
import os
import sys
import time
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from time import sleep
from threading import Thread

import pika

cf = configparser.ConfigParser()
current_dir = os.curdir
cf.read(current_dir + "/config.ini")

logger = logging.getLogger("mqlog")

send_host = cf.get("sendmq", "host")
send_username = cf.get("sendmq", "username")
send_password = cf.get("sendmq", "password")
send_port = cf.get("sendmq", "port")
send_exchange = cf.get("sendmq", "exchange")
send_que = cf.get("sendmq", "queue")

rec_host = cf.get("recmq", "host")
rec_username = cf.get("recmq", "username")
rec_password = cf.get("recmq", "password")
rec_port = cf.get("recmq", "port")
rec_que = cf.get("recmq", "queue")

global send_connection
global send_channel

def connect_rabbitmq(host, username, password, port):
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host,
        credentials=credentials,
        port=port,
        socket_timeout=5,
        heartbeat=10))
    return connection


def get_send_connect():
    return connect_rabbitmq(send_host, send_username, send_password, send_port)


def get_rec_connect():
    return connect_rabbitmq(rec_host, rec_username, rec_password, rec_port)





def log_init():
    LOG_LEVEL_NAME = cf.get("log", "level")
    LOG_LEVEL = logging._nameToLevel.get(cf.get("log", "level"))  # 日志级别 debug,info
    LOG_PATH = cf.get("log", "path")
    LOG_FILE = LOG_PATH + '/mqt.log'
    DATE_FMT = '%Y-%m-%d %H:%M:%S'
    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)
    fmt = '%(asctime)s.%(msecs)03d  %(filename)s:%(lineno)d  %(levelname)s  %(message)s'  # type: str
    handlers = [
        # 按日志大小分割文件 #from logging.handlers import RotatingFileHandler ，保留5个文件，每个最大10M
        RotatingFileHandler(LOG_FILE, mode='a', maxBytes=10 * 1024 * 1024 * 1024, backupCount=20, encoding='utf-8',
                            delay=False),
        # 按天分割文件,delay表示是否延迟写入文件
        # TimedRotatingFileHandler(log_file, when='D', interval=1,backupCount=5,encoding='UTF-8', delay=True),
        StreamHandler()
    ]
    logging.basicConfig(level=LOG_LEVEL, handlers=handlers, format=fmt, datefmt=DATE_FMT)
    logger.info("日志路径为：" + LOG_PATH + ",日志级别：" + LOG_LEVEL_NAME)


def close(connection ,channel):
    try:
        if not connection:
            connection.close()
        if not channel:
            channel.close()
    except:
        logger.error(sys.exc_info())


# 0-未知 1-移动GSM 2-联通GSM  3-电信CDMA 4-移动4G  5-联通4G 6-电信4G
def get_net(rec_json_message):
    net = 0
    imei = rec_json_message.get("imei", "")
    mnc = rec_json_message.get("mnc", "")
    if mnc == '中国移动':
        if not imei:
            net = 4
        else:
            net = 1
    elif mnc == '中国联通':
        if not imei:
            net = 5
        else:
            net = 2
    elif mnc == '中国电信':
        if not imei:
            net = 6
        else:
            net = 3
    else:
        net = 0
    return net


def time_tostamp(dt):  # 传入单个时间比如'2019-8-01 00:00:00'，类型为str
    # 转为时间数组
    timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
    # 转为时间戳
    timeStamp = int(time.mktime(timeArray))
    return timeStamp


def get_send_message(rec_json_message):
    send_message_tem = '{"devid":"%s","subid":00000,"devtype":"00000","msgtype":"IMSI_REPORT","data":{"imsi":"%s","imei":"%s","time":%d,"net":%d}}'
    imei = rec_json_message.get("imei", "")
    net = get_net(rec_json_message)
    dt = rec_json_message.get("collecttime", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    send_message = send_message_tem % (
        rec_json_message.get("deviceid", "00000"), rec_json_message.get("imsi", "000000000000000"), imei,
        time_tostamp(dt),
        net)
    return send_message


def send_mq(channel, message):
    try:
        logger.info("开始向mq服务器:" + send_host + ":" + str(send_port) + "，消息队列：" + send_que + "发送消息：" + message)
        channel.basic_publish(exchange=send_exchange, routing_key=send_que, body=message)
    except Exception as e:
        logger.error(repr(e))
        send_connection.close()
        channel.close()


def callback(channel, method, properties, body):
    try:
        #logger.info("======="+str(channel.is_open))
        rec_message = body.decode('utf-8')
        logger.info("接收到服务端:" + rec_host + ":" + str(rec_port) + "，监听队列：" + rec_que + "的消息" + rec_message)
        rec_json_message = json.loads(rec_message)
        send_message = get_send_message(rec_json_message)
        send_mq(send_channel, send_message)
    except Exception as e:
        channel.close()
        logger.error(repr(e))



def consumer(connection,channel):
    try:
        #python2
        #channel.basic_consume(queue=rec_que, on_message_callback=callback, auto_ack=True)
        #python3
        channel.basic_consume(callback, queue=rec_que, no_ack=True)
        channel.start_consuming()
        logger.debug("=================消费端开始消费========================")
    except Exception as e:
        logger.error("=================消费端出错consumer,消费端开始关闭联接========================")
        logger.error(repr(e))
        close(connection,channel)


def do_consumer():
    try:
        global send_connection
        global send_channel
        send_connection = get_send_connect()
        rec_connection = get_rec_connect()
        rec_channel = rec_connection.channel()
        send_channel=send_connection.channel()
        logger.debug("===================消费端连接成功===================")
        consumer(rec_connection,rec_channel)

    except Exception as e:
        logger.debug("===================do_consumer 消费队列或连接出错联接===================")
        logger.error(repr(e))


# 打印时间函数
def reConnect():
    while 1:
        try:
            rec = get_rec_connect()
            sleepCon()
        except Exception as e:
            logger.error("服务器断开。。。")
            sleepCon()
            reConnect()

def sleepCon():
    sleep(30)

if __name__ == '__main__':
    log_init()
    t = Thread(target=reConnect)
    t.start()
    while 1:
        do_consumer()
        sleep(1)

