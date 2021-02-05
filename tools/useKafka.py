# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang'


import time
from proto import tag_pb2
from datetime import datetime
from pykafka import KafkaClient


# 处理kafka消费到的数据：protobuf反序列化，并处理成指定格式
def deal_with_consumer_data(msg):
    target = tag_pb2.interface_param()
    target.ParseFromString(msg)
    dict_result = {}
    time = target.Param[0].time
    time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + " " + time[8:10] \
           + ":" + time[10:12] + ":" + time[12:14]
    dict_result['create_time'] = time
    for tar in target.Param:
        tag = str(tar.name).replace('.', '')
        if tag not in dict_result.keys():
            dict_result[tag] = tar.value
    return dict_result


# python操作kafka
class pyKafka:
    def __init__(self, hosts, topic_name):
        self.client = KafkaClient(hosts=hosts, broker_version='0.10.0')
        # 选择一个topic,不存在则新建
        self.topic = self.client.topics[bytes(topic_name, encoding='utf-8')]
        self.producer = self.topic.get_sync_producer()  # 同步生产
        # self.producer = self.topic.get_producer()  #异步生产

    # 查看所有topic
    def get_all_topics(self):
        return self.client.topics

    # 生产者
    def product(self, message):
        try:
            key = datetime.now().strftime("%Y%m%d%H%M%S")
            print("生产开始")
            self.producer.produce(message, key.encode())
            print("生产结束")
        except Exception as e:
            print(e)

    # 生产者关闭
    def product_stop(self):
        self.producer.stop()

    # 消费者
    def consumer(self):
        try:
            topic = self.topic
            # offsets_earliest = topic.earliest_available_offsets()  # 最早可用偏移量
            offsets_latest = topic.latest_available_offsets()  # 最近可以偏移量
            partitions = topic.partitions
            consumer = topic.get_simple_consumer()
            for key in partitions:
                aa = offsets_latest[key].offset[0] - 1
                consumer.reset_offsets([(partitions[key], aa)])  # 设置offset
            print("消费开始")
            while True:
                message = consumer.consume()
                if message is not None:
                    value = message.value
                    deal_with_consumer_data(value)
        except Exception as e:
            print(e)

    # 消费者
    def customConsumer(self, format_value, conn):
        try:
            topic = self.topic

            # offsets_latest = topic.latest_available_offsets()  # 最近可以偏移量
            # partitions = topic.partitions
            # consumer = topic.get_simple_consumer()
            # for key in partitions:
            #     aa = offsets_latest[key].offset[0] - 1
            #     consumer.reset_offsets([(partitions[key], aa)])  # 设置offset

            consumer = topic.get_simple_consumer()
            print("消费开始")

            while True:
                message = consumer.consume()
                if message is not None:
                    value = message.value
                    format_value(value, conn)
        except Exception as e:
            print(e)
