import datetime
from proto import tag_pb2
from tools.getMesInfo import MesData
from tools.globalconfig import Global
from tools.useKafka import pyKafka


# 处理kafka消费到的数据：protobuf反序列化，并处理成指定格式
def deal_with_consumer_data(msg, conn):
    gp = MesData(conn)

    target = tag_pb2.interface_param()
    target.ParseFromString(msg)
    dict_result = {}
    time = target.Param[0].time
    time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + " " + time[8:10] \
           + ":" + time[10:12] + ":" + time[12:14]
    dict_result['create_time'] = time

    for tar in target.Param:
        sql = "INSERT INTO test.ods_pspace_data_cxnf(tag, value, time, dw_create_time) " \
              "VALUES ('%s', '%s', '%s', '%s');" % (tar.name, tar.value, time, time)
        gp.db_rw(sql)
    print('kafka时间为%s的数据录入数据库' % time)
    # for tar in target.Param:
    #     tag = str(tar.name).replace('.', '')
    #     if tag not in dict_result.keys():
    #         dict_result[tag] = tar.value
    # return dict_result


global_config = Global()
gp_db = global_config.gp_db
gp_conn = gp_db.get_postgresql_conn()


# ai_data.cursor.execute(sql)
# ai_data.db_conn.commit()


ka = pyKafka('192.168.8.211:9092', 'opc-data-cxnf')
print(ka.get_all_topics())
ka.customConsumer(deal_with_consumer_data, gp_conn)
