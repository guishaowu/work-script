#!/usr/bin/env python
import pika
import conf
import json
import time
import datetime
from oslo_utils import timeutils
from futurist import ProcessPoolExecutor
import uuid
import random
import copy
import logging
import sys



def get_logger():
    logger_name = 'log'
    logger_file = 'log'
    log = logging.getLogger(logger_name)
    log.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        '%(asctime)s %(lineno)d %(funcName)s %(message)s',
        '%Y-%m-%d %H:%M:%S')
    f_hdr = logging.FileHandler(logger_file)
    f_hdr.setFormatter(fmt)
    f_hdr.setLevel(logging.DEBUG)
    log.addHandler(f_hdr)

    return log

LOG = get_logger()

prg_message = { "event_type": "protectiongroup.capability", "timestamp": "2017-03-29 07:24:06.636368", 
                "payload": {"capability": [
                           {"id":"1d2944f9-f8e9-4b2b-8df1-465f759a6339", "size": 50}
                           ]},
                "priority": "INFO"}

def generate_message_for_prg(num):
    message = copy.deepcopy(prg_message)
    for i in xrange(num):
        message['payload']['capability'].append({'id': str(uuid.uuid4()), 'size': random.randint(1,100)})
    return message


lb_message = {"event_type": "lbaasv2.lb_monitor", "timestamp": "2017-03-29 07:24:06.636368", 
              "payload": {
                         }, 
              "priority": "INFO"}

def generate_message_for_lb(num):
    message = copy.deepcopy(lb_message)
    for i in xrange(num):
        message['payload'][str(uuid.uuid4())] = {"connections": random.randint(1,15), "inpps": random.randint(1,15), "req_rate": random.randint(1,15), "tenant_id": "9eed8b80f61a414885580382f926050d"}
    return message

network_message = {
    "event_type": "l3.traffic",
    "timestamp": "2016-09-05 11:07:58.430672",
    "priority": "INFO",
    "payload": {
        "router_id": "a22b1c0b-b7b2-452d-ac77-49525c2af3d0",
        "o_bytes": 7224,
        "i_bytes": 109481,
        "host": "openstack",
        "tenant_id": "fake-tenant-id-1",
        "time": "2016-03-23 07:09:54.924059",
        "start_time": "2016-03-23 07:09:44.924059",
        "fips_traffic": []},
}

def generate_message_for_network(num):
    net_message = copy.deepcopy(network_message)
    for i in xrange(num):
        floating_ip_info = { 'o_bytes': random.randint(10,50), 'tenant_id': '715a85ddfa3840c48196c3458981ddcd', 'i_bytes': random.randint(10,300), 'floatingip_id':  str(uuid.uuid4())}
        net_message['payload']['fips_traffic'].append(floating_ip_info)
        net_message['payload']['router_id'] = str(uuid.uuid4())
    return net_message

snapshot_message = {"event_type":"snapshot_chain.capability","timestamp": "2017-03-29 07:23:03.234234",
                    "priority": "INFO",
                    "payload" : { "snapshot_chains":[] }}

def generate_message_for_snapshot(num):
    snap_message = copy.deepcopy(snapshot_message)
    for i in xrange(num):
        snap_message['payload']['snapshot_chains'].append({'id': str(uuid.uuid4()),'size': random.randint(1,100)})
    return snap_message

trove_message = {"event_type":"trove.instance.metrics.mysql","timestamp":"2017-03-29 07:24:06.653533",
                 "priority": "INFO",
                 "payload": {"payload": { "slow_queries": 100, "connections": 100, "threads_connected": 100, "com_select": 100, "com_insert": 100, "com_update": 100,
                                       "com_delete": 100, "qps": 100, "tps": 100, "innodb_rows_deleted": 100, "innodb_rows_inserted": 100, "innodb_rows_read": 100,
                                       "innodb_rows_updated": 100, "innodb_data_read": 100, "innodb_data_reads": 100, "innodb_data_writes": 100, "innodb_data_written": 100,
                                       "innodb_log_writes": 100, "innodb_log_write_requests": 100, "innodb_os_log_fsyncs": 100, "innodb_buffer_read_hit_ratio": 100,
                                       "innodb_buffer_usage_ratio": 100, "innodb_buffer_dirty_ratio": 100, "cpu_percent": 100, "memory_percent": 100, "disk_usage": 100,
                                       "read_count_ps": 100, "write_count_ps": 100, "bytes_sent_ps": 100, "bytes_received_ps": 100},
                             "instance_id": "1d2944f9-f8e9-4b2b-8df1-465f759a6339", "project_id": "1d2944f9-f8e9-4b2b-8df1-465f759a6339" 
                           }}

def generate_message_for_trove(num):
    message_trove = copy.deepcopy(trove_message)
    for i in xrange(num):
        message_trove['payload']['instance_id'] = str(uuid.uuid4()) 
    return message_trove 

MESSAGE_METHOD = {'lb': generate_message_for_lb,
                  'prg': generate_message_for_prg,
                  'network': generate_message_for_network,
                  'snapshot': generate_message_for_snapshot,
                  'trove': generate_message_for_trove}

def generate_message(message_type, num):
    func = MESSAGE_METHOD.get(message_type, None)
    return func(num)
#    if message_type == 'lb':
#        return generate_message_for_lb(num)
#    if message_type == 'prg':
#        return generate_message_for_prg(num)
#    if message_type == 'network':
#        return generate_message_for_network(num)
#    if message_type == 'snapshot':
#        return generate_message_for_snapshot(num)
#    if message_type == 'trove':
#        return generate_message_for_trove(num)
#    else:
#        return None

def get_connection():
    properties = pika.BasicProperties()
    properties.content_type = "application/json"
    connection = pika.BlockingConnection(pika.URLParameters(conf.RABBIT_URL))
    return connection

def convert_message_time(message, message_time):
    message['timestamp'] = str(message_time)
    if message['event_type'] == 'l3.traffic':
        message['payload']['time'] = str(message_time)
        message['payload']['start_time'] = str(message_time - datetime.timedelta(seconds=60))

def send_message(messages):
    properties = pika.BasicProperties()
    properties.content_type = "application/json"
    connection = pika.BlockingConnection(pika.URLParameters(conf.RABBIT_URL))
    channel = connection.channel()
    sw = timeutils.StopWatch()
    start = datetime.datetime.now() - datetime.timedelta(seconds=60 * 5)
    while True:
        sw.start()
        message_time = start + datetime.timedelta(seconds=60)
        for message in messages:
            convert_message_time(message, message_time)
            LOG.info(message)
            content = json.dumps(message)
            while True:
                try:
                    channel.basic_publish(exchange=conf.EXCHANGE,
                                     routing_key=conf.TOPIC,
                                     body=content, 
                                     properties=properties)
                    LOG.info("Sent success")
                    break
                except Exception:
                    pass
        start = message_time
        time.sleep(60-sw.elapsed())
        sw.stop()
   # LOG.info(" [x] Sent " + content)
    connection.close()

#messages_map = [('trove', 1, 6),('lb', 5, 6),('snapshot', 5, 6), ('network', 5, 6)]
messages_map = [('trove', 1, 6),('lb', 1, 6),('snapshot', 1, 6), ('network', 1, 6)]
#messages_map = [('lb', 1, 1)]

def batch_send_message(vm_num):
    messages = []
    for j in xrange(vm_num):
        for message_info in messages_map:
            message_type = message_info[0] 
            resource_num = message_info[1]
            message_num = message_info[2]
            for i in xrange(message_num):
                messages.append(generate_message(message_type, resource_num))
    start = datetime.datetime.now() - datetime.timedelta(seconds=60 * 1)
    sw = timeutils.StopWatch()
    workers = 100
    with ProcessPoolExecutor(max_workers=workers) as executor:
        message_num = len(messages) / workers
        extra = len(messages) % workers
        start = 0
        for i in xrange(workers):
            if i < extra:
                end = start + message_num + 1
            else:
                end = start + message_num 
            to_send = messages[start:end]
            executor.submit(send_message, to_send)
            start = end

if __name__ == "__main__":
    ipm_num=sys.argv[1]
    batch_send_message(int(ipm_num))
    
