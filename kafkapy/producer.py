from kafka import KafkaProducer
import json

import time
from kafka.errors import KafkaError

def get_registered_user(i):
    
    return {"name":i,
    "adress":i
    }


# def json_serializer(data):
#     return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                        
def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

if __name__ == "__main__":
    i=0
    while 1 == 1:
        
        registered_user = get_registered_user(i)
        print(registered_user)
        #producer.send("registered_user", registered_user).add_callback(on_send_success).add_errback(on_send_error)
        producer.send("registered_user", registered_user).add_both(add_callback=on_send_success,add_errback=on_send_error)
        
        i=i+1
        time.sleep(4)