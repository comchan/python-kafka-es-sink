from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import sys


es_hosts = ['http://elasticsearch-1:9200']
es_use_ssl = False
es_verify_cert = False # Set false if self-sign your ES server cert
es_ssl_shown_warn = False # Set false if self-sign your ES server cert
es_username = 'user'
es_password = 'password'

es_index = 'game.game-event' # Index which the message is posted to

kafka_bootstrap_servers = ['kafka-1:9092'] 
kafka_group_id = 'game-event-listener'
kafka_topic = 'game.game-event' # Kafka topic to listen to


def main():
    es = Elasticsearch(
        hosts=es_hosts, 
        use_ssl=es_use_ssl,
        verify_certs=es_verify_cert,
        ssl_show_warn=es_ssl_show_warn, 
        http_auth=(es_username, es_password) # remove if your ES doesn't require auth
    )
    
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=kafka_group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        message = message.value
        try:
            print(es.create(es_index, message['betInstanceID'], message, ignore=[400,409]))
        except elasticsearch.exceptions.ElasticsearchException as e:
            print(e)

if __name__ == '__main__':
    main()
