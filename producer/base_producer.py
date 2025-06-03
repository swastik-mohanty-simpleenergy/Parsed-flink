from kafka import KafkaProducer
import kafka
import json
from logger import log  
import time
import psutil
import logging
import sys


class CustomKafkaProducer():
    """
    Base class for Kafka producers, providing common functionalities for producing messages.
    
    Attributes:
        producer (Producer): Confluent Kafka Producer instance.
    """
    def __init__(self, config):
        """
        Initialize the KafkaProducer with the list of broker addresses.

        Args:
            brokers (List[str]): A list of Kafka broker addresses.
        """
        self.producer = KafkaProducer(
            bootstrap_servers=config['brokers'],
            security_protocol=config['security_protocol'],
            sasl_mechanism=config['sasl_mechanism'],
            sasl_plain_username=config['sasl_username'],
            sasl_plain_password=config['sasl_password'],                
            batch_size=16384,               
            request_timeout_ms=180000,
            max_in_flight_requests_per_connection=1,
            retries=2,
            acks=1                
        )



    def send_data(self, data, topic, key=None):
        
        try:
            future = self.producer.send(topic=topic,key=json.dumps(key).encode('utf-8'),value=json.dumps(data).encode('utf-8'))
            return future
            
        except Exception as e:
            log("[Producer] Critical Error", level=logging.CRITICAL, exception=e)
            raise

