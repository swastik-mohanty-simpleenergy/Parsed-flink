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
            linger_ms=10,                  
            batch_size=32768,              
            compression_type='lz4',        
            max_in_flight_requests_per_connection=5,  
            acks=1,                      
            retries=5,                     
            buffer_memory=67108864
        )

    def delivery_report(self, err, msg): 
        if err is not None:
            if err.code() == kafka.errors.OffsetNotAvailableError:
                log(f"[Producer]: No offset stored for topic {msg.topic()}, partition {msg.partition()}.", level=logging.WARNING)
            else:
                log("[Producer]: Message delivery failed.", level=logging.ERROR, error=err)
        else:
            log(f"[Producer]: Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}", level=logging.INFO)

    def send_data(self, data, topic, key=None):
        try:
            encoded_data = json.dumps(data).encode('utf-8')
            encoded_key = json.dumps(key).encode('utf-8') if key is not None else None
            future = self.producer.send(topic, encoded_data, key=encoded_key)

            future.add_callback(lambda meta: log(
                f"Delivered to {meta.topic}[{meta.partition}] @ {meta.offset}"
            ))
            future.add_errback(lambda exc: log(
                f"Delivery failed: {exc}", level=logging.ERROR
            ))
            
        except Exception as e:
            log("[Producer] Critical Error", level=logging.CRITICAL, exception=e)
            raise

        except BufferError:

            log('[Producer]: Local producer queue is full, consider backing off', level=logging.WARNING)
            queue_length = len(self.producer)
            log(f'[Producer]: Current producer queue length: {queue_length}', level=logging.WARNING)
            
            metrics = self.producer.metrics()
            log(f'[Producer]: Kafka producer metrics: {json.dumps(metrics, indent=2)}', level=logging.INFO)

            memory_info = psutil.virtual_memory()
            cpu_usage = psutil.cpu_percent(interval=1)
            log(f'[Producer]: System memory usage: {memory_info.percent}%', level=logging.INFO)
            log(f'[Producer]: CPU usage: {cpu_usage}%', level=logging.INFO)
            
            time.sleep(1)
        except Exception as e:
            log('[Producer]: Kafka Producer Unexpected error', level=logging.CRITICAL, exception=e, data=data)
            raise e

    def flush(self):
        try:
            self.producer.flush()
            log(f'[Producer]: All messages flushed successfully by Producer.', level=logging.INFO)
        except Exception as e:
            log(f'[Producer]: Unable to perform the flush.', level=logging.CRITICAL, exception=e)