import json
from logger import log
from cantools.database.namedsignalvalue import NamedSignalValue
from .base_producer import CustomKafkaProducer
from utils import MessagePayload
import time
import logging

class KafkaDataProducer(CustomKafkaProducer):
    """
    Kafka producer for standard data messages, handling serialization and topic-specific production.
    """
    def __init__(self, config, topic_path):
        super().__init__(config)
        
        with open(topic_path, 'r') as file:
            self.topics : dict = json.load(file)

        self.partialFault_canID = []

        for key, value in self.topics.items():
            if isinstance(value,dict):
                self.partialFault_canID.append(int(key))

    def send_data(self, payload: MessagePayload):
        """
        Process and send data to specific Kafka topics based on the data content.
        """

        def delivery_callback(meta):
            pass

        def error_callback(exc):
            log(f"Delivery failed: {exc}", level=logging.ERROR)

        try:
            payload.time_in_millis_producer_start = time.time_ns()
            if payload.error_flag:
                return self.error_data_producer(payload)
    
            vin = payload.vin
            event_time = payload.event_time
            topics_data = self.prepare_topics_data(payload, vin, event_time)


            if not isinstance(topics_data, tuple):

                future= super().send_data(  key=self.create_key(vin),
                                    data=topics_data, 
                                    topic=self.topics[str(payload.can_id_int)]
                                )
                future.add_callback(delivery_callback)
                future.add_errback(error_callback)
            
            else:

                future_fault=super().send_data( key=self.create_key(vin),
                                    data=topics_data[0], 
                                    topic="Faults"
                                )
                future_fault.add_callback(delivery_callback)
                future_fault.add_errback(error_callback)
                future_nonFault=super().send_data( key=self.create_key(vin),
                                    data=topics_data[1], 
                                    topic=self.topics[str(payload.can_id_int)]["non_faults"]
                                )
                future_nonFault.add_callback(delivery_callback)
                future_nonFault.add_errback(error_callback)

        except Exception as e:
                log(f"[Data Producer]: Failed to send the complete processed data to Kafka of {payload.vin} at {payload.event_time} due to error {e}.", level=logging.ERROR)
                payload.error_flag = True
                return self.error_data_producer(payload)

    
        log(f"[Data Producer]: Message batch for VIN {payload.vin} is processed.", level=logging.INFO)
        payload.time_in_millis_producer_end = time.time_ns()

        log(f"[Data Producer]: Producer send took {payload.time_in_millis_producer_end-payload.time_in_millis_producer_start} ns for {payload.vin} at {payload.event_time}.", level=logging.INFO)
        return topics_data
 
    def prepare_topics_data(self, payload : MessagePayload, vin : str, event_time : int) -> dict:
        """
        Organize data by topics for sending to Kafka.
        """

        if payload.can_id_int not in self.partialFault_canID:

            payload.signal_value_pair["vin"] = vin
            payload.signal_value_pair["event_time"] = event_time

            return payload.signal_value_pair
        
        else:

            faults_data = {}
            non_faults_data = {}

            map = self.topics.get(str(payload.can_id_int))

            for key, value in payload.signal_value_pair.items():
                if key in map["faults"]:                    
                    faults_data[key] = value
                else:
                    non_faults_data[key] = value 

            faults_data["vin"] = vin
            faults_data["event_time"] = event_time

            non_faults_data["vin"] = vin
            non_faults_data["event_time"] = event_time     
            
        return faults_data, non_faults_data
    
    
    def create_key(self, vin):
        """
        Create a key by combining VIN and event time.
        """
        return f"{vin}"
    
    def error_data_producer(self, payload: MessagePayload):
        """
        Handle error data by sending it to the DLQ topic.
        """  
        super().send_data(data=payload.message_json, topic=payload.dlq_topic)
        log(f"[Data Producer]: {payload.vin} data of event time {payload.event_time} is sent to {payload.dlq_topic} topic", level=logging.INFO)
        
        return {"vin": payload.vin, "event_time": payload.event_time , "error": str(payload.error_tag)}