from typing import Dict, Any
import json
import time

class MessagePayload:
    """
    Manages the payload of messages, handling user information, compressed data, and processing errors.

    Attributes:
        username (str): Username associated with the message.
        vin (str): Vehicle Identification Number.
        message_json (Dict[str, Any]): Original message in JSON format.
        compressed (Optional[str]): Base64-encoded compressed data string.
        decompressed (Optional[bytes]): Decompressed data bytes.
        kafka_producer_error (Optional[str]): Error message from Kafka producer, if any.
        parsed (Optional[Any]): Parsed data from the decompressed message.
        can_decoded_data (Optional[Any]): CAN bus decoded data.
        can_decoding_errors (Optional[Dict]): Error message from CAN decoding, if any.
    """

    def __init__(self, binary_message: bytes):
        """
        Constructs a MessagePayload object by decoding a binary JSON message.

        Args:
            binary_message (bytes): The binary message containing a JSON-encoded string.

        Raises:
            json.JSONDecodeError: If the binary message is not properly JSON-encoded.
        """
        json_message = json.loads(binary_message)
        self.message_json : dict = json_message
        self.vin : str = json_message.get('vin', None)
        self.error_flag : bool = False
        
        # Processed Consumer Variables
        self.signal_value_pair : dict = {}
        self.can_id_int : int = int(json_message.get('raw_can_id', None),16)
        self.event_time : int = json_message.get('event_time', None)
        
        # Common
        self.success_counts = 0
        self.dlq_topic = "can-dlq"
        self.error_tag = None

        # Time Tracking
        self.time_in_millis_MessagePayload_instance = time.time_ns()
        self.time_in_millis_decode_start = None
        self.time_in_millis_decode_end = None
        self.time_in_millis_producer_start = None
        self.time_in_millis_producer_end = None

    def __str__(self):
        return f"MessagePayload(vin={self.vin})"