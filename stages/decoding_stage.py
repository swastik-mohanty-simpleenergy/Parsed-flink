import cantools
from interface import Stage
from utils.message_payload import MessagePayload
from pathlib import Path
from logger import log  
import time
import logging

class CANMessageDecoder(Stage):
    def __init__(self, dbc_file: str):
        """
        Initialize the CANMessageDecoder with a DBC file.

        Args:
            dbc_file_path (str): Path to the DBC file.
        """
        self.dbc = cantools.database.load_file(dbc_file)

    def execute(self, payload: MessagePayload) -> MessagePayload:
        """
        Decode a CAN message from the payload.

        Args:
            payload (MessagePayload): The message payload to decode.
        """
        payload.time_in_millis_decode_start = time.time_ns()

        if payload.error_flag:
            return payload

        try:
            message = payload.message_json
            can_id_hex = payload.can_id_hex
            
            can_id_29bit = int(str(can_id_hex), 16) & 0x1FFFFFFF
            decoded_message = self.dbc.get_message_by_frame_id(can_id_29bit)
            
            data_bytes = bytearray([
                message.get(f"byte{i+1}", 0) for i in range(8)
            ])
            
            decoded_signals = decoded_message.decode(data_bytes, decode_choices=False)
            
            payload.signal_value_pair = decoded_signals
            payload.time_in_millis_decode_end = time.time_ns()

            log(f'[CANMessageDecoder]: Decoding stage took {payload.time_in_millis_decode_end-payload.time_in_millis_decode_start} ns for {payload.vin} at {payload.event_time}', level=logging.INFO)
        
        except Exception as e:
            log(f'[CANMessageDecoder]: error {e} occured while decoding {payload.vin} at {payload.event_time}', level=logging.CRITICAL)
            payload.error_tag = e
            payload.error_flag = True

        finally:
            return payload

