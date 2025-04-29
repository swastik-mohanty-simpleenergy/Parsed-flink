from producer import KafkaSender 
from stages import CANMessageDecoder
from utils import KafkaConfig, MessagePayload, setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = Path(__file__).parent
DBC_FILE_PATH = str(BASE_DIR / "dbc_files/SimpleOneGen1_V2_2.dbc")
JSON_FILE = str(BASE_DIR / "canId_TopicMap.json")

def main():

    env = setup_flink_environment()
    kafka_source = KafkaConfig.create_kafka_source()
    kafka_output_config = KafkaConfig.get_kafka_producer_config()
    
    can_decoder = CANMessageDecoder(DBC_FILE_PATH)
    kafka_sender = KafkaSender(kafka_output_config, JSON_FILE)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    processed_stream = (data_stream
                        .map(MessagePayload, output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(can_decoder.execute, output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(kafka_sender, output_type=Types.STRING()))
    
    processed_stream.print() 

    env.execute("Flink_parser")

if __name__ == "__main__":

    main()
