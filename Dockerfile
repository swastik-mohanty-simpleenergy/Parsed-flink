# Use Flink 1.20 with Java 17 (Official Image)
FROM flink:1.20-java17

# Install Python, Pip & Zip
RUN apt-get update && apt-get install -y python3 python3-pip zip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Remove old Kafka connector if it exists
RUN rm -f /opt/flink/lib/flink-connector-kafka-*.jar

# Download the correct Kafka connector for Flink 1.20
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.4.0-1.20/flink-connector-kafka-3.4.0-1.20.jar

# Set the working directory inside the container
WORKDIR /opt/pyflink-job

# Copy requirements.txt first to leverage Docker layer caching
COPY requirements.txt /opt/pyflink-job/requirements.txt

# Install PyFlink & Dependencies
RUN pip install apache-flink && pip install --no-cache-dir -r /opt/pyflink-job/requirements.txt

#zipping the required components
RUN mkdir /tmp/zip-source
COPY stages /tmp/zip-source/stages
COPY producer /tmp/zip-source/producer
COPY .env /tmp/zip-source/.env
COPY interface /tmp/zip-source/interface
COPY logger /tmp/zip-source/logger
COPY utils /tmp/zip-source/utils

# Create a zip archive of required components
RUN cd /tmp/zip-source && zip -r /opt/pyflink-job/dependencies.zip .

# # Copy the entire project folder into the container AFTER installing dependencies
COPY main.py /opt/pyflink-job/
COPY dbc_files /opt/pyflink-job/dbc_files
COPY canId_TopicMap.json /opt/pyflink-job/
# COPY .env /opt/pyflink-job/

# Copy Kafka client JAR if it exists
# RUN test -f /opt/pyflink-job/jars/kafka-clients-3.4.0.jar && cp /opt/pyflink-job/jars/kafka-clients-3.4.0.jar /opt/flink/lib/ || echo "Kafka client JAR not found, skipping copy"
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# RUN mv /opt/pyflink-job/jars/kafka-clients-3.4.0.jar /opt/flink/lib/

RUN wget -P /opt/flink/plugins/s3 https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/1.20.1/flink-s3-fs-presto-1.20.1.jar

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# Download an official flink-python JAR (if you verify one is compatible)
# RUN wget -O /opt/flink/lib/python-container.jar \
#   https://repo1.maven.org/maven2/org/apache/flink/flink-python_2.12/1.15.4/flink-python_2.12-1.15.4.jar


# Set default command to run PyFlink job
ENTRYPOINT ["flink", "run",  "--pyExecutable", "/usr/bin/python3", "--pyfs", "dependencies.zip", "-py", "/opt/pyflink-job/main.py"]