
from flask import Flask, request
from pyiceberg import iceberg
from pyarrow import schema as arrow_schema
from pyspark.sql import SparkSession
import json
# Initialize Spark session
spark = SparkSession.builder.appName("IcebergSession").getOrCreate()
app = Flask(__name__)

# Define Iceberg schema for Metrics, Logs, and Traces
metrics_schema = arrow_schema([
    ('Name', 'string'),
    ('Kind', 'string'),
    ('Unit', 'double'),
    ('Description', 'string')
])

logs_schema = arrow_schema([
    ('timestamp', 'timestamp[ms]'),
    ('observed_timestamp', 'timestamp[ms]'),
    ('trace_id', 'string'),
    ('span_id', 'string'),
    ('trace_flags', 'string'),
    ('severity_text', 'string'),
    ('severity_number', 'int'),
    ('body', 'string'),
    ('resource', 'string'),
    ('instrumentation_scope', 'string')
   
])
traces_schema = arrow_schema([
    ('name', 'string'),
    ('trace_id', 'string'),
    ('span_id', 'string'),
    ('parent_id', 'string'),
    ('start_time', 'timestamp[ms]'),
    ('end_time', 'timestamp[ms]'),
    ('http_route', 'string'),  
    ('event_name', 'string'),  
    ('event_timestamp', 'timestamp[ms]'), 
    ('event_attributes', 'int'), 
])
#local file paths for each tabld
Metrics_datapath= "/Documents/Opentelemetry/MetricsData"
Logs_datapath= "/Documents/Opentelemetry/LogsData"
Traces_datapath= "/Documents/Opentelemetry/TracesData"

# creating 3 different tables for metrics, logs and traces

spark.sql(f"CREATE TABLE metrics USING iceberg OPTIONS ('path'='{Metrics_datapath}', 'schema'='{metrics_schema}')")

spark.sql(f"CREATE TABLE logs USING iceberg OPTIONS ('path'='{Logs_datapath}', 'schema'='{logs_schema}')")

spark.sql(f"CREATE TABLE traces USING iceberg OPTIONS ('path'='{Traces_datapath}', 'schema'='{traces_schema}')")

#metrics_table = iceberg.create("/Documents/Opentelemetry/MetricsData", schema=metrics_schema)
#logs_table = iceberg.create("/Documents/Opentelemetry/LogsData", schema=logs_schema)
#traces_table = iceberg.create("/Documents/Opentelemetry/TracesData", schema=traces_schema)

@app.route('/otlp-endpoint', methods=['POST'])
def receive_otlp_data():
    try:
        data = request.get_data()
        # Process and store the OTLP data to the appropriate Iceberg table
        store_data_in_iceberg(data)
        print(f"Received OTLP data: {data}")
        return "Data received and stored successfully", 200
    except Exception as e:
        print(f"Error processing OTLP data: {e}")
        return "Error processing data", 500

def store_data_in_iceberg(data):
    
   # Transform the incoming data to fit the iceberg table schemas
    transformed_data = transform_data(data)
    # Determine the telemetry type and write data to the corresponding Iceberg table
    telemetry_type = determine_telemetry_type(transformed_data)

     # Determine the telemetry type and write transformed data to the corresponding Iceberg table 
    if telemetry_type == 'metrics':
        spark.sql(f"INSERT INTO metrics VALUES {transformed_data}")
    elif telemetry_type == 'logs':
        spark.sql(f"INSERT INTO logs VALUES {transformed_data}")
    elif telemetry_type == 'traces':
        spark.sql(f"INSERT INTO traces VALUES {transformed_data}")
    else:
        print(f"Unsupported telemetry type: {telemetry_type}")
def transform_data(raw_data):
    
    data_dict = json.loads(raw_data)

    # Common fields across all telemetry types
    common_fields = {
        'timestamp': data_dict.get('timestamp', None),
    }

    telemetry_type = determine_telemetry_type(data_dict)

    if telemetry_type == 'metrics':
        return {
            **common_fields,
            'metric_name': data_dict.get('metric_name', None),
            'value': data_dict.get('value', None),
        }
    elif telemetry_type == 'logs':
        return {
            **common_fields,
            'log_message': data_dict.get('log_message', None),
        }
    elif telemetry_type == 'traces':
        return {
            **common_fields,
            'trace_id': data_dict.get('trace_id', None),
            'span_id': data_dict.get('span_id', None),
            'start_time': data_dict.get('start_time', None),
            'end_time': data_dict.get('end_time', None),
        }
    else:
        print(f"Unsupported telemetry type: {telemetry_type}")
        return None
    
def determine_telemetry_type(data):
    if 'filtered_attributes' in data:
        return 'metrics'
    elif 'severity_message' in data:
        return 'logs'
    else:
        return 'traces'
    

if __name__ == '__main__':
    try:
        app.run(port=5000)
    finally:
        
        spark.stop()

