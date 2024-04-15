from Monitoring.PodResourceOperator.operator import PodResourceOperator
from Monitoring.configs import MonitoringAPI, MonitoringTableID
from google.protobuf.timestamp_pb2 import Timestamp
import datetime

start_time = datetime.datetime.now() - datetime.timedelta(days=5)
start_timestamp = Timestamp()
start_timestamp.FromDatetime(start_time)
end_time = datetime.datetime.now()
end_timestamp = Timestamp()
end_timestamp.FromDatetime(end_time)

memory_request_operator = PodResourceOperator(
    'memory_request_bytes',
    MonitoringAPI.memory_request_bytes_url,
    MonitoringTableID.memory_request_table,
    'int64_value',
    start_timestamp,
    end_timestamp
    )
memory_request_operator.run()

memory_used_operator = PodResourceOperator(
    'memory_usage_bytes',
    MonitoringAPI.memory_used_bytes_url,
    MonitoringTableID.memory_used_table,
    'int64_value',
    start_timestamp,
    end_timestamp
    )
memory_used_operator.run()

cpu_request_operator = PodResourceOperator(
    'cpu_request_bytes',
    MonitoringAPI.cpu_request_cores_url,
    MonitoringTableID.cpu_request_table,
    'int64_value',
    start_timestamp,
    end_timestamp
    )
cpu_request_operator.run()

cpu_used_operator = PodResourceOperator(
    'cpu_used_bytes',
    MonitoringAPI.cpu_usage_time_url,
    MonitoringTableID.cpu_used_table,
    'int64_value',
    start_timestamp,
    end_timestamp
    )
cpu_used_operator.run()

ssd_request_operator = PodResourceOperator(
    'ssd_request_bytes',
    MonitoringAPI.ephemeral_storage_request_bytes_url,
    MonitoringTableID.ssd_request_table,
    'int64_value',
    start_timestamp,
    end_timestamp
    )
ssd_request_operator.run()

ssd_request_operator = PodResourceOperator(
    'ssd_used_bytes',
    MonitoringAPI.ephemeral_storage_used_bytes_url,
    MonitoringTableID.ssd_used_table,
    'int64_value',
    start_timestamp,
    end_timestamp
    )
ssd_request_operator.run()
