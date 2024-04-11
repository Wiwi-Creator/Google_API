from Monitoring.PodResourceOperator.operator import PodResourceOperator
from Monitoring.configs import MonitoringAPI
import datetime


# Define the start and end time for the query
start_time = datetime.datetime.now() - datetime.timedelta(days=5)
end_time = datetime.datetime.now()
memory_metric_type = MonitoringAPI.memory_used_bytes_url
cpu_metric_type = MonitoringAPI.cpu_usage_time_url
ssd_metric_type = MonitoringAPI.ephemeral_storage_used_bytes_url

memory_table_id = "GKE_POD_MEMORY"
ssd_table_id = "GKE_POD_SSD"

memory_operator = PodResourceOperator('max_memory_usage', memory_metric_type, memory_table_id, start_time, end_time)
memory_operator.run()

#memory_operator = PodResourceOperator('max_ssd_usage', ssd_metric_type, ssd_table_id, start_time, end_time)
#memory_operator.run()