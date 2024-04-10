from google.cloud import monitoring_v3
from google.cloud import bigquery
from google.cloud.monitoring_v3.types import TimeInterval
from google.protobuf.timestamp_pb2 import Timestamp
from Monitoring.configs import MonitoringAPI


class PodResourceOperator:
    def __init__(self, start_time, end_time):
        self.client = monitoring_v3.MetricServiceClient()
        self.project_name = "projects/datapool-1806"
        self.memory_metric_type = MonitoringAPI.memory_used_bytes_url
        self.cpu_metric_type = MonitoringAPI.cpu_usage_time_url
        self.ssd_metric_type = MonitoringAPI.ephemeral_storage_used_bytes_url

        # Create TimeInterval
        self.interval = TimeInterval()

        # Create Timestamps for start and end time
        start_timestamp = Timestamp()
        start_timestamp.FromDatetime(start_time)
        self.interval.start_time = start_timestamp

        end_timestamp = Timestamp()
        end_timestamp.FromDatetime(end_time)
        self.interval.end_time = end_timestamp

    def run(self):
        filter_string_memory = self._get_filter(self.memory_metric_type)
        memory_results = self._get_results(filter_string_memory)
        pod_info_memory = self._get_pod_info(memory_results, 'max_memory_usage')

        filter_string_cpu = self._get_filter(self.cpu_metric_type)
        cpu_results = self._get_results(filter_string_cpu)
        pod_info_cpu = self._get_pod_info(cpu_results, 'cpu_usage_time')

        filter_string_ssd = self._get_filter(self.ssd_metric_type)
        ssd_results = self._get_results(filter_string_ssd)
        pod_info_ssd = self._get_pod_info(ssd_results, 'ssd_usage')

        pod_info = self._merge_pod_info(pod_info_memory, pod_info_cpu, pod_info_ssd)
        self._insert_into_bigquery(pod_info)

    def _get_filter(self, metric_type):
        filter_string = (
            f'metric.type = "{metric_type}" '
            'AND resource.type = "k8s_container" '
            'AND resource.labels.namespace_name = "default" '
            )
        return filter_string

    def _get_results(self, filter_string):
        # Construct the query for memory usage
        results = self.client.list_time_series(
            request={
                "name": self.project_name,
                "filter": filter_string,
                "interval": self.interval,
            }
        )
        return results

    def _get_pod_info(self, results, metric_name):
        pod_info = {}
        for result in results:
            for point in result.points:
                metric_value = point.value.int64_value
                key = (result.resource.labels["pod_name"], result.resource.labels["namespace_name"])
                if key not in pod_info:
                    pod_info[key] = {
                        'start_time': point.interval.start_time,
                        'end_time': point.interval.end_time,
                        metric_name: metric_value
                    }
                else:
                    pod_info[key][metric_name] = metric_value
                    pod_info[key]['start_time'] = point.interval.start_time
                    pod_info[key]['end_time'] = point.interval.end_time
        return pod_info

    def _merge_pod_info(self, pod_info_memory, pod_info_cpu, pod_info_ssd):
        pod_info = {}
        for d in [pod_info_memory, pod_info_cpu, pod_info_ssd]:
            for key, value in d.items():
                if key not in pod_info:
                    pod_info[key] = value
                else:
                    pod_info[key].update(value)
        return pod_info
    
    def _insert_into_bigquery(self, pod_info):
        client = bigquery.Client()
        full_table_id = "datapool-1806.wiwi_test.GKE_POD_MEMORY_USAGE"

        schema = [
            bigquery.SchemaField("Pod_Name", "STRING"),
            bigquery.SchemaField("Namespace", "STRING"),
            bigquery.SchemaField("Start_Time", "TIMESTAMP"),
            bigquery.SchemaField("End_Time", "TIMESTAMP"),
            bigquery.SchemaField("Max_Memory_Usage_GB", "FLOAT64"),
            bigquery.SchemaField("CPU_Usage_Time", "FLOAT64"),
            bigquery.SchemaField("SSD_Usage", "FLOAT64"),
        ]

        table = bigquery.Table(full_table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)

        rows_to_insert = [
            (
                pod_name,
                namespace,
                info['start_time'],
                info['end_time'],
                info.get('max_memory_usage', None),
                info.get('cpu_usage_time', None),
                info.get('ssd_usage', None),
            )
            for (pod_name, namespace), info in pod_info.items()
        ]

        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Errors while inserting rows: {errors}")
        else:
            print("Rows inserted successfully.")