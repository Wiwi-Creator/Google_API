from google.cloud import monitoring_v3
from google.cloud import bigquery
from google.cloud.monitoring_v3.types import TimeInterval
from google.protobuf.timestamp_pb2 import Timestamp


class PodResourceOperator:
    def __init__(self, start_time, end_time):
        self.client = monitoring_v3.MetricServiceClient()
        self.project_name = "projects/datapool-1806"
        self.memory_metric_type = "kubernetes.io/container/memory/used_bytes"

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
        filter_string = self._get_filter(self.memory_metric_type)
        memory_results = self._get_results(filter_string)
        pod_info_memory = self._get_pod_info(memory_results)
        self._insert_into_bigquery(pod_info_memory)

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

    def _get_pod_info(self, results):
        pod_info = {}
        for result in results:
            for point in result.points:
                memory_usage_gb = point.value.int64_value / (1024 * 1024 * 1024)  # Convert to GB
                if (result.resource.labels["pod_name"], result.resource.labels["namespace_name"]) not in pod_info:
                    pod_info[(result.resource.labels["pod_name"], result.resource.labels["namespace_name"])] = {
                        'start_time': point.interval.start_time,
                        'end_time': point.interval.end_time,
                        'max_memory_usage': memory_usage_gb
                    }
                else:
                    if memory_usage_gb > pod_info[(result.resource.labels["pod_name"], result.resource.labels["namespace_name"])]['max_memory_usage']:
                        pod_info[(result.resource.labels["pod_name"], result.resource.labels["namespace_name"])] = {
                            'start_time': point.interval.start_time,
                            'end_time': point.interval.end_time,
                            'max_memory_usage': memory_usage_gb
                        }
        return pod_info

    def _insert_into_bigquery(self, pod_info_memory):
        client = bigquery.Client()
        full_table_id = "datapool-1806.wiwi_test.GKE_POD_MEMORY_USAGE"

        schema = [
            bigquery.SchemaField("Pod_Name", "STRING"),
            bigquery.SchemaField("Namespace", "STRING"),
            bigquery.SchemaField("Start_Time", "TIMESTAMP"),
            bigquery.SchemaField("End_Time", "TIMESTAMP"),
            bigquery.SchemaField("Max_Memory_Usage_GB", "FLOAT64"),
        ]

        table = bigquery.Table(full_table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)  # Use exists_ok=True to not raise error if table already exists

        rows_to_insert = [
            (
                pod_name,
                namespace,
                info['start_time'],
                info['end_time'],
                info['max_memory_usage'],
            )
            for (pod_name, namespace), info in pod_info_memory.items()
        ]

        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Errors while inserting rows: {errors}")
        else:
            print("Rows inserted successfully.")