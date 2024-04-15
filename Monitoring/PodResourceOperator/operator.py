from google.cloud import monitoring_v3
from google.cloud import bigquery
from google.cloud.monitoring_v3.types import TimeInterval


class PodResourceOperator:
    def __init__(self, metric_type, metric_url, table_id, value_type, start_timestamp, end_timestamp):
        self.client = monitoring_v3.MetricServiceClient()
        self.project_name = "projects/datapool-1806"
        self.metric_type = metric_type
        self.metric_url = metric_url
        self.table_id = table_id
        self.value_type = value_type
        self.interval = TimeInterval()
        # Create Timestamps for start and end time
        self.interval.start_time = start_timestamp
        self.interval.end_time = end_timestamp

    def run(self):
        filter_string_memory = self._get_filter(self.metric_url)
        memory_results = self._get_results(self.interval, filter_string_memory)
        pod_info_memory = self._get_pod_info(memory_results, self.metric_type, self.value_type)
        self._insert_into_bigquery(pod_info_memory, self.table_id, self.metric_type)

    def _get_filter(self, metric_url):
        filter_string = (
            f'metric.type = "{metric_url}" '
            'AND resource.type = "k8s_container" '
            'AND resource.labels.namespace_name = "default" '
            )
        return filter_string

    def _get_results(self, interval, filter_string):
        results = self.client.list_time_series(
            request={
                "name": self.project_name,
                "filter": filter_string,
                "interval": interval,
            }
        )
        return results

    def _get_pod_info(self, results, metric_type, value_type):
        pod_info = []
        for result in results:
            for point in result.points:
                metric_value = getattr(point.value, value_type)
                key = (result.resource.labels["pod_name"], result.resource.labels["namespace_name"])
                pod_info.append({
                    'pod_name': key[0],
                    'namespace': key[1],
                    'start_time': point.interval.start_time,
                    'end_time': point.interval.end_time,
                    metric_type: metric_value
                })
        return pod_info

    def _insert_into_bigquery(self, pod_info, table_name, metric_type):
        client = bigquery.Client()
        full_table_id = f"datapool-1806.wiwi_test.{table_name}"

        schema = [
            bigquery.SchemaField("Pod_Name", "STRING"),
            bigquery.SchemaField("Namespace", "STRING"),
            bigquery.SchemaField("Start_Time", "TIMESTAMP"),
            bigquery.SchemaField("End_Time", "TIMESTAMP"),
            bigquery.SchemaField(metric_type, "FLOAT64"),
        ]

        table = bigquery.Table(full_table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)

        rows_to_insert = [
            {
                "pod_name": info['pod_name'],
                "namespace": info['namespace'],
                "start_time": info['start_time'],
                "end_time": info['end_time'],
                metric_type: info[metric_type],
            }
            for info in pod_info
        ]

        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Errors while inserting rows: {errors}")
        else:
            print(f"Rows inserted successfully into {table_name}.")
