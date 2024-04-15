from pathlib import Path
import logging
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3.types import TimeInterval
from Monitoring.utils.exporter import BigqueryExporterBase


class PodResourceOperator:
    def __init__(self, metric_type, metric_url, table_id, value_type, start_timestamp, end_timestamp):
        self.client = monitoring_v3.MetricServiceClient()
        self.project_id = "datapool-1806"
        self.project_name = f"projects/{self.project_id}"
        self.schema_path = Path(__file__).parent / f"schemas/{table_id}.json"
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
        pod_info = self._get_pod_info(memory_results, self.metric_type, self.value_type)
        bq_operator = BigqueryExporterBase(projectID=self.project_id)
        logging.info(f"Updating table {self.table_id} with {len(pod_info)} rows")
        bq_operator.update_table_using_replace(data=pod_info,
                                               schema_path=self.schema_path,
                                               datasetID='GKE_monitor_raw',
                                               tableID=self.table_id)

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
                    'start_time': point.interval.start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                    'end_time': point.interval.end_time.strftime('%Y-%m-%dT%H:%M:%S'),
                    metric_type: metric_value
                })
        return pod_info
