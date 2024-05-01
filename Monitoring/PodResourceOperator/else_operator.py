from pathlib import Path
from pytz import timezone, utc
from Monitoring.configs import MonitoringAPI, MonitoringTableID
import logging
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3.types import TimeInterval
from Monitoring.utils.exporter import BigqueryExporterBase


class ElseOperator:
    def __init__(self, start_timestamp, end_timestamp):
        self.client = monitoring_v3.MetricServiceClient()
        self.project_id = "datapool-1806"
        self.project_name = f"projects/{self.project_id}"
        self.interval = TimeInterval()
        # Create Timestamps for start and end time
        self.interval.start_time = start_timestamp
        self.interval.end_time = end_timestamp

    def run(self):

        metric_types = [
            ("cpu_request_cores", MonitoringAPI.cpu_request_cores_url, MonitoringTableID.cpu_request_table, 'int64_value'),
            ("cpu_request_utilization", MonitoringAPI.cpu_request_utilization_url, MonitoringTableID.cpu_utilization_table, 'double_value'),
            ("ssd_request_bytes", MonitoringAPI.ephemeral_storage_request_bytes_url, MonitoringTableID.ssd_request_table, 'int64_value'),
            ("ssd_used_bytes", MonitoringAPI.ephemeral_storage_used_bytes_url, MonitoringTableID.ssd_used_table, 'int64_value')
        ]

        for metric_name, metric_url, table_id, value_type in metric_types:
            schema_path = Path(__file__).parent / f"schemas/{table_id}.json"
            filter_string = self._get_filter(metric_url)
            results = self._get_results(self.interval, filter_string)
            pod_info = self._get_pod_info(results, metric_name, value_type)

            bq_operator = BigqueryExporterBase(projectID=self.project_id)
            logging.info(f"Updating table {table_id} with {len(pod_info)} rows.")

            bq_operator.update_table_using_replace(data=pod_info,
                                                   schema_path=schema_path,
                                                   datasetID='GKE_monitor_raw',
                                                   tableID=table_id)

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
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            }
        )
        return results

    def _get_pod_info(self, results, metric_type, value_type):
        pod_info = []
        for result in results:
            for point in result.points:
                metric_value = getattr(point.value, value_type)
                pod_info.append({
                    'Pod_name': result.resource.labels["pod_name"],
                    'Container_name': result.resource.labels["container_name"],
                    'Namespace': result.resource.labels["namespace_name"],
                    'Execute_Time': point.interval.start_time.replace(tzinfo=utc).astimezone(timezone('Asia/Taipei')).strftime('%Y-%m-%dT%H:%M:%S'),
                    metric_type: metric_value
                })
        return pod_info
