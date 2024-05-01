from pathlib import Path
from pytz import timezone, utc
from Monitoring.configs import MonitoringAPI, MonitoringTableID
import logging
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3.types import TimeInterval
from Monitoring.utils.exporter import BigqueryExporterBase


class CPUUsageOperator:
    def __init__(self, start_timestamp, end_timestamp):
        self.client = monitoring_v3.MetricServiceClient()
        self.project_id = "datapool-1806"
        self.project_name = f"projects/{self.project_id}"
        self.interval = TimeInterval()
        # Create Timestamps for start and end time
        self.interval.start_time = start_timestamp
        self.interval.end_time = end_timestamp

    def run(self):
        schema_path = Path(__file__).parent / f"schemas/{MonitoringTableID.cpu_used_table}.json"
        filter_string = self._get_filter(MonitoringAPI.cpu_usage_time_url)
        results = self._get_results(self.interval, filter_string)
        pod_info = self._get_cpu_accu_pod_info(results, "cpu_used_seconds", "double_value")
        bq_operator = BigqueryExporterBase(projectID=self.project_id)
        logging.info(f"Updating table {MonitoringTableID.cpu_used_table} with {len(pod_info)} rows.")
        bq_operator.update_table_using_replace(data=pod_info,
                                               schema_path=schema_path,
                                               datasetID='GKE_monitor_raw',
                                               tableID=MonitoringTableID.cpu_used_table)

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

    def _get_cpu_accu_pod_info(self, results, metric_type, value_type):
        pod_info = {}
        for result in results:
            for point in result.points:
                metric_value = getattr(point.value, value_type)
                pod_info[result.resource.labels["pod_name"]] = {
                    'Pod_name': result.resource.labels["pod_name"],
                    'Container_Name': result.resource.labels["container_name"],
                    'Namespace': result.resource.labels["namespace_name"],
                    'Execute_Time': point.interval.start_time.replace(tzinfo=utc).astimezone(timezone('Asia/Taipei')).strftime('%Y-%m-%dT%H:%M:%S'),
                    metric_type: metric_value
                }
        return list(pod_info.values())
