class MonitoringAPI:
    cpu_usage_time_url = "kubernetes.io/container/cpu/core_usage_time"
    cpu_request_utilization_url = "kubernetes.io/container/cpu/request_utilization"
    cpu_utilization_url = "kubernetes.io/container/cpu/utilization"
    cpu_request_cores_url = "kubernetes.io/container/cpu/request_cores"
    memory_used_bytes_url = "kubernetes.io/container/memory/used_bytes"
    memory_request_bytes_url = "kubernetes.io/container/memory/request_bytes"
    memory_request_utilization_url = "kubernetes.io/container/memory/request_utilization"
    ephemeral_storage_limit_bytes_url = "kubernetes.io/container/ephemeral_storage/limit_bytes"
    ephemeral_storage_request_bytes_url = "kubernetes.io/container/ephemeral_storage/request_bytes"
    ephemeral_storage_used_bytes_url = "kubernetes.io/container/ephemeral_storage/used_bytes"


class MonitoringTableID:
    memory_utiliztion_table = "GKE_POD_MEMORY_UTILIZATION"
    memory_request_table = "GKE_POD_MEMORY_REQUEST"
    memory_used_table = "GKE_POD_MEMORY_USED"
    cpu_utiliztion_table = "GKE_POD_CPU_UTILIZATION"
    cpu_request_table = "GKE_POD_CPU_REQUEST"
    cpu_used_table = "GKE_POD_CPU_USED"
    ssd_utiliztion_table = "GKE_POD_SSD_UTILIZATION"
    ssd_request_table = "GKE_POD_SSD_REQUEST"
    ssd_used_table = "GKE_POD_SSD_USED"
