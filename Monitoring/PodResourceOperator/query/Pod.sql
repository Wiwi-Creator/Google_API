WITH 
    Pod_MEMORY_USED AS (
        SELECT 
            Pod_Name AS POD_NAME,
            Namespace,
            memory_used_bytes,
            DATE(Execute_time) AS Execute_date
        FROM `datapool-1806.GKE_monitor_raw.GKE_POD_MEMORY_USED`
    ),
    Pod_MEMORY_REQUEST AS (
        SELECT 
            Pod_Name AS POD_NAME,
            Namespace,
            memory_request_bytes,
            DATE(Execute_time) AS Execute_date
        FROM `datapool-1806.GKE_monitor_raw.GKE_POD_MEMORY_REQUEST`
    ),
    Pod_MEMORY_UTILIZATION AS (
        SELECT 
            Pod_Name AS POD_NAME,
            Namespace,
            memory_request_utilization,
            DATE(Execute_time) AS Execute_date
        FROM `datapool-1806.GKE_monitor_raw.GKE_POD_MEMORY_UTILIZATION`
    )

SELECT 
    Pod_List.Pod_Name AS Pod_name,
    Pod_List.Namespace,
    AVG(request.memory_request_bytes) /  (1024 * 1024 * 1024) AS AVG_memory_request_GiB,
    AVG(used.memory_used_bytes) / (1024 * 1024 * 1024) AS AVG_memory_used_GiB,
    MAX(used.memory_used_bytes) / (1024 * 1024 * 1024) AS MAX_memory_used_GiB,
    AVG(memory_utilization.memory_request_utilization) AS AVG_memory_utilization,
    used.Execute_date As Execute_date
FROM `datapool-1806.GKE_monitor_raw.GKE_POD_LIST` Pod_List
JOIN Pod_MEMORY_REQUEST request
    ON Pod_List.Pod_Name = request.POD_NAME
JOIN Pod_MEMORY_USED used
    ON Pod_List.Pod_Name = used.POD_NAME
JOIN Pod_MEMORY_UTILIZATION memory_utilization
    ON Pod_List.Pod_Name = memory_utilization.POD_NAME
GROUP BY Pod_name, Namespace, Execute_date 
;