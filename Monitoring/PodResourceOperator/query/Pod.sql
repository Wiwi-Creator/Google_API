SELECT 
    -- MEMORY Info
    Pod_List.Pod_Name AS Pod_name,
    Pod_List.Namespace AS Namespace,
    AVG(memory_request.memory_request_bytes)/ (1024 * 1024 * 1024) AS AVG_memory_request_GiB,
    AVG(memory_used.memory_used_bytes) / (1024 * 1024 * 1024) AS AVG_memory_used_GiB,
    MAX(memory_used.memory_used_bytes) / (1024 * 1024 * 1024) AS MAX_memory_used_GiB,
    AVG(memory_utilization.memory_request_utilization) AS AVG_memory_utilization,
    Pod_List.Execute_date As Execute_date
FROM `datapool-1806.GKE_monitor_lobby.GKE_POD_LIST` Pod_List
JOIN `datapool-1806.GKE_monitor_raw.GKE_POD_MEMORY_REQUEST` memory_request
    ON Pod_List.Pod_Name = memory_request.POD_NAME
    AND Pod_List.Execute_date = DATE(memory_request.Execute_time)
JOIN `datapool-1806.GKE_monitor_raw.GKE_POD_MEMORY_USED` memory_used
    ON Pod_List.Pod_Name = memory_used.POD_NAME
    AND  Pod_List.Execute_Date = DATE(memory_used.Execute_time)
JOIN `datapool-1806.GKE_monitor_raw.GKE_POD_MEMORY_UTILIZATION` memory_utilization
    ON Pod_List.Pod_Name = memory_utilization.POD_NAME
    AND  Pod_List.Execute_Date = DATE(memory_utilization.Execute_time)
WHERE Pod_List.Execute_date = '2024-04-17'
GROUP BY Pod_name, Namespace, Execute_date 
;