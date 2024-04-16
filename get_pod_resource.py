from Monitoring.PodResourceOperator.operator import PodResourceOperator
from google.protobuf.timestamp_pb2 import Timestamp
import datetime

start_time = datetime.datetime.now() - datetime.timedelta(days=1)
start_timestamp = Timestamp()
start_timestamp.FromDatetime(start_time)
end_time = datetime.datetime.now()
end_timestamp = Timestamp()
end_timestamp.FromDatetime(end_time)

pod_operator = PodResourceOperator(
    start_timestamp,
    end_timestamp
    )

pod_operator.run()
