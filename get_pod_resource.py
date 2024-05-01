from Monitoring.PodResourceOperator.memory_operator import MemoryOperator
from Monitoring.PodResourceOperator.else_operator import ElseOperator
from Monitoring.PodResourceOperator.cpu_usage_operator import CPUUsageOperator
from google.protobuf.timestamp_pb2 import Timestamp
import datetime

start_time = datetime.datetime.now() - datetime.timedelta(days=1)
start_timestamp = Timestamp()
start_timestamp.FromDatetime(start_time)
end_time = datetime.datetime.now()
end_timestamp = Timestamp()
end_timestamp.FromDatetime(end_time)


def main(start_timestamp, end_timestamp):
    memory_operator = MemoryOperator(start_timestamp, end_timestamp)
    cpu_usage_operator = CPUUsageOperator(start_timestamp, end_timestamp)
    else_usage_operator = ElseOperator(start_timestamp, end_timestamp)
    memory_operator.run()
    cpu_usage_operator.run()
    else_usage_operator.run()


main(start_timestamp, end_timestamp)
