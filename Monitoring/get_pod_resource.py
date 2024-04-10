from PodResourceOperator.operator import PodResourceOperator
import datetime


# Define the start and end time for the query
start_time = datetime.datetime.now() - datetime.timedelta(days=1)
end_time = datetime.datetime.now()

get_pod_operator = PodResourceOperator(start_time, end_time)
get_pod_operator.run()