from solutions.first_task_using_SQL import result_sql
from solutions.first_task_using_pandas import result_pd

keys = ['communication_id', 'campaign_id']
clear_result_sql = result_sql[keys].sort_values(by=keys).reset_index(drop=True)
clear_result_pd = result_pd[keys].sort_values(by=keys).reset_index(drop=True)
if clear_result_sql.equals(clear_result_pd):
    print("DataFrame равны без учета нумерации строк")
else:
    print("DataFrame не равны без учета нумерации строк")
