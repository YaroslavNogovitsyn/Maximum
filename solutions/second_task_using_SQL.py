import pandas as pd

from config import sync_engine

sql_query = """
WITH cte AS (SELECT MAX(c.communication_id)     as communication_id,
                    MAX(s.visitor_session_id)   AS visitor_session_id,
                    COUNT(DISTINCT s.date_time) AS row_n,
                    MAX(s.date_time)            AS session_date_time
             FROM web_data.communications c
                      LEFT JOIN web_data.sessions s
                                 ON s.visitor_id = c.visitor_id AND s.site_id = c.site_id AND c.date_time > s.date_time
             GROUP BY c.communication_id)
SELECT DISTINCT cte.communication_id,
                c.site_id,
                c.visitor_id,
                c.date_time AS communication_date_time,
                cte.visitor_session_id,
                cte.session_date_time,
                s.campaign_id,
                cte.row_n
FROM web_data.communications c
         INNER JOIN cte ON cte.communication_id = c.communication_id
         LEFT JOIN web_data.sessions s
                   ON s.visitor_id = c.visitor_id AND s.site_id = c.site_id AND cte.session_date_time = s.date_time;
"""

with sync_engine.connect() as conn:
    result_sql = pd.read_sql(sql_query, conn)
    print(result_sql)


