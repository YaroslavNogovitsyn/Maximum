import pandas as pd

from config import sync_engine

sql_query = """
    WITH cte AS (SELECT MAX(s.visitor_session_id)   AS visitor_session_id,
                    c.visitor_id,
                    s.site_id,
                    COUNT(DISTINCT s.date_time) AS row_n,
                    MAX(s.date_time)            AS session_date_time
             FROM web_data.communications c
                      LEFT JOIN web_data.sessions s ON c.visitor_id = s.visitor_id AND c.site_id = s.site_id AND c.date_time >= s.date_time
             GROUP BY c.visitor_id, s.site_id)
SELECT c.communication_id,
       c.site_id,
       c.visitor_id,
       c.date_time AS communication_date_time,
       s.visitor_session_id,
       cte.session_date_time,
       s.campaign_id,
       cte.row_n
FROM web_data.sessions s
         INNER JOIN web_data.communications c
                    ON s.visitor_id = c.visitor_id AND s.site_id = c.site_id
         INNER JOIN cte ON cte.visitor_session_id = s.visitor_session_id;
"""

with sync_engine.connect() as conn:
    result_sql = pd.read_sql(sql_query, conn)
    print(result_sql)


