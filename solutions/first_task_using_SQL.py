import pandas as pd

from config import sync_engine

sql_query = """
    SELECT DISTINCT communication_id, campaign_id
    FROM web_data.sessions s
         INNER JOIN web_data.communications c
                    ON s.visitor_id = c.visitor_id AND s.site_id = s.site_id
    ORDER BY communication_id, campaign_id;
"""

with sync_engine.connect() as conn:
    result_sql = pd.read_sql(sql_query, conn)
    print(result_sql)
