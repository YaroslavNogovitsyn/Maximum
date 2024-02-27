import pandas as pd

from config import sync_engine


get_sessions = """
    SELECT * FROM web_data.sessions;
"""


get_communications = """
    SELECT * FROM web_data.communications;
"""

with sync_engine.connect() as conn:
    sessions_df = pd.read_sql(get_sessions, conn)
    communications_df = pd.read_sql(get_communications, conn)
    result_pd = pd.merge(
        sessions_df,
        communications_df,
        on=['visitor_id', 'site_id'],
        how='inner'
    )[['communication_id', 'campaign_id']]\
        .drop_duplicates()\
        .sort_values(by=['communication_id', 'campaign_id'])

    print(result_pd)