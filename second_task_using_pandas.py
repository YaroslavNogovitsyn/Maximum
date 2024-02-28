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

    merged_data = communications_df.merge(sessions_df, on=['visitor_id', 'site_id'], suffixes=('_c', '_s'))
    merged_data = merged_data[merged_data['date_time_c'] >= merged_data['date_time_s']]
    cte = merged_data.groupby(['visitor_id', 'site_id']).agg(
        visitor_session_id=('visitor_session_id', 'max'),
        row_n=('date_time_s', 'nunique'),
        session_date_time=('date_time_s', 'max')
    ).reset_index()
    result = sessions_df.merge(communications_df, on=['visitor_id', 'site_id']).merge(cte, on='visitor_session_id')
    result = result.rename(
        columns=
        {
            'site_id_x': 'site_id',
            'visitor_id_x': 'visitor_id',
            'date_time_x': 'communication_date_time'
        }
    )

    print(result[['communication_id', 'site_id', 'visitor_id', 'communication_date_time', 'visitor_session_id',
                  'session_date_time', 'campaign_id', 'row_n']])
