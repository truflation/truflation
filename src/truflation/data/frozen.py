import pandas as pd
import dateparser
from typing import Optional
from icecream import ic

def get_frozen_values(
        data_frame: pd.DataFrame,
        keys: list,
        frozen_date_string: Optional[str]
) -> pd.DataFrame:
    indexes = ['date'] + keys
    data_frame = data_frame.reset_index()
    data_frame['date'] = pd.to_datetime(data_frame['date'])
    data_frame['created_at'] = pd.to_datetime(data_frame['created_at'])
    data_frame = data_frame.sort_values(['date', 'created_at'])
    result = data_frame.groupby(indexes).first().drop(columns=['index'])
    ic(result)
    if frozen_date_string is not None:
        frozen_date = dateparser.parse(frozen_date_string)
        filtered_df = data_frame[data_frame['created_at'] <= frozen_date]
        grouped_df = filtered_df.groupby(indexes).last().drop(
            columns=['index']
        )
        ic(grouped_df)
        result = result.merge(
            grouped_df,
            left_index=True,
            right_index=True,
            how='left', suffixes=('', '_y')
        )
        for col in data_frame.columns:
            if col.endswith('_y'):
                result[col[:-2]] = result[col].fillna(result[col[:-2]])
        result = result.drop([
            col for col in result.columns if col.endswith('_y')
        ], axis=1)

    result = result.reset_index()
    result['date'] = result['date'].dt.strftime('%Y-%m-%d')
    ic(result)
    return result


