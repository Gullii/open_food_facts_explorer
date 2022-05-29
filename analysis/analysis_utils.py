import pandas as pd


def get_average_kcal_by_country(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data[["code", "kcal_per100"]].groupby(by="code").mean
