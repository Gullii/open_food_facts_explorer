import pandas as pd

from off_api import OpenFoodFactsAPI
from utils.extract_utils import gather_data_for_countries
from os.path import exists
from utils.country_codes import ISO3166_EUROPE
import datetime as dt

if __name__ == "__main__":
    off = OpenFoodFactsAPI()
    fields = ["code", "product_name", "nutriments", "countries_tags"]

    if exists("full_list.csv"):
        ll = pd.read_csv("full_list.csv")
    else:
        # Get Data for all european countries
        df = gather_data_for_countries(off, fields, ISO3166_EUROPE)
        df.to_csv(f"off_full_list-{dt.datetime.now().date()}.csv")
