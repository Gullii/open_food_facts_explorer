import pandas as pd

from off_api import OpenFoodFactsAPI
from utils.extract_utils import gather_data_for_countries
from os.path import exists

if __name__ == "__main__":
    off = OpenFoodFactsAPI()
    fields = ["code", "product_name", "nutriments", "countries_tags"]

    if exists("full_list.csv"):
        ll = pd.read_csv("full_list.csv")
    else:
        gather_data_for_countries(off, fields)
