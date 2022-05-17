import pandas as pd
import datetime as dt
from off_api import OpenFoodFactsAPI
from utils.country_codes import ISO3166


def get_all_products_from_country_code(
    api: OpenFoodFactsAPI, ccode: str, fields: list
) -> pd.DataFrame:
    """
    :param api: initialized OpenFoodFactsApi Object
    :param ccode: Country code
    :param fields: Optional list to limit fields to be queried from Open Food Facts
    :return: Dataframe with all products from the specified country
    """
    api.set_product_language_code(ccode)
    prods = api.get_all_products(fields)
    prods = get_kcal_from_nutriments(prods)
    return prods


def get_kcal_from_nutriments(prod: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the kcal per 100gram from a result dataframe. It needs the column "nutri"
    :param prod: Dataframe with a column nutri that has a json-like structure
    :return: Dataframe with additional column
    """
    for i, v in prod.iterrows():
        nutri = v.nutriments
        # TODO not best practice
        try:
            kcal_per_100 = nutri["energy-kcal"]
        except KeyError as ke:
            kcal_per_100 = 0
        except TypeError as te:
            kcal_per_100 = 0
        except Exception as e:
            raise e
        prod.at[i, "kcal_per100"] = kcal_per_100
    return prod


def gather_data_for_all_countries(off: OpenFoodFactsAPI, fields: list = None):
    """
    Gets all products for each existing country code from an OpenFoodFactsAPI and writes the result to a csv
    :param off: initialized OpenFoodFactsApi Object
    :param fields: Optional list to limit fields to be queried from Open Food Facts
    :return:
    """
    full_list = pd.DataFrame()

    for country in ISO3166:
        print(f"getting products for country {ISO3166.get(country)}")
        try:
            prods = get_all_products_from_country_code(off, country, fields)
            full_list = pd.concat([full_list, prods])
        except Exception as e:
            raise e

    full_list.reset_index(inplace=True, drop=True)
    full_list = full_list.loc[full_list.kcal_per100 != 0]
    full_list.to_csv(
        f"off_full_list-{dt.datetime.now().date()}-{dt.datetime.now().time()}.csv"
    )
