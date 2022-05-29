import logging

import pandas as pd

from typing import Dict

from off_api import OpenFoodFactsAPI
from utils.country_codes import ISO3166_WORLD


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


def gather_data_for_countries(
    off: OpenFoodFactsAPI,
    fields: list = None,
    country_dict: Dict[str, str] = ISO3166_WORLD,
):
    """
    Gets all products for each existing country code from an OpenFoodFactsAPI and writes the result to a Dataframe
    :param off: initialized OpenFoodFactsApi Object
    :param fields: Optional list to limit fields to be queried from Open Food Facts
    :param country_dict: Optional way to specify a dict to be used as lookup for country codes. If not specified use
    list of all countries
    :return: Dataframe with specified fields
    """
    full_list = pd.DataFrame()
    failed = []
    for country in country_dict:
        print(f"getting products for country {ISO3166_WORLD.get(country)}")
        try:
            prods = get_all_products_from_country_code(off, country, fields)
            full_list = pd.concat([full_list, prods])
        except TimeoutError as toe:
            logging.warning(f"Failed for {country} with Timeout")
            failed.append(country)
        except Exception as e:
            logging.warning(f"Failed for {country} with unknown error {e}")
            failed.append(country)

    full_list.reset_index(inplace=True, drop=True)
    full_list = full_list.loc[full_list.kcal_per100 != 0]
    if len(failed) > 0:
        logging.warning(
            f"Failed to fetch following countries {','.join(f for f in failed)}"
        )
    return full_list
