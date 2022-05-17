import pandas as pd
import requests


def _get_page_count_for_cgi_request(url) -> int:
    resp = requests.get(url)
    return resp.json()["page_count"]


def _get_page_count_for_api_request(url) -> int:
    resp = requests.get(url)
    return resp.json()["page_count"]


class OpenFoodFactsAPI:
    def __init__(self):
        self.api_path = "https://world.openfoodfacts.org/api/v2"
        self.cgi_path = "https://world.openfoodfacts.org/cgi/search.pl?"
        self.language_code = None

    def get_product_by_barcode(self, barcode: str, fields: list = None):
        """
        Request information for a specific product based on the barcode
        :param barcode: Barcode of product as a string
        :param fields: Optional. If defined only gets the fields listed
        :return: pd.Dataframe with fields as columns
        """
        if fields is None:
            resp = requests.get(f"{self.api_path}/search/code={barcode}")
        else:
            resp = requests.get(
                f"{self.api_path}/search/code={barcode}&fields={','.join(fields)}"
            )

        payload = resp.json()["products"]
        return pd.DataFrame(payload)

    def get_multiple_products_by_barcode(self, barcodes: list, fields: list = None):
        """
        Request information for multiple products at once based on their barcode
        :param barcodes: List with all the barcodes that should be looked up
        :param fields: Optional. If defined only gets fields listed
        :return: pd.Dataframe with fields as columns
        """
        barcode_string = ",".join(barcodes)
        return self.get_product_by_barcode(barcode_string, fields)

    def set_product_language_code(self, language_code: str = None):
        """
        When called
        :param language_code:
        :return:
        """
        if language_code is None:
            self.api_path = "https://world.openfoodfacts.org/api/2"
            self.cgi_path = "https://world.openfoodfacts.org/cgi/search.pl?"
            self.language_code = None
        else:
            self.api_path = f"https://{language_code}.openfoodfacts.org/api/2"
            self.cgi_path = f"https://{language_code}.openfoodfacts.org/cgi/search.pl?"
            self.language_code = language_code

    def get_current_language_code(self):
        """
        Returns the current language search settings
        :return: Language Code or Worldwide
        """
        if self.language_code is None:
            return "Worldwide"
        else:
            return self.language_code

    def get_all_products(self, fields: list = None) -> pd.DataFrame:
        """
        Get all known products
        :param fields: Optional. If defined only gets fields listed
        :return: Dataframe with defined fields as columns and products in rows
        """
        page_count = _get_page_count_for_api_request(
            f"{self.api_path}/search/?fields=page_count"
        )
        out = pd.DataFrame()
        for page in range(1, page_count + 1):
            if fields is None:
                url = f"{self.api_path}/search?page={page}"
            else:
                url = f"{self.api_path}/search?page={page}&fields={','.join(fields)}"

            resp = requests.get(url)
            products_on_page = resp.json()["products"]
            app = pd.DataFrame(products_on_page)
            out = pd.concat([out, app])
        return out

    def get_products_by_category(
        self, category: str, fields: list = None
    ) -> pd.DataFrame:
        """
        Get all products for a category
        :param category: Category name in Open Food Facts
        :param fields: Optional. If defined only gets fields listed
        :return: Dataframe with defined fields as columns
        """
        out = pd.DataFrame()
        if fields is None:
            url = (
                f"{self.cgi_path}action=process&tagtype_0=categories&tag_contains_0=contains&tag_0="
                f"{category}&json=true"
            )
        else:
            url = (
                f"{self.cgi_path}action=process&tagtype_0=categories&tag_contains_0=contains&tag_0="
                f"{category}&fields={','.join(fields)}&json=true"
            )

        page_count = _get_page_count_for_cgi_request(url)

        for page in range(1, page_count + 1):
            resp = requests.get(f"{url}&page={page}")
            products_on_page = resp.json()["products"]
            app = pd.DataFrame(products_on_page)
            out = pd.concat([out, app])
        return out
