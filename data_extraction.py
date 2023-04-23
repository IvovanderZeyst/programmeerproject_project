import json
import requests
import os
import pandas as pd
from loguru import logger
import pprint
import uuid


class DataExtraction:
    """

    """
    def __init__(self, query, username, password):
        self.url = "https://api.99spokes.com/v1/sheets/bikes/Bikes?include=*"
        self.query = query
        self.username = username
        self.password = password
        self.response = self.get_response()
        self.data = self.fetch_data()
        self.data_depth = self.data_levels(self.data)
        self.data_frames = self.parse_data()

    def get_response(self):

        response = requests.get(self.query, auth=(username, password))

        return response

    def connection_check(self):

        print(f"response code is {self.response.status_code}")

    def fetch_data(self):

        data = json.loads(self.response.text)

        return data

    def print_data(self):
        """
        :return:
        """
        pp = pprint.PrettyPrinter(indent=4)

        logger.info(f"\n{pp.pprint(self.data)}")

    def data_levels(self, dictionary, level=0):

        if isinstance(dictionary, dict):

            for key, value in dictionary.items():

                if isinstance(value, dict):

                    yield key, None, level

                    yield from self.data_levels(value, level=level + 1)

                elif isinstance(value, list):

                    yield key, None, level

                    for item in value:
                        yield from self.data_levels(item, level=level + 1)

                else:
                    yield key, value, level

    def print_data_levels(self):
        for key, value, level in self.data_depth:
            print(f"{'- ' * level}{key}: {value}")


    def parse_data(self):

        models_list = []
        prices_list = []
        price_history_list = []
        analysis_list = []

        for i, row in enumerate(self.data["items"]):
            models_list.append({
                "id": row["id"], "url": row["url"], "makerId": row["makerId"], "maker": row["maker"],
                "year": row["year"], "model": row["model"], "family": row["family"], "category": row["category"],
                "subcategory": row["subcategory"], "buildKind": row["buildKind"], "isFrameset": row["isFrameset"],
                "isEbike": row["isEbike"], "gender": row["gender"]})

            for j, row_j in enumerate(row["prices"]):
                prices_list.append({
                    "id": row["id"], "currency": row_j["currency"], "amount": row_j["amount"],
                    "discountedAmount": row_j["discountedAmount"], "discount": row_j["discount"]})

            for k, row_k in enumerate(row["priceHistory"]):

                for l, row_l in enumerate(row_k["history"]):

                    price_history_list.append({
                        "id": row["id"], "currency": row_k["currency"], "date": row_l["date"],
                        "amount": row_l["amount"], "change": row_l["change"],
                        "discountedAmount": row_l["discountedAmount"],
                        "discount": row_l["discount"]})

            for m, row_m in enumerate(row["analysis"]):

                if m % 2 == 0:

                    try:
                        analysis_list.append({
                            "id": row["id"],
                            "specLevel_value": row["analysis"]["specLevel"]["value"],
                            "groupKey": row["analysis"]["specLevel"]["groupKey"],
                            "frame": row["analysis"]["specLevel"]["explanation"]["frame"],
                            "wheels": row["analysis"]["specLevel"]["explanation"]["wheels"],
                            "brakes": row["analysis"]["specLevel"]["explanation"]["brakes"],
                            "groupset": row["analysis"]["specLevel"]["explanation"]["groupset"],
                            "shifting": row["analysis"]["specLevel"]["explanation"]["shifting"],
                            "seatpost": row["analysis"]["specLevel"]["explanation"]["seatpost"],
                            "forkRank": row["analysis"]["specLevel"]["explanation"]["forkRank"],
                            "valueProp": row["analysis"]["valueProp"]["value"]
                        })
                    except:
                        analysis_list.append({
                            "id": row["id"],
                            "specLevel_value": row["analysis"]["specLevel"]["value"],
                            "groupKey": row["analysis"]["specLevel"]["groupKey"],
                            "frame": row["analysis"]["specLevel"]["explanation"]["frame"],
                            "wheels": row["analysis"]["specLevel"]["explanation"]["wheels"],
                            "brakes": row["analysis"]["specLevel"]["explanation"]["brakes"],
                            "groupset": row["analysis"]["specLevel"]["explanation"]["groupset"],
                            "shifting": row["analysis"]["specLevel"]["explanation"]["shifting"],
                            "seatpost": row["analysis"]["specLevel"]["explanation"]["seatpost"],
                            "forkRank": row["analysis"]["specLevel"]["explanation"]["forkRank"],
                            "valueProp": None
                        })

        models_df = pd.DataFrame.from_dict(models_list)
        prices_df = pd.DataFrame.from_dict(prices_list)
        price_history_df = pd.DataFrame.from_dict(price_history_list)
        analysis_df = pd.DataFrame.from_dict(analysis_list)

        all_dfs = [models_df, prices_df, price_history_df, analysis_df]

        return all_dfs

    def print_dataframes(self):

        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)

        for dataframe in self.data_frames:
            print(dataframe)

    def dataframes_to_parquet(self):

        for dataframe in self.data_frames:
            dataframe.to_parquet(f"{str(uuid.uuid4())}.parquet")


if __name__ == '__main__':

    query = "https://api.99spokes.com/v1/bikes?q=&isFrameset=false&cursor=start&include=analysis,prices,suspension,priceHistory&category=mountain&year=2023&subcategory=enduro&isEbike=false&limit=100"
    username = os.environ.get("USERNAME")
    password = os.environ.get("PASSWORD")

    e = DataExtraction(query=query, username=username, password=password)

    #print(e.data)

    #e.print_data_levels()

    #e.parse_data()

    #e.print_dataframes()

    e.dataframes_to_parquet()
