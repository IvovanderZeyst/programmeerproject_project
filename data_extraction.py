import json
import requests
import os
import pandas as pd
from loguru import logger
import pprint
import uuid
from typing import Dict

"""
This class handles all functionalities that (support in) the extraction 
of data from the 99spokes API
"""
class DataExtraction:
    """
    """
    def __init__(self, base_url: str, query_params: Dict[str, str], username: str, password: str):
        self.url = base_url
        self.query_params = query_params
        self.username = username
        self.password = password
        self.data = {}
        self.models_table = None
        self.prices_table = None
        self.price_history_table = None
        self.analysis_table = None
        self.cursor_count = 0

    def build_full_url(self):
        query_string = ""
        for k, v in self.query_params.items():
            query_string += f"{k}={v}&"
        query_string = query_string[:-1]
        return f"{self.url}?{query_string}"

    def get_response(self):
        full_url = self.build_full_url()
        response = requests.get(full_url, auth=(username, password))
        return response

    def set_cursor(self, cursor: str, increase_cursor_count: bool = False):
        self.query_params['cursor'] = cursor
        if increase_cursor_count:
            self.cursor_count += 1

    def decrease_limit(self, amount: int = 1):
        new_limit = int(self.query_params.get('limit', 0)) - amount
        if new_limit <= 0:
            raise ValueError(f"Limit cannot be <= 0. Limit was '{new_limit}'")
        self.query_params['limit'] = str(new_limit)

    def fetch_data(self, parse_to_self: bool = False, keep_trying_if_500_error: bool = False):
        response = self.get_response()
        if keep_trying_if_500_error and response.status_code == 500:
            self.decrease_limit()
            self.fetch_data(parse_to_self, keep_trying_if_500_error)
        data = json.loads(response.text)
        if parse_to_self:
            _ = self.parse_data(data)
        return data

    def parse_data(self, data):

        models_list = []
        prices_list = []
        price_history_list = []
        analysis_list = []

        for i, row in enumerate(data["items"]):
            models_list.append({
                "id": str(uuid.uuid4()),
                "model_id": row.get('model_id', None),
                "url": row.get('url', None),
                "makerId": row.get('makerId', None),
                "maker": row.get('maker', None),
                "year": row.get('year', None),
                "model": row.get('model', None),
                "family": row.get('family', None),
                "category": row.get('category', None),
                "subcategory": row.get('subcategory', None),
                "buildKind": row.get('buildKind', None),
                "isFrameset": row.get('isFrameset', None),
                "isEbike": row.get('isEbike', None),
                "gender": row.get('gender', None)})

            for j, row_j in enumerate(row["prices"]):
                prices_list.append({
                    "id": str(uuid.uuid4()),
                    "model_id": row.get('model_id', None),
                    "currency": row_j.get("currency", None),
                    "amount": row_j.get("amount", None),
                    "discountedAmount": row_j.get("discountedAmount", None),
                    "discount": row_j.get("discount", None)})

            for k, row_k in enumerate(row["priceHistory"]):

                for l, row_l in enumerate(row_k["history"]):
                    price_history_list.append({
                        "id": str(uuid.uuid4()),
                        "model_id": row.get('model_id', None),
                        "currency": row_k.get("currency", None),
                        "date": row_l.get("date", None),
                        "amount": row_l.get("amount", None),
                        "change": row_l.get("change", None),
                        "discountedAmount": row_l.get("discountedAmount", None),
                        "discount": row_l.get("discount", None)})

            for m, row_m in enumerate(row["analysis"]):

                if m % 2 == 0:

                    try:
                        analysis_list.append({
                            "id": str(uuid.uuid4()), "model_id": row.get('model_id', None),
                            "specLevel_value": row.get("analysis", {'speclevel': {}}).get("specLevel", {'value': None}).get('value'),
                            "groupKey": row.get("analysis", {'speclevel': {}}).get("specLevel", {'groupKey': None}).get('groupKey'),
                            "frame": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'frame': None}).get("frame"),
                            "wheels": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'wheels': None}).get("wheels"),
                            "brakes": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'brakes': None}).get("brakes"),
                            "groupset": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'groupset': None}).get("groupset"),
                            "shifting": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'shifting': None}).get("shifting"),
                            "seatpost": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'seatpost': None}).get("seatpost"),
                            "forkMaterial": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'forkMaterial': None}).get("forkMaterial"),
                            "forkrank": row.get("analysis", {'speclevel': {}}).get("specLevel", {'explanation': {}}).get("explanation", {'forkrank': None}).get("forkrank"),
                            "valueProp": row.get("analysis", {'valueProp': {}}).get("valueProp", {'value': None}).get('value')
                        })
                    except Exception as e:
                        logger.exception(e)
                        logger.info(row["analysis"])
                        raise e

        self.models_table = pd.DataFrame.from_dict(models_list)
        self.prices_table = pd.DataFrame.from_dict(prices_list)
        self.price_history_table = pd.DataFrame.from_dict(price_history_list)
        self.analysis_table = pd.DataFrame.from_dict(analysis_list)

        return self.models_table, self.prices_table, self.price_history_table, self.analysis_table

    def print_dataframes(self):
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)

        for dataframe in [self.models_table, self.prices_table, self.price_history_table, self.analysis_table]:
            print(dataframe)

    def dataframes_to_parquet(self):
        self.models_table.to_parquet(f"data_files/{self.cursor_count}-models-{str(uuid.uuid4())}.parquet")
        self.prices_table.to_parquet(f"data_files/{self.cursor_count}-prices-{str(uuid.uuid4())}.parquet")
        self.price_history_table.to_parquet(f"data_files/{self.cursor_count}-price_history-{str(uuid.uuid4())}.parquet")
        self.analysis_table.to_parquet(f"data_files/{self.cursor_count}-analysis_{str(uuid.uuid4())}.parquet")

    def dataframes_to_csv(self):
        if any([self.models_table is None, self.prices_table is None, self.price_history_table is None, self.analysis_table is None]):
            raise ValueError("One or more tables are not loaded (correctly) into the DataExtraction object. Run the fetch_data() function with parameter parse_to_self=True to resolve")
        uuid_str = str(uuid.uuid4())
        self.models_table.to_csv(f"data_files/{self.cursor_count}-models-{uuid_str}.csv")
        self.prices_table.to_csv(f"data_files/{self.cursor_count}-prices"
                                 f"-{uuid_str}.csv")
        self.price_history_table.to_csv(f"data_files/{self.cursor_count}-price_history-{uuid_str}.csv")
        self.analysis_table.to_csv(f"data_files/{self.cursor_count}-analysis_{uuid_str}.csv")

if __name__ == '__main__':

    username = os.environ.get("USERNAME")
    password = os.environ.get("PASSWORD")

    cursor = "start"
    limit = os.environ.get('limit', 100)

    query_params = {
        'isFrameset': 'false',
        'cursor': cursor,
        'include': 'analysis,prices,suspension,priceHistory',
        'limit': limit
    }

    extraction = DataExtraction(base_url="https://api.99spokes.com/v1/bikes", query_params=query_params, username=username, password=password)

    write_result = True

    while cursor is not None:
        data = extraction.fetch_data(parse_to_self=write_result, keep_trying_if_500_error=True)
        cursor = data["nextCursor"]
        extraction.set_cursor(cursor, increase_cursor_count=True)
        logger.info(f"{cursor = }")
        if write_result:
            extraction.dataframes_to_csv()

    logger.info(f"Finished extracting API with '{extraction.cursor_count}' pages of '{limit}' items")