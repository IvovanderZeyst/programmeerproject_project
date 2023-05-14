"""This code defines a class named DataExtraction that extracts and parses data
   from an API response.

The class has several methods such as __init__, __build_full_url, get_response,
    set_cursor, decrease_limit, fetch_data,
and parse_data. The __init__ method initializes a new instance of the class
    with parameters such as
base_url, query_params, username, and password.

The __build_full_url method builds the full URL for the API request,
    including query parameters.
The get_response method sends a GET request to the API and returns the response.
The set_cursor method sets the cursor parameter in the API request to the
    specified value.
The decrease_limit method decreases the limit parameter in the API request by
    the specified amount.
The fetch_data method sends a GET request to the API and returns the response
    data as a Python object.
The parse_data method parses bike data from the provided dictionary.
"""
import json
import uuid
from typing import Dict

import pandas as pd
import requests
from loguru import logger

#from modules \
import data_extraction_constants
#from modules \
from data_extraction_dataframe_parser import *


class DataExtraction:
    """A class to extract and parse data from an API response."""

    def __init__(
        self, base_url: str, query_params: Dict[str, str], username: str,
            password: str):
        """Initializes a new DataExtraction instance.

        Args:
        - base_url (str): The base URL of the API endpoint.
        - query_params (Dict[str, str]): A dictionary of query parameters to be
            included in the API request.
        - username (str): The username for API authentication.
        - password (str): The password for API authentication.
        """
        self.url = base_url
        self.query_params = query_params
        self.username = username
        self.password = password
        self.data = {}

        self.models_table = None
        self.prices_table = None
        self.price_history_table = None
        self.analysis_table = None

        self.models_mapping = data_extraction_constants.MODELS_TABLE_MAPPING
        self.prices_mapping = data_extraction_constants.PRICES_TABLE_MAPPING
        self.price_history_mapping = \
            data_extraction_constants.PRICE_HISTORY_TABLE_MAPPING
        self.analysis_mapping = data_extraction_constants.ANALYSIS_TABLE_MAPPING

        self.cursor_count = 0

        print(self.username)
        print(self.password)

    def build_full_url(self):
        """Builds the full URL for the API request, including query parameters.
        """
        query_string = ""
        for k, v in self.query_params.items():
            query_string += f"{k}={v}&"
        query_string = query_string[:-1]
        return f"{self.url}?{query_string}"

    def get_response(self):
        """Sends a GET request to the API and returns the response."""
        full_url = self.build_full_url()
        response = requests.get(
            full_url, auth=(self.username, self.password), timeout=60
        )
        return response

    def set_cursor(self, cursor: str, increase_cursor_count: bool = False):
        """Sets the cursor parameter in the API request to the specified value.

        Args:
        - cursor (str): The value to set the cursor parameter to.
        - increase_cursor_count (bool): Whether to increment the cursor_count
            attribute by 1.
        """
        self.query_params["cursor"] = cursor
        if increase_cursor_count:
            self.cursor_count += 1

    def decrease_limit(self, amount: int = 1):
        """Decreases the limit parameter in the API request by the specified
            amount.

        Args:
        - amount (int): The amount to decrease the limit parameter by.
        """
        new_limit = int(self.query_params.get("limit", 0)) - amount
        if new_limit <= 0:
            raise ValueError(f"Limit cannot be <= 0. Limit was {new_limit!r}")
        self.query_params["limit"] = str(new_limit)

    def fetch_data(
        self, parse_to_self: bool = False,
            keep_trying_if_500_error: bool = False
    ):
        """Sends a GET request to the API and returns the response data as a
            Python object.

        Args:
        - parse_to_self (bool): Whether to parse the data to the DataExtraction
            instance.
        - keep_trying_if_500_error (bool): Whether to keep trying the request
            if a 500 error is encountered.

        Returns:
        - The response data as a Python object.
        """
        response = self.get_response()
        if keep_trying_if_500_error and response.status_code == 500:
            logger.warning("Encountered 500 error from API. Retrying "
                           "with decreased page size")
            self.decrease_limit()
            self.fetch_data(parse_to_self, keep_trying_if_500_error)
        data = json.loads(response.text)
        if parse_to_self:
            _ = self.parse_data(data)
        return data

    def apply_mapping(self):
        """Applies clean mapping to the relevant dataframes.

        This method applies the clean mapping specified for each of the
            relevant dataframes
        (analysis_table, models_table, price_history_table, and prices_table)
            and renames
        the columns as specified in the clean mapping.

        Returns:
            None
        """
        if not self.analysis_table.empty:
            self.analysis_table = apply_clean_mapping(
                dataframe=self.analysis_table,
                clean_mapping=self.analysis_mapping,
                rename_columns=True
            )

        if not self.models_table.empty:
            self.models_table = apply_clean_mapping(
                dataframe=self.models_table,
                clean_mapping=self.models_mapping,
                rename_columns=True
            )

        if not self.price_history_table.empty:
            self.price_history_table = apply_clean_mapping(
                dataframe=self.price_history_table,
                clean_mapping=self.price_history_mapping,
                rename_columns=True
            )

        if not self.prices_table.empty:
            self.prices_table = apply_clean_mapping(
                dataframe=self.prices_table,
                clean_mapping=self.prices_mapping,
                rename_columns=True
            )
        pass

    def parse_data(self, data):
        """Parse bike data from the provided dictionary.

        Args:
            data: A dictionary containing bike data.

        Returns:
            A tuple containing four dataframes: models_table, prices_table,
                price_history_table, and analysis_table.

        Raises:
            Exception: If any exception is raised here, this needs to be fixed
                in any case.
        """
        models_list = []
        prices_list = []
        price_history_list = []
        analysis_list = []

        for _i, row in enumerate(data["items"]):
            models_list.append(
                {
                    "id": str(uuid.uuid4()),
                    "model_id": row.get("id", None),
                    "url": row.get("url", None),
                    "makerId": row.get("makerId", None),
                    "maker": row.get("maker", None),
                    "year": row.get("year", None),
                    "model": row.get("model", None),
                    "family": row.get("family", None),
                    "category": row.get("category", None),
                    "subcategory": row.get("subcategory", None),
                    "buildKind": row.get("buildKind", None),
                    "isFrameset": row.get("isFrameset", None),
                    "isEbike": row.get("isEbike", None),
                    "gender": row.get("gender", None),
                }
            )

            for _j, row_j in enumerate(row["prices"]):
                prices_list.append(
                    {
                        "id": str(uuid.uuid4()),
                        "model_id": row.get("id", None),
                        "currency": row_j.get("currency", None),
                        "amount": row_j.get("amount", None),
                        "discountedAmount": row_j.get("discountedAmount", None),
                        "discount": row_j.get("discount", None),
                    }
                )

            for _k, row_k in enumerate(row["priceHistory"]):
                for _l, row_l in enumerate(row_k["history"]):
                    price_history_list.append(
                        {
                            "id": str(uuid.uuid4()),
                            "model_id": row.get("id", None),
                            "currency": row_k.get("currency", None),
                            "date": row_l.get("date", None),
                            "amount": row_l.get("amount", None),
                            "change": row_l.get("change", None),
                            "discountedAmount": row_l.get("discountedAmount",
                                                          None),
                            "discount": row_l.get("discount", None),
                        }
                    )

            for _m, _row_m in enumerate(row["analysis"]):
                if _m % 2 == 0:
                    try:
                        analysis_list.append(
                            {
                                "id": str(uuid.uuid4()),
                                "model_id": row.get("id", None),
                                "specLevel_value": row.get(
                                    "analysis", {"speclevel": {}}
                                )
                                .get("specLevel", {"value": None})
                                .get("value"),
                                "groupKey": row.get("analysis", {"speclevel":
                                                                     {}})
                                .get("specLevel", {"groupKey": None})
                                .get("groupKey"),
                                "frame": row.get("analysis", {"speclevel": {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"frame": None})
                                .get("frame"),
                                "wheels": row.get("analysis", {"speclevel": {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"wheels": None})
                                .get("wheels"),
                                "brakes": row.get("analysis", {"speclevel": {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"brakes": None})
                                .get("brakes"),
                                "groupset": row.get("analysis", {"speclevel":
                                                                     {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"groupset": None})
                                .get("groupset"),
                                "shifting": row.get("analysis", {"speclevel":
                                                                     {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"shifting": None})
                                .get("shifting"),
                                "seatpost": row.get("analysis", {"speclevel":
                                                                     {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"seatpost": None})
                                .get("seatpost"),
                                "forkMaterial": row.get("analysis",
                                                        {"speclevel": {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"forkMaterial": None})
                                .get("forkMaterial"),
                                "forkrank": row.get("analysis", {"speclevel":
                                                                     {}})
                                .get("specLevel", {"explanation": {}})
                                .get("explanation", {"forkrank": None})
                                .get("forkrank"),
                                "valueProp": row.get("analysis", {"valueProp":
                                                                      {}})
                                .get("valueProp", {"value": None})
                                .get("value"),
                            }
                        )
                    except Exception as e:
                        logger.exception(e)
                        logger.info(row["analysis"])
                        raise e

        self.models_table = pd.DataFrame.from_dict(models_list)
        self.prices_table = pd.DataFrame.from_dict(prices_list)
        self.price_history_table = pd.DataFrame.from_dict(price_history_list)
        self.analysis_table = pd.DataFrame.from_dict(analysis_list)

        self.apply_mapping()

        return (
            self.models_table,
            self.prices_table,
            self.price_history_table,
            self.analysis_table,
        )

    def print_dataframes(self):
        """Prints the dataframes in a formatted manner by setting display
            options.

        Args:
            None

        """
        # Set pandas display options
        pd.set_option("display.max_rows", 500)
        pd.set_option("display.max_columns", 500)
        pd.set_option("display.width", 1000)

        # Print each dataframe
        for dataframe in [
            self.models_table,
            self.prices_table,
            self.price_history_table,
            self.analysis_table,
        ]:
            print(dataframe)
        pass

    def dataframes_to_parquet(self):
        """Converts dataframes to Parquet format and saves them in respective
            folders.

        Args:
            None

        Raises:
            ValueError: If one or more tables are not loaded (correctly) into
                            the DataExtraction object.
                        Run the fetch_data() function with parameter
                            parse_to_self=True to resolve.

        """
        # Check if all tables are loaded correctly, otherwise raise ValueError
        if any(
            [
                self.models_table is None,
                self.prices_table is None,
                self.price_history_table is None,
                self.analysis_table is None,
            ]
        ):
            raise ValueError(
                "One or more tables are not loaded (correctly) into the "
                "DataExtraction object."
                "Run the fetch_data() function with parameter "
                "parse_to_self=True to resolve"
            )

        # Convert each dataframe to Parquet format and save in respective folders
        uuid_str = str(uuid.uuid4())
        self.models_table.to_parquet(
            f"../data_files/models/{self.cursor_count}-models"
            f"-{uuid_str}.parquet"
        )
        self.prices_table.to_parquet(
            f"../data_files/prices/{self.cursor_count}-prices"
            f"-{uuid_str}.parquet"
        )
        self.price_history_table.to_parquet(
            f"../data_files/price_history/{self.cursor_count}-price_history"
            f"-{uuid_str}.parquet"
        )
        self.analysis_table.to_parquet(
            f"../data_files/analysis/{self.cursor_count}-analysis"
            f"_{uuid_str}.parquet"
        )
        pass

    def dataframes_to_csv(self):
        """Converts dataframes to CSV format and saves them in respective
            folders.

        Args:
            None

        Raises:
            ValueError: If one or more tables are not loaded (correctly) into
                            the DataExtraction object.
                        Run the fetch_data() function with parameter
                            parse_to_self=True to resolve.
        """
        # Check if all tables are loaded correctly, otherwise raise ValueError
        if any(
            [
                self.models_table is None,
                self.prices_table is None,
                self.price_history_table is None,
                self.analysis_table is None,
            ]
        ):
            raise ValueError(
                "One or more tables are not loaded (correctly) into the "
                "DataExtraction object."
                "Run the fetch_data() function with parameter parse_to_self"
                "=True to resolve"
            )

        # Convert each dataframe to CSV format and save in respective folders
        uuid_str = str(uuid.uuid4())
        self.models_table.to_csv(
            f"../data_files/models/{self.cursor_count}-models-{uuid_str}.csv",
            index=False
        )
        self.prices_table.to_csv(
            f"../data_files/prices/{self.cursor_count}-prices-{uuid_str}.csv",
            index=False
        )
        self.price_history_table.to_csv(
            f"../data_files/price_history/{self.cursor_count}-price_history"
            f"-{uuid_str}.csv", index=False
        )
        self.analysis_table.to_csv(
            f"../data_files/analysis/{self.cursor_count}-analysis"
            f"_{uuid_str}.csv", index=False
        )
        pass

    # TODO Implement this later
    """
    data to postgres
    build incremental mechanism
    """