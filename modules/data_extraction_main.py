"""This Python script extracts data from the 99 Spokes API and saves the results
in CSV files.

It uses the `DataExtraction` class from the `data_extraction` module to fetch
the data from the API
and the `parameters` module to access the API credentials and other
configuration parameters.

The script creates several output folders if they do not exist already, where
the CSV files will be saved.
The script uses the `os` module to interact with the file system.

The script imports several modules, including `json`, `uuid`, `typing`,
`pandas`, `requests`, `loguru`.

The script can be run from the command line with an optional limit on the
number of items to extract per request.
"""

import os

from loguru import logger

from data_extraction_class import DataExtraction

def create_output_folders():
    """Create several output folders where the CSV files will be saved if they
        do not exist already."""
    if not os.path.exists("../data_files/models/"):
        try:
            os.mkdir("../data_files/")
            os.mkdir("../data_files/models/")
        except:
            os.mkdir("../data_files/models/")
    if not os.path.exists("../data_files/prices/"):
        try:
            os.mkdir("../data_files/")
            os.mkdir("../data_files/prices/")
        except:
            os.mkdir("../data_files/prices/")
    if not os.path.exists("../data_files/price_history/"):
        try:
            os.mkdir("../data_files/")
            os.mkdir("../data_files/price_history/")
        except:
            os.mkdir("../data_files/price_history/")
    if not os.path.exists("../data_files/analysis/"):
        try:
            os.mkdir("../data_files/")
            os.mkdir("../data_files/analysis/")
        except:
            os.mkdir("../data_files/analysis/")


if __name__ == "__main__":
    cursor = "start"
    limit = os.environ.get("LIMIT", 100)

    query_params = {
        "isFrameset": "false",
        "cursor": cursor,
        "include": "analysis,prices,suspension,priceHistory",
        "limit": limit,
    }

    extraction = DataExtraction(
        base_url="https://api.99spokes.com/v1/bikes",
        query_params=query_params,
        username=os.environ.get("API_USERNAME"),
        password=os.environ.get("API_PASSWORD")
    )

    write_result = True
    if write_result:
        create_output_folders()

    while cursor is not None:
        try:
            data = extraction.fetch_data(
                parse_to_self=write_result, keep_trying_if_500_error=True
            )
            cursor = data["nextCursor"]
            extraction.set_cursor(cursor, increase_cursor_count=True)
            logger.info(f"Successfully retrieved page "
                        f"{extraction.cursor_count!r}")

            if write_result:
                extraction.dataframes_to_csv()

        except:
            logger.info("API error, continue")
            break

    logger.info(
        f"Finished extracting API with {extraction.cursor_count!r} pages"
        f" of {limit!r} items"
    )

    logger.info(
        f"Start writing to database"
    )

    extraction.csv_to_sql()

    logger.info(
        f"Ended writing to database"
    )
