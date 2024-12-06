import itertools
from typing import Callable
import pandas as pd
from app.etl.data.data_factories import (
    LoaderDataFactory,
    ExtractorDataFactory,
)
from app.etl.data.base_data_types import IExtractor, ILoader
from app.etl.helpers import apply_filtering


transformed_data = None


def extract(data_source_type: str, data_source_path: str) -> pd.DataFrame:
    data_extractor: IExtractor = ExtractorDataFactory.create(
        data_source_type, data_source_path
    )
    data: pd.DataFrame = data_extractor.extract()
    return data


def transform_select(data: pd.DataFrame, criteria: dict) -> pd.DataFrame:

    # filtering
    if criteria["FILTER"]:
        data = apply_filtering(data, criteria["FILTER"])

    # ordering
    if criteria["ORDER"]:
        tuple = criteria["ORDER"]
        column: str = tuple[0]
        sorting_way: str = tuple[1]

        # to handle if the column is passed by number not buy name
        if column.startswith("[") and column.endswith("]"):
            column_number = int(column[1:-1])
            column = data.columns[column_number]
        data = data.sort_values(column, ascending=sorting_way == "asc")

    # columns
    if criteria["COLUMNS"] != "__all__":
        columns: list[str] = criteria["COLUMNS"]
        is_column_number: Callable[[str], bool] = lambda x: x.startswith(
            "["
        ) and x.endswith("]")

        # get column names from column number
        column_names = [
            data.columns[int(column[1:-1])] if is_column_number(column) else column
            for column in columns
        ]

        # Select columns
        data = data[column_names]
    # distinct
    if criteria["DISTINCT"]:
        data = data.drop_duplicates()

    # limit
    if criteria["LIMIT_OR_TAIL"] != None:
        operator, number = criteria["LIMIT_OR_TAIL"]
        if number == 0:
            # empty data frame
            data = pd.DataFrame(columns=data.columns)
        elif operator == "limit":
            data = data[:number]
        else:
            data = data[-number:]

    global transformed_data
    transformed_data = data
    return data


def load(data: pd.DataFrame, source_type: str, data_destination: str):
    data_loader: ILoader = LoaderDataFactory.create(source_type, data_destination)
    data_loader.load(data)
