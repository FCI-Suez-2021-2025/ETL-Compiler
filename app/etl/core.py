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
    
    # grouping 1
    # if criteria["GROUP"]:
    #     group_columns: list[str] = criteria["GROUP"]

    #     # Check if group columns are column numbers or names
    #     is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")
    #     group_column_names = [
    #         data.columns[int(column[1:-1])] if is_column_number(column) else column
    #         for column in group_columns
    #     ]

    #     # Perform grouping
    #     # Default aggregation: count rows in each group
    #     data = data.groupby(group_column_names).size().reset_index(name='count')


    # grouping 2
    # if criteria["GROUP"]:
    #     group_columns: list[str] = criteria["GROUP"]

    #     # Normalize data columns for consistent matching
    #     data.columns = data.columns.str.strip().str.lower()

    #     # Function to detect if a column is specified by index
    #     is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")

    #     # Resolve column names from group_columns (numbers or names)
    #     try:
    #         group_column_names = [
    #             data.columns[int(column[1:-1])] if is_column_number(column) else column.strip().lower()
    #             for column in group_columns
    #         ]
    #     except IndexError:
    #         raise ValueError(f"One of the grouping columns {group_columns} is invalid. Available columns: {list(data.columns)}")

    #     print("Grouping by columns:", group_column_names)  # Debugging step

    #     # Perform grouping - Use default aggregation (row count per group)
    #     grouped_data = data.groupby(group_column_names, as_index=False).size()
    #     grouped_data.rename(columns={'size': 'count'}, inplace=True)
    #     data = grouped_data


    # grouping 3
    # if criteria["GROUP"]:
    #     group_columns: list[str] = criteria["GROUP"]

    #     # Normalize column names for consistency
    #     data.columns = data.columns.str.strip().str.lower()

    #     # Check if group columns are numbers or names
    #     is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")
    #     group_column_names = [
    #         data.columns[int(column[1:-1])] if is_column_number(column) else column.strip().lower()
    #         for column in group_columns
    #     ]

    #     # Group by the specified columns
    #     grouped = data.groupby(group_column_names, as_index=False)

    #     # Add a 'count' column to indicate the group sizes
    #     grouped_data = grouped.size().reset_index(name="count")

    #     # Merge the grouped result with the selected columns
    #     if criteria["COLUMNS"] != "__all__":
    #         selected_columns = [
    #             data.columns[int(col[1:-1])] if is_column_number(col) else col.strip().lower()
    #             for col in criteria["COLUMNS"]
    #         ]
    #         # Retain only the selected columns and the 'count' column
    #         grouped_data = grouped_data[selected_columns + ["count"]]

    #     # Update the data to reflect the grouped result
    #     data = grouped_data



    # grouping 4
    # if criteria["GROUP"]:
    #     group_columns: list[str] = criteria["GROUP"]

    #     # Normalize column names for consistency
    #     data.columns = data.columns.str.strip().str.lower()

    #     # Check if group columns are numbers or names
    #     is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")
    #     group_column_names = [
    #         data.columns[int(column[1:-1])] if is_column_number(column) else column.strip().lower()
    #         for column in group_columns
    #     ]

    #     # Group by the specified columns and calculate the count
    #     grouped = data.groupby(group_column_names).size().reset_index(name="count")

    #     # If `COLUMNS` specifies additional columns beyond `GROUP`, merge them
    #     if criteria["COLUMNS"] != "__all__":
    #         selected_columns = [
    #             data.columns[int(col[1:-1])] if is_column_number(col) else col.strip().lower()
    #             for col in criteria["COLUMNS"]
    #         ]

    #         # Ensure the selected columns include group-by columns
    #         merged_columns = set(selected_columns).union(set(group_column_names))

    #         # Create a reduced DataFrame with only the required columns
    #         reduced_data = data.loc[:, merged_columns]

    #         # Merge the grouped data with the reduced data to retain the selected columns
    #         grouped_data = grouped.merge(reduced_data, on=group_column_names, how="left")

    #         # Ensure only the specified columns in `COLUMNS` and `count` are retained
    #         grouped_data = grouped_data.loc[:, selected_columns + ["count"]]

    #     else:
    #         # If all columns are needed, retain group columns and count
    #         grouped_data = grouped

    #     # Update the data with the grouped results
    #     data = grouped_data


    # grouping 5
    # if criteria["GROUP"]:
    #     group_columns: list[str] = criteria["GROUP"]

    #     # Normalize column names for consistency
    #     data.columns = data.columns.str.strip().str.lower()

    #     # Check if group columns are numbers or names
    #     is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")
    #     group_column_names = [
    #         data.columns[int(column[1:-1])] if is_column_number(column) else column.strip().lower()
    #         for column in group_columns
    #     ]

    #     # Group by the specified columns and calculate the count
    #     grouped = data.groupby(group_column_names).size().reset_index(name="count")

    #     # If `COLUMNS` specifies additional columns beyond `GROUP`, merge them
    #     if criteria["COLUMNS"] != "__all__":
    #         selected_columns = [
    #             data.columns[int(col[1:-1])] if is_column_number(col) else col.strip().lower()
    #             for col in criteria["COLUMNS"]
    #         ]

    #         # Ensure the selected columns include group-by columns
    #         merged_columns = list(set(selected_columns).union(set(group_column_names)))  # Convert set to list

    #         # Create a reduced DataFrame with only the required columns
    #         reduced_data = data.loc[:, merged_columns]

    #         # Merge the grouped data with the reduced data to retain the selected columns
    #         grouped_data = grouped.merge(reduced_data, on=group_column_names, how="left")

    #         # Ensure only the specified columns in `COLUMNS` and `count` are retained
    #         grouped_data = grouped_data.loc[:, selected_columns + ["count"]]

    #     else:
    #         # If all columns are needed, retain group columns and count
    #         grouped_data = grouped

    #     # Update the data with the grouped results
    #     data = grouped_data


    # grouping 6 working but there is no count column added
    # if criteria["GROUP"]:
    #     group_columns: list[str] = criteria["GROUP"]

    #     # Normalize column names for consistency
    #     data.columns = data.columns.str.strip().str.lower()

    #     # Check if group columns are numbers or names
    #     is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")
    #     group_column_names = [
    #         data.columns[int(column[1:-1])] if is_column_number(column) else column.strip().lower()
    #         for column in group_columns
    #     ]

    #     # Perform grouping and aggregate
    #     grouped = (
    #         data.groupby(group_column_names)
    #         .agg({col: "first" for col in data.columns if col not in group_column_names})  # Take the first value of other columns
    #         .reset_index()
    #     )

    #     # Add a count column for the number of rows in each group
    #     grouped["count"] = data.groupby(group_column_names).size().values

    #     # If `COLUMNS` is specified, select only those columns
    #     if criteria["COLUMNS"] != "__all__":
    #         selected_columns = [
    #             data.columns[int(col[1:-1])] if is_column_number(col) else col.strip().lower()
    #             for col in criteria["COLUMNS"]
    #         ]
    #         grouped = grouped.loc[:, selected_columns + ["count"]]

    #     # Update the data with the grouped results
    #     data = grouped



    # grouping 7
    if criteria["GROUP"]:
        group_columns: list[str] = criteria["GROUP"]

        # Normalize column names for consistency
        data.columns = data.columns.str.strip().str.lower()

        # Check if group columns are numbers or names
        is_column_number: Callable[[str], bool] = lambda x: x.startswith("[") and x.endswith("]")
        group_column_names = [
            data.columns[int(column[1:-1])] if is_column_number(column) else column.strip().lower()
            for column in group_columns
        ]

        # Perform grouping
        grouped = data.groupby(group_column_names)

        # Aggregate: First value for other columns (keep one value per group), and calculate the count
        aggregated = grouped.agg({col: "first" for col in data.columns if col not in group_column_names}).reset_index()
        
        # Add the count column explicitly
        aggregated['count'] = grouped.size().reset_index(drop=True)

        # If `COLUMNS` is specified, select only those columns
        if criteria["COLUMNS"] != "__all__":
            selected_columns = [
                data.columns[int(col[1:-1])] if is_column_number(col) else col.strip().lower()
                for col in criteria["COLUMNS"]
            ]
            # Ensure count column is included in the output
            selected_columns.append('count')
            aggregated = aggregated[selected_columns]

        # Update the data with the grouped results
        data = aggregated





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
