import pandas as pd
import re


def apply_filtering(data: pd.DataFrame, filters_expressions_tree: dict) -> pd.DataFrame:
    # if it's a unary expression
    if len(filters_expressions_tree) == 2:
        operator: str = filters_expressions_tree["type"]
        operand: dict = filters_expressions_tree["operand"]
        if operator == "not":
            removed_data = apply_filtering(data, operand)
            return pd.concat([data, removed_data]).drop_duplicates(keep=False)
    operator: str = filters_expressions_tree["type"]
    if operator == "or" or operator == "and":
        # if it's a binary expression
        left_operand: dict = filters_expressions_tree["left"]
        right_operand: dict = filters_expressions_tree["right"]
        # data after applying left operand
        left = apply_filtering(data, left_operand)
        right = apply_filtering(data, right_operand)

        if operator == "or":
            data = pd.concat([left, right])
        else:
            data = pd.merge(left, right)
        return data[~data.index.duplicated(keep="first")]

    left_operand: str = filters_expressions_tree["left"]

    right_operand = filters_expressions_tree["right"]
    # region get the value in the right operand and check if it is a int or float or string or its a column passed by name or number
    if type(right_operand) == str:
        if right_operand.startswith('"') and right_operand.endswith('"'):
            right_operand: str = right_operand[1:-1]
        elif right_operand.startswith("[") and right_operand.endswith("]"):
            column_number = int(right_operand[1:-1])
            right_operand: pd.DataFrame = data[data.columns[column_number]]
        else:
            right_operand: pd.DataFrame = data[right_operand]
    # endregion
    # get the column in the left operand and check if its passed by name or number
    if left_operand.startswith("[") and left_operand.endswith("]"):
        column_number = int(left_operand[1:-1])
        left_operand = data[data.columns[column_number]]
    else:
        left_operand = data[left_operand]

    if operator == "like":
        return data[
            [True if re.match(right_operand, str(x)) else False for x in left_operand]
        ]

    if operator == ">":
        return data[left_operand > right_operand]
    if operator == ">=":
        return data[left_operand >= right_operand]
    elif operator == "<":
        return data[left_operand < right_operand]
    elif operator == "<=":
        return data[left_operand <= right_operand]
    elif operator == "==":
        return data[left_operand == right_operand]
    elif operator == "!=":
        return data[left_operand != right_operand]

    return data


# def __get_source_type(data_source:str) -> str:
#     if data_source == 'CONSOLE':
#         return 'CONSOL'
#     elif re.search(r'.*\.csv(\.zip)?', data_source):
#         return 'CSV'
#     elif re.search(r'.*\.db/\w+', data_source):
#         return 'SQLITE'
#     elif re.search(r'Data Source.*', data_source):
#         return 'MSSQL'
#     elif re.search(r'.*\.html', data_source):
#         return 'HTML'
#     elif re.search(r'.*\.json', data_source):
#         return 'JSON'
#     elif re.search(r'.*\.xml', data_source):
#         return 'XML'
#     elif re.search( r'(.+\.xlsx)| (.+\.xls) | (.+\.xlsm)| (.+\.xlsb)| (.+\.odf)| (.+\.ods)| (.+\.odt)', data_source):
#         return 'EXCEL'
