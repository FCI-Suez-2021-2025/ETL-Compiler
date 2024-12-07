from app.compiler import parser

# from returns.result import Success, Failure
from typing import Union
import traceback
from pandas import DataFrame

from app.core.errors import LexerError, ParserError, PythonExecutionError
from app.core.result_monad import Failure, Success


def compile_to_python(
    query: str,
) -> Union[
    Success[str],
    Failure[ParserError, None],
    Failure[LexerError, None],
    Failure[str, None],
]:
    """
    Compiles a SQL-like query string into equivalent Python code or returns an error if parsing fails.

    Args:
        query (str): The SQL-like query to be compiled. Examples:
            - Successful query: "SELECT * FROM {csv:data/players.csv} WHERE age > 30;"
            - Erroneous query: "SELECT * FROM WHERE age > 30"

    Returns:
        Union[Success[str], Failure[ParserError, None], Failure[LexerError, None], Failure[str, None]]:
            - Success[str]: Contains the generated Python code if the query is successfully parsed.
            - Failure[ParserError, None]: Contains a `ParserError` object if parsing the query fails due to syntax issues.
            - Failure[LexerError, None]: Contains a `LexerError` object if tokenizing the query fails.
            - Failure[str, None]: Contains a generic error message with the stack trace if an unexpected exception occurs.
    """
    try:
        # Assuming parser.parse() is some parsing logic for the SQL-like query
        parsing_result = parser.parse(query)
        if parsing_result:
            return Success(str(parsing_result))  # type: ignore
    except (LexerError, ParserError) as ex:
        return Failure(ex, None)
    except:
        # Return a Failure monad containing the stack trace in case of an error
        return Failure(traceback.format_exc())


def execute_python_code(
    python_code: str,
) -> Union[Success[DataFrame], Failure[PythonExecutionError, None]]:
    """
    Executes the given Python code and returns the resulting transformed data as a `DataFrame`,
    or an error message if execution fails.

    Args:
        python_code (str): The Python code to be executed. The code should perform data transformations
                           and create a variable named `transformed_data`, which will be returned as a `DataFrame`.

                           Examples of valid `python_code`:

                           **Example 1: Basic extraction and transformation**
                           ```python
                           from app import etl
                           extracted_data = etl.extract('csv', 'data_sets/hotel_bookings.csv')
                           transformed_data = etl.transform(
                               extracted_data,
                               {
                                   'COLUMNS': '__all__',
                                   'DISTINCT': False,
                                   'FILTER': None,
                                   'GROUP': None,
                                   'ORDER': None,
                                   'LIMIT_OR_TAIL': ('limit', 10),
                               }
                           )
                           ```

                           **Example 2: Applying a filter and ordering**
                           ```python
                           from app import etl
                           extracted_data = etl.extract("csv", "data_sets/hotel_bookings.csv")
                           transformed_data = etl.transform(
                               extracted_data,
                               {
                                   "COLUMNS": "__all__",
                                   "DISTINCT": False,
                                   "FILTER": {"type": "==", "left": "hotel", "right": "City Hotel"},
                                   'GROUP': None,
                                   "ORDER": ("arrival_date_year", "ASC"),
                                   "LIMIT_OR_TAIL": ("limit", 10),
                               }
                           )
                           ```

                           **Example 3: Partial data export**
                           ```python
                           from app import etl
                           extracted_data = etl.extract('csv', 'data_sets/hotel_bookings.csv')
                           transformed_data = etl.transform(
                               extracted_data,
                               {
                                   'COLUMNS': [0, 1, 2],
                                   'DISTINCT': False,
                                   'FILTER': None,
                                   'GROUP': None,
                                   'ORDER': None,
                                   'LIMIT_OR_TAIL': None,
                               }
                           )
                           etl.load(transformed_data, 'csv', 'e.csv')
                           ```

    Returns:
        Union[Success[DataFrame], Failure[PythonExecutionError, None]]:
            - Success[DataFrame]: Contains the resulting `DataFrame` if the code executes successfully.
            - Failure[PythonExecutionError, None]: Contains a `PythonExecutionError` object with details of the error and stack trace if execution fails.
    """
    try:
        exec(python_code)
        from app.etl.core import transformed_data

        return Success(transformed_data)

    except Exception as ex:

        return Failure(
            PythonExecutionError(
                message=str(ex),
                code=python_code,
                line=None,
                position=None,
            ),
            None,
        )
        # return Failure(traceback.format_exc(), None)
