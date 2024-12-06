from app import *
from app.etl.controllers import *
from app.gui.ui_compiler import UICompiler


if __name__ == "__main__":
    app = UICompiler()
    app.mainloop()

## for testing the program without ui
#     query = r"""
# Select email,
# [email 2],
# [3] into
# {csv:transformed data.csv}
# from
# {csv:testing_datasets/hotel_bookings.csv}
# Where not [1]=="moas" and email==1
# limit 10;"""
#     compilation_result = compile_to_python(query)
#     if type(compilation_result) == Success:

#         python_code = compilation_result.unwrap()
#         print(python_code)
#         execution_result = execute_python_code(python_code)
#         if type(execution_result) == Success:
#             print(execution_result.unwrap())

#         else:
#             print(execution_result.unwrap_error())

#     # to print failure string value
#     else:
#         print(compilation_result.unwrap_error())
