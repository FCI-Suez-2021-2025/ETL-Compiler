from collections import defaultdict
import pandas as pd

# Sample DataFrame
data = {
    "Department": ["HR", "HR", "IT", "IT", "Finance", "Finance", "IT"],
    "Location": [
        "New York",
        "New York",
        "San Francisco",
        "New York",
        "San Francisco",
        "New York",
        "San Francisco",
    ],
    "firstname": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"],
    "lastname": ["Ali", "medo", "ghareeb", "hossam", "alharery", "mark", "belbes"],
    "Salary": [70000, 80000, 90000, 95000, 85000, 88000, 91000],
    "Bonus": [5000, 6000, 7000, 7500, 8000, 8500, 7200],
}

df = pd.DataFrame(data)
print(df)
# Group by 'Department' and 'Location', aggregate 'Salary' and 'Bonus'
# grouped_df = (
#     df.groupby(["Department", "Location"])
#     .agg(
#         Total_Salary=("Salary", "min"),
#         var_Salary=("Salary", "var"),
#         std_Bonus=("Bonus", "std"),
#         Count_Employee=("Employee", "size"),
#     )
#     .reset_index()
# )
list = [
    ("firstname", "last"),
    ("lastname", "first"),
    ("firstname", "count"),
]
dict = {}
for key, value in list:
    current_value = dict.get(key, [])
    current_value.append(value)
    dict[key] = current_value

grouped_df = df.groupby(["Department"]).agg(dict).reset_index()

list_of_columns = []
for column in grouped_df.columns:
    if type(column) is tuple:
        if column[1].strip():
            list_of_columns.append("_".join(column).strip())
        else:
            list_of_columns.append(column[0])
    else:
        list_of_columns.append(column)

grouped_df.columns = list_of_columns
print("\n----------------- grouped data --------------------\n")
print(grouped_df)
# first,last,min,max,mean
# new_name = f"{aggregation}_{str(col)}"
