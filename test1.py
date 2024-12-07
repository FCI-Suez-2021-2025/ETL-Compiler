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
    "Employee": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"],
    "Salary": [70000, 80000, 90000, 95000, 85000, 88000, 91000],
    "Bonus": [5000, 6000, 7000, 7500, 8000, 8500, 7200],
}

df = pd.DataFrame(data)
print(df)
# Group by 'Department' and 'Location', aggregate 'Salary' and 'Bonus'
grouped_df = (
    df.groupby(["Department", "Location"])
    .agg(
        Total_Salary=("Salary", "min"),
        var_Salary=("Salary", "var"),
        std_Bonus=("Bonus", "std"),
        Count_Employee=("Employee", "size"),
    )
    .reset_index()
)
print("-------------------------------------\n")
print(grouped_df)
# first,last,min,max,mean
