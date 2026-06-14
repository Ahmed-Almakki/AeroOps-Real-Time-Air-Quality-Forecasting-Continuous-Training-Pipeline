import pandas as pd
import numpy as np
from evidently import Report, DataDefinition, Regression, Dataset
from evidently.presets import DataDriftPreset, RegressionPreset # Added RegressionPreset for performance tracking

# Set a seed so the random numbers generate the exact same way every time
np.random.seed(42)

print("1. Generating large datasets with targets and predictions...")


reference_df = pd.DataFrame({
    'square_feet': np.random.normal(loc=2000, scale=400, size=200),
    'price_per_sqft': np.random.normal(loc=150, scale=20, size=200),
    'property_age_years': np.random.normal(loc=30, scale=10, size=200)
})
reference_df['target'] = reference_df['square_feet'] * reference_df['price_per_sqft']
reference_df['prediction'] = reference_df['target'] + np.random.normal(loc=0, scale=15000, size=200)


current_df = pd.DataFrame({
    'square_feet': np.random.normal(loc=1200, scale=300, size=200),
    'price_per_sqft': np.random.normal(loc=280, scale=40, size=200),
    'property_age_years': np.random.normal(loc=31, scale=10, size=200)
})
current_df['target'] = current_df['square_feet'] * current_df['price_per_sqft']
current_df['prediction'] = (current_df['square_feet'] * 150) + np.random.normal(loc=0, scale=15000, size=200)

print("creating schemas...")
schema = DataDefinition(
    numerical_columns=["square_feet", "price_per_sqft", "property_age_years"],
    regression=[Regression(target='target', prediction='prediction')]
)

cur_data = Dataset.from_pandas(current_df, data_definition=schema)
ref_data = Dataset.from_pandas(reference_df, data_definition=schema)

report = Report([
    RegressionPreset()
], include_tests=True,)

eval = report.run(current_data=cur_data, reference_data=ref_data)

dict_eval = eval.dict()
# print(f"Report:\n{dict_eval['metrics']}\n{'==='*40}\ntest:\n{dict_eval['tests']}")
for ind, item in enumerate(dict_eval['tests']):
    print(f"item_{ind}:\n{item['status'].name}\n{item}\n{'==='*40}")
    break