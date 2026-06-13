import pandas as pd
import numpy as np
from evidently import Report
from evidently.presets import TargetDriftPreset, RegressionPreset
from evidently import ColumnMapping

# Set seed for reproducibility
np.random.seed(42)

print("1. Generating Reference Data (Normal Market)...")
# The model was trained here and performs well. House prices are ~$150 per sqft.
ref_sqft = np.random.normal(loc=2000, scale=400, size=200)
ref_target = ref_sqft * 150 + np.random.normal(0, 10000, size=200) # Actual Price
ref_prediction = ref_target + np.random.normal(0, 5000, size=200)  # Model guess (very accurate)

reference_df = pd.DataFrame({
    'square_feet': ref_sqft,
    'target': ref_target,
    'prediction': ref_prediction
})

print("2. Generating Current Data (Shifted Market)...")
# The market bubbled! Houses are smaller, but cost ~$280 per sqft.
cur_sqft = np.random.normal(loc=1200, scale=300, size=200)
cur_target = cur_sqft * 280 + np.random.normal(0, 15000, size=200) # Actual Price Skyrocketed

# CRITICAL: The model doesn't know the market changed! 
# It still predicts prices using the old $150/sqft logic.
cur_prediction = cur_sqft * 150 + np.random.normal(0, 5000, size=200) 

current_df = pd.DataFrame({
    'square_feet': cur_sqft,
    'target': cur_target,
    'prediction': cur_prediction
})

print("3. Setting up Column Mapping...")
# This maps out our dataframe so Evidently knows what roles the columns play
column_mapping = ColumnMapping(
    target='target',
    prediction='prediction',
    numerical_features=['square_feet']
)

print("4. Running the Performance & Target Drift Report...")
# Add both presets to the report!
report = Report(metrics=[
    TargetDriftPreset(),
    RegressionPreset()
])

report.run(
    reference_data=reference_df, 
    current_data=current_df, 
    column_mapping=column_mapping
)

# We will skip printing the giant dictionary this time and just save the HTML.
# The HTML dashboard is the best way to view Model Performance.
report.save_html("model_performance_report.html")
print("\n✅ Saved detailed report to 'model_performance_report.html'. Open it in your browser!")