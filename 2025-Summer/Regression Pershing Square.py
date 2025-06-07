import pandas as pd
import statsmodels.formula.api as smf


# Load datasets
combined = pd.read_csv('/Users/admin/Library/CloudStorage/OneDrive-Persönlich/Desktop/QFE/Projects/13f/Data/Pershing_holdings_all.csv')

# Run OLS regression
model = smf.ols('price_change_pct ~  hld_pct_change', data=combined).fit(cov_type='nonrobust')
print(model.summary())

print(combined.head())


