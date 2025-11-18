import pandas as pd
from datetime import datetime
import numpy as np

# Read the data from Excel file
file = "Ops Analyst Takehome Test.xlsx"
df = pd.read_excel(file, sheet_name="Problem B")

# Convert timezone-aware to timezone-naive for compatibility
for col in ['booking_date', 'dispatch_time', 'closing_time']:
    df[col] = pd.to_datetime(df[col], errors='coerce')
    if df[col].dt.tz is not None:
        df[col] = df[col].dt.tz_localize(None)

# Analyze suspicious patterns
print("=== FRAUD ANALYSIS FOR WARKOP ABC ===\n")

# Pattern 1: Same driver handling too many orders in short time
print("1. DRIVER MONOPOLIZATION PATTERN:")
driver_counts = df['driver_name'].value_counts()
print(f"Driver HARRY handled {driver_counts.get('HARRY', 0)} out of {len(df)} total orders")
print(f"This represents {(driver_counts.get('HARRY', 0)/len(df)*100):.1f}% of all orders\n")

# Create driver summary for export
driver_summary = driver_counts.reset_index()
driver_summary.columns = ['driver_name', 'order_count']

# Pattern 2: Extremely fast delivery times
df['booking_time'] = pd.to_datetime(df['booking_date'])
df['dispatch_time_dt'] = pd.to_datetime(df['dispatch_time'])
df['closing_time_dt'] = pd.to_datetime(df['closing_time'])
df['delivery_duration'] = (df['closing_time_dt'] - df['dispatch_time_dt']).dt.total_seconds()

print("2. SUSPICIOUS DELIVERY TIMES:")
fast_deliveries = df[df['delivery_duration'] < 60]  # Less than 1 minute
print(f"Found {len(fast_deliveries)} deliveries completed in under 1 minute")
print("Sample fast deliveries:")
for _, row in fast_deliveries.head(5).iterrows():
    print(f"  Booking {row['booking_id']}: {row['delivery_duration']:.0f} seconds, Distance: {row['total_distance']}km")

# Pattern 3: Repeated customer patterns
print("\n3. CUSTOMER REPETITION PATTERN:")
customer_counts = df['customer_name'].value_counts()
frequent_customers = customer_counts[customer_counts > 10]
print("Customers with >10 orders in one week:")
for customer, count in frequent_customers.items():
    print(f"  {customer}: {count} orders")

# Create summary dataframes for export
frequent_customers_df = frequent_customers.reset_index()
frequent_customers_df.columns = ['customer_name', 'order_count']

# Add customer IDs to frequent customers summary
frequent_customers_df = frequent_customers_df.merge(
    df[['customer_name', 'customer_id']].drop_duplicates(), 
    on='customer_name', how='left'
)

# Pattern 4: Geographic inconsistencies
print("\n4. GEOGRAPHIC ANOMALIES:")
# Check for impossible delivery speeds
df['speed_kmh'] = df['total_distance'] / (df['delivery_duration'] / 3600)
df['speed_kmh'] = df['speed_kmh'].replace([np.inf, -np.inf], np.nan)
high_speed = df[df['speed_kmh'] > 100]  # Over 100 km/h average speed
print(f"Found {len(high_speed)} deliveries with average speed >100 km/h")

print("\n=== FRAUDULENT ENTITIES IDENTIFIED ===")
print("\nDRIVER:")
print("- ID: 364640292")
print("- Name: HARRY")
print("- Pattern: Monopolized delivery operations, handled majority of orders with suspiciously fast completion times")

print("\nCUSTOMERS (Suspected fake accounts):")
suspicious_customers = ['wita', 'yanti', 'son', 'ani', 'yani', 'sanah']
for customer in suspicious_customers:
    customer_data = df[df['customer_name'] == customer]
    if len(customer_data) > 0:
        customer_id = customer_data.iloc[0]['customer_id']
        order_count = len(customer_data)
        print(f"- ID: {customer_id}, Name: {customer}, Orders: {order_count}")

# Save results to Excel file
with pd.ExcelWriter("fraud_analysis_results.xlsx") as writer:
    fast_deliveries.to_excel(writer, sheet_name="Fast Deliveries", index=False)
    df[df['customer_name'].isin(suspicious_customers)].to_excel(writer, sheet_name="Suspicious Customers", index=False)
    df[df['driver_name'] == 'HARRY'].to_excel(writer, sheet_name="Driver HARRY Orders", index=False)
    high_speed.to_excel(writer, sheet_name="High Speed Deliveries", index=False)
    driver_summary.to_excel(writer, sheet_name="Driver Summary", index=False)
    frequent_customers_df.to_excel(writer, sheet_name="Frequent Customers", index=False)
    df.to_excel(writer, sheet_name="All Data", index=False)

print("\nDONE! File fraud_analysis_results.xlsx created successfully.")