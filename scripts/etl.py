import pandas as pd
import matplotlib.pyplot as plt
import os

# ---------------- Extract ----------------
def extract_data(file_path="data/raw/hotel_bookings.csv"):
    """
    Extract raw CSV data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found. Make sure the CSV is in data/raw/")
    
    df = pd.read_csv(file_path)
    print(f"Extracted {len(df)} rows from {file_path}")
    return df

# ---------------- Transform ----------------
def transform_data(df):
    """
    Clean and transform the dataset for analysis.
    """
    # Handle missing values
    df['children'] = df['children'].fillna(0)
    df['country'] = df['country'].fillna("Unknown")
    df['agent'] = df['agent'].fillna(0)
    df['company'] = df['company'].fillna(0)

    # Calculate total stay
    df['total_stay'] = df['stays_in_weekend_nights'] + df['stays_in_week_nights']

    # Convert reservation_status_date to datetime
    df['reservation_status_date'] = pd.to_datetime(df['reservation_status_date'])

    # Calculate total guests
    df['total_guests'] = df['adults'] + df['children'] + df['babies']

    print(f"Transformed data: {len(df)} valid rows")
    return df

# ---------------- Load ----------------
def load_data(df, mode="csv"):
    """
    Save cleaned data for Data Warehouse.
    Raw CSV acts as Data Lake.
    """
    os.makedirs("data/transformed", exist_ok=True)
    if mode == "csv":
        output_file = "data/transformed/cleaned_hotel_bookings.csv"
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
    elif mode == "parquet":
        try:
            import pyarrow
            output_file = "data/transformed/cleaned_hotel_bookings.parquet"
            df.to_parquet(output_file, index=False)
            print(f"Data saved to {output_file}")
        except ImportError:
            print("pyarrow not installed. Falling back to CSV.")
            df.to_csv("data/transformed/cleaned_hotel_bookings.csv", index=False)
    else:
        raise ValueError("mode must be 'csv' or 'parquet'")

# ---------------- Analyze ----------------
def analyze_data(df):
    """
    Generate simple analytics charts.
    """
    # Top 5 hotels by bookings
    top_hotels = df['hotel'].value_counts().head(5)
    print("Top 5 Hotels:\n", top_hotels)
    top_hotels.plot(kind='bar', title="Top 5 Hotels by Bookings")
    plt.savefig("visuals/top_hotels.png")
    plt.show()

    # Monthly bookings
    df['month'] = df['reservation_status_date'].dt.to_period('M')
    monthly_bookings = df.groupby('month').size()
    print("Monthly Bookings:\n", monthly_bookings)
    monthly_bookings.plot(kind='line', marker='o', title="Monthly Bookings")
    plt.savefig("visuals/monthly_bookings.png")
    plt.show()

    # Cancellation rate per hotel
    cancellation_rate = df.groupby('hotel')['is_canceled'].mean() * 100
    print("Cancellation Rate (%):\n", cancellation_rate)
    cancellation_rate.plot(kind='bar', title="Cancellation Rate per Hotel")
    plt.savefig("visuals/cancellation_rate.png")
    plt.show()

    # Average Daily Rate (ADR) by hotel
    adr_by_hotel = df.groupby('hotel')['adr'].mean()
    print("Average Daily Rate (ADR):\n", adr_by_hotel)
    adr_by_hotel.plot(kind='bar', title="Average Daily Rate by Hotel")
    plt.savefig("visuals/adr_by_hotel.png")
    plt.show()

# ---------------- Main ETL Flow ----------------
if __name__ == "__main__":
    # Ensure visuals folder exists
    os.makedirs("visuals", exist_ok=True)

    # Extract
    data = extract_data("data/raw/hotel_bookings.csv")

    # Transform
    data = transform_data(data)

    # Load
    load_data(data, mode="csv")  # Can change to "parquet" if pyarrow is installed

    # Analyze
    analyze_data(data)
