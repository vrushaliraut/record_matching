import pandas as pd
from sqlalchemy import create_engine

def load_data(file_path):
    return pd.read_csv(file_path)

def save_to_sql(data, table_name, engine):
    data.to_sql(table_name, engine, if_exists='replace', index=False)

def identify_missing_records(engine, data1_table, data2_table):
    query = f"""
    SELECT DISTINCT data1.*
    FROM {data1_table} data1
    LEFT JOIN {data2_table} data2
    ON data1.`Order ID` = data2.`Order ID` AND data1.`Product ID` = data2.`Product ID`
    WHERE data2.`Order ID` IS NULL AND data2.`Product ID` IS NULL
    """
    return pd.read_sql(query, engine)

def main():
    # File paths
    data1_file = '/Users/vrushali/PycharmProjects/record_matching/data/data1.csv'
    data2_file = '/Users/vrushali/PycharmProjects/record_matching/data/data2.csv'
    print(f"reach out to path ")

    # Load data
    data1 = load_data(data1_file)
    data2 = load_data(data2_file)
    print(f"2")

    # Database connection (SQLite in this example)
    engine = create_engine('sqlite:///record_matching.db')
    print(f"3")
    # Save data to SQL tables
    save_to_sql(data1, 'data1', engine)
    save_to_sql(data2, 'data2', engine)

    # Identify missing records
    missing_records = identify_missing_records(engine, 'data1', 'data2')
    print(f"4")
    # Print the results
    print(f"Number of records missing in data2: {len(missing_records)}")
    print(missing_records)

if __name__ == "__main__":
    main()