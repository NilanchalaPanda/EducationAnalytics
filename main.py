from src.extract import extract_csv_to_parquet


def run_initial_etl():
    # Step 1: Extract CSV
    extract_csv_to_parquet(
        csv_path="data/students_dataset.csv",
        parquet_path="data/processed/students.parquet"
    )

if __name__ == "__main__":