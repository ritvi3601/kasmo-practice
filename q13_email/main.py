# main.py
from src.extract import gmail_authenticate, extract_emails
from src.transform import transform
from src.load import load_to_mssql

if __name__ == "__main__":
    service = gmail_authenticate()
    emails = extract_emails(service, max_results=5)
    df = transform(emails)
    load_to_mssql(df)
