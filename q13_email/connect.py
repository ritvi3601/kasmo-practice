from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import base64
import boto3
import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# --- CONFIG ---
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
S3_BUCKET = "email-url"
DRIVER = "{ODBC Driver 17 for SQL Server}"
SERVER = "RITVI-SHAH"        # your server name
DATABASE = "ritvi_python"    # your database name
TRUSTED_CONNECTION = "yes"
TABLE_NAME = "emails"


# --- Gmail Authentication ---
def gmail_authenticate():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


# --- S3 Upload ---
def upload_to_s3(file_name, file_bytes):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=S3_BUCKET, Key=file_name, Body=file_bytes)
    return f"s3://{S3_BUCKET}/{file_name}"


# --- Helpers ---
def get_header(headers, name):
    return next((h["value"] for h in headers if h["name"].lower() == name.lower()), None)


def get_body(payload):
    if "parts" in payload:
        for part in payload["parts"]:
            if part["mimeType"] in ["text/plain", "text/html"]:
                return base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
            elif "parts" in part:
                return get_body(part)
    elif "data" in payload.get("body", {}):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    return ""


def get_attachments(service, msg_id, payload):
    urls = []
    if "parts" in payload:
        for part in payload["parts"]:
            filename = part.get("filename")
            if filename:
                att_id = part["body"].get("attachmentId")
                if att_id:
                    att = service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=att_id
                    ).execute()
                    file_data = base64.urlsafe_b64decode(att["data"])
                    s3_url = upload_to_s3(filename, file_data)
                    urls.append(s3_url)
    return urls


def extract_email(service, msg_id):
    message = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    headers = message["payload"]["headers"]

    return {
        "msg_id": msg_id,
        "sender": get_header(headers, "From"),
        "receiver": get_header(headers, "To"),
        "cc": get_header(headers, "Cc"),
        "subject": get_header(headers, "Subject"),
        "body": get_body(message["payload"]),
        "attachment_1": np.nan,
        "attachment_2": np.nan,
        "attachments": get_attachments(service, msg_id, message["payload"])
    }


def load_to_mssql(data):
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)
    
    # Map attachment URLs to separate columns
    for i, col in enumerate(['attachment_1', 'attachment_2']):
        df[col] = df['attachments'].apply(lambda x: x[i] if len(x) > i else np.nan)
    
    df.drop(columns=['attachments'], inplace=True)

    # Create SQLAlchemy engine
    engine = create_engine(
    f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)

    # Write to SQL Server
    df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
    print(f"✅ {len(df)} emails loaded to table {TABLE_NAME}")


# --- Main ---
if __name__ == "__main__":
    service = gmail_authenticate()
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], maxResults=5).execute()
    messages = results.get("messages", [])

    email_data = []
    for msg in messages:
        data = extract_email(service, msg["id"])
        email_data.append(data)

    load_to_mssql(email_data)
