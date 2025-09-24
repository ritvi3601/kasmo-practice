# extract.py
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import base64
import boto3
import os
import numpy as np
from config import SCOPES, CREDENTIALS_FILE, TOKEN_FILE, S3_BUCKET

# --- Gmail Authentication ---
def gmail_authenticate():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def upload_to_s3(file_name, file_bytes):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=S3_BUCKET, Key=file_name, Body=file_bytes)
    return f"s3://{S3_BUCKET}/{file_name}"


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


def extract_emails(service, max_results=5):
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], maxResults=max_results).execute()
    messages = results.get("messages", [])

    email_data = []
    for msg in messages:
        message_detail = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
        headers = message_detail["payload"]["headers"]

        attachments = get_attachments(service, msg["id"], message_detail["payload"])

        data = {
            "msg_id": msg["id"],
            "sender": get_header(headers, "From"),
            "receiver": get_header(headers, "To"),
            "cc": get_header(headers, "Cc"),
            "subject": get_header(headers, "Subject"),
            "body": get_body(message_detail["payload"]),
            "attachment_1": attachments[0] if len(attachments) > 0 else np.nan,
            "attachment_2": attachments[1] if len(attachments) > 1 else np.nan,
        }
        email_data.append(data)
    return email_data
