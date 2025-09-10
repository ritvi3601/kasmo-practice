import os
import pdfplumber
from config import s3, BUCKET_NAME, INCOMING_PREFIX, LOCAL_DIR

def download_resumes(local_dir=LOCAL_DIR):
    os.makedirs(local_dir, exist_ok=True)

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=INCOMING_PREFIX)
    if "Contents" not in response:
        print("⚠️ No files found in S3 bucket!")
        return []

    files = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".pdf"):
            local_path = os.path.join(local_dir, os.path.basename(key))
            print(f"Downloading {key} → {local_path}")
            s3.download_file(BUCKET_NAME, key, local_path)

            # Convert PDF → TXT
            txt_path = local_path.replace(".pdf", ".txt")
            with pdfplumber.open(local_path) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(text)

            files.append((key, local_path, txt_path))
            print(f"✅ Converted to TXT: {txt_path}")

    return files
