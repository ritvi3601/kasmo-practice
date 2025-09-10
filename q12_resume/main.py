from src import extract, transform, load, archive

def run_pipeline():
    files = extract.download_resumes()  # returns list of tuples: (s3_key, pdf_path, txt_path)
    for s3_key, pdf_path, txt_path in files:  # unpack the tuple correctly

        # Transform using the TXT file
        resume_data = transform.convert_and_parse(txt_path)
        print(resume_data)

        # Load into database
        load.insert_resume(resume_data)

        # Archive in S3
        archive.archive_resume(s3_key)

if __name__ == "__main__":
    run_pipeline()
