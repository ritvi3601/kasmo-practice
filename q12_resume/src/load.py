from sqlalchemy import create_engine, Table, Column, String, MetaData
import config

# 1️⃣ For Windows Authentication (Trusted Connection)
engine = create_engine(
    f"mssql+pyodbc://@{config.SERVER}/{config.DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

# 2️⃣ (Optional) For SQL Server Authentication)
# engine = create_engine(
#     f"mssql+pyodbc://{config.USERNAME}:{config.PASSWORD}@{config.SERVER}/{config.DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
# )

metadata = MetaData()

# Example table definition (adjust columns as per your parsed resume data)
resumes_table = Table(
    "Resumes",
    metadata,
    Column("Name", String(100)),
    Column("Email", String(100)),
    Column("ContactNumber", String(50)),
    Column("Summary", String(1000)),
    Column("Skills", String(500)),
    Column("Education", String(500)),
    Column("ProfessionalExperience", String(2000)),
)

metadata.create_all(engine)  # creates table if not exists

def insert_resume(resume_data):
    """Insert a single resume record into SQL Server"""
    with engine.connect() as conn:
        conn.execute(
            resumes_table.insert(),
            {
                "Name": resume_data.get("Name"),
                "Email": resume_data.get("Email"),
                "ContactNumber": resume_data.get("ContactNumber"),
                "Summary": resume_data.get("Summary"),
                "Skills": resume_data.get("Skills"),
                "Education": resume_data.get("Education"),
                "ProfessionalExperience": resume_data.get("ProfessionalExperience"),
            },
        )
        conn.commit()
    print(f"Inserted {resume_data.get('Name')} into SQL Server")
