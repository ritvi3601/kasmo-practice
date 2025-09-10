from src.extract import load_text_file_to_mongodb
from src import transform
from sqlalchemy import create_engine
import config
from src import load

engine = create_engine(
    f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_DATABASE}"
)
def main():
    file_paths = ["unstructured_project.txt"]
    db_config = {"database": "my_database", "collection": "text_files"}
    
    print(" Uploading files to MongoDB...")
    for path in file_paths:
        loaded_file= load_text_file_to_mongodb(path, db_config["database"], db_config["collection"])
        print(f"→ Loaded '{path}'")
        print(loaded_file)

    print("Files loaded. Running transformation...")

    df_transformed1,df_transformed2,df_transformed3,df_transformed4=transform.transform_data(db_config["database"], db_config["collection"])

    df_load=load.load_to_sql(df_transformed1, "projects_updated", engine)
    print(df_load)
    df_load1=load.load_to_sql(df_transformed2, "project_technology", engine)
    print(df_load1)
    df_load2=load.load_to_sql(df_transformed3, "project_team_members", engine)
    print(df_load2)
    df_load3=load.load_to_sql(df_transformed4, "project_milestone", engine)
    print(df_load3)


if __name__ == "__main__":
    main()