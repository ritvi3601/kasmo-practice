from src.extract import load_text_file_to_mongodb
from src import transform
import pymongo  
from sqlalchemy import create_engine
import config
from src import load
engine = create_engine(
    f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_DATABASE}"
)
def main():
    file_paths = ["project.txt"]
    db_config = {"database": "my_database", "collection": "text_files"}

    print(" Uploading files to MongoDB...")
    for path in file_paths:
        loaded_file= load_text_file_to_mongodb(path, db_config["database"], db_config["collection"])
        print(f"→ Loaded '{path}'")
        print(loaded_file)

    print("Files loaded. Running transformation...")

    # Call your transform function
    df_transformed1,df_transformed2= transform.transform_data(db_config["database"], db_config["collection"])

    print(" Transformation complete.")
    df_load=load.load_to_sql(df_transformed1, "projects_transformed_q7", engine)
    print(df_load)
    df_load1=load.load_to_sql(df_transformed2, "project_technologies_q7", engine)
    print(df_load1)

if __name__ == "__main__":
    main()
