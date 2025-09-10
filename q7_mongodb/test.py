import pymongo
import config
from datetime import datetime  # Correct import for datetime

def load_text_file_to_mongodb(file_path, db_name, collection_name):

    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client[config.DB_NAME]
        collection = db[config.COLLECTION_NAME]
        
        # Read the file content
        with open(file_path, 'r') as file:
            content = file.read()
            
        # Create a document to insert
        document = {
            "filename": file_path,
            "content": content,
            "timestamp": datetime.now() # Use the correct datetime object here
        }
        
        # Insert the document
        result = collection.insert_one(document)
        print(f"Successfully inserted document with _id: {result.inserted_id} from file: {file_path}")
        
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error: Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    # Define the file paths
    file1_path = "project.txt"
    file2_path = "Doc_unstructured_1.txt"

    # Define MongoDB details
    database_name = "my_database"
    collection_name = "text_files"

    # Load each file, adding a placeholder for collection_name2
    load_text_file_to_mongodb(file1_path, database_name, collection_name)
    load_text_file_to_mongodb(file2_path, database_name, collection_name)