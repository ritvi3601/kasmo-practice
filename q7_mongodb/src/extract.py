import pymongo
import json
import config

def load_text_file_to_mongodb(file_path, database, collection):
    client = pymongo.MongoClient(config.MONGO_URI)
    db = client[database]
    coll = db[collection]  

    coll.delete_many({})
    print(f"🗑️ Cleared all existing records in collection '{collection}'")

    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read().strip()

        try:
            # Parse JSON
            document = json.loads(content)

            # Handle list of objects
            if isinstance(document, list):
                coll.insert_many(document)
                print(f"✅ Inserted {len(document)} documents from file: {file_path}")
            else:
                coll.insert_one(document)
                print(f"✅ Inserted document from file: {file_path}")

        except json.JSONDecodeError:
            # Store raw text
            coll.insert_one({"content": content})
            print(f"⚠️ Stored raw text from file: {file_path}")
