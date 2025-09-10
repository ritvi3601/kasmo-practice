import pymongo
import pandas as pd
import config

def transform_data(database, collection_name):
    client = pymongo.MongoClient(config.MONGO_URI)
    db = client[database]
    collection = db[collection_name]

    projects = []
    technologies = []

    for doc in collection.find({}):
        inner = doc  # adjust if JSON stored in "content"

        project_id = inner.get("project_id")
        if not project_id:
            continue  # skip invalid rows

        # Project fields
        projects.append({
            "project_id": project_id,
            "project_name": inner.get("project_name"),
            "client": inner.get("client"),
            "domain": inner.get("domain"),
            "location": inner.get("location"),
            "project_manager": inner.get("project_manager"),
            "start_date": inner.get("start_date"),
            "end_date": inner.get("end_date"),
            "status": inner.get("status"),
        })

        # Explode technologies
        for tech in inner.get("technologies", []):
            technologies.append({
                "project_id": project_id,
                "technology": tech
            })

    # Convert to DataFrames
    projects_df = pd.DataFrame(projects)
    technologies_df = pd.DataFrame(technologies)

    projects_df = projects_df.drop_duplicates().reset_index(drop=True)
    technologies_df = technologies_df.drop_duplicates().reset_index(drop=True)

    status_map = {
        "In Progress": "Active",
        "Planned": "Pending",
        "Completed": "Done"
    }
    projects_df["status"] = projects_df["status"].map(status_map)

    # ✅ Convert start_date and end_date to datetime
    projects_df["start_date"] = pd.to_datetime(projects_df["start_date"], errors='coerce')
    projects_df["end_date"] = pd.to_datetime(projects_df["end_date"], errors='coerce')

    print("📊 Projects Table:")
    print(projects_df)
    print("\n📊 Technologies Table :")
    print(technologies_df)

    return projects_df, technologies_df
