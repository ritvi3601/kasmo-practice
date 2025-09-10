import pymongo
import pandas as pd
import config

def transform_data(database, collection_name):
    client = pymongo.MongoClient(config.MONGO_URI)
    db = client[database]
    collection = db[collection_name]

    projects = []
    technologies = []
    team = []
    team_members = []
    milestones = []

    for doc in collection.find({}):
        inner = doc

        project_id = inner.get("project_id")
        if not project_id:
            continue  # skip invalid rows

        client_info = inner.get("client", {})
        location_info = client_info.get("location", {})

        team_info = inner.get("team", {})
        members_info = team_info.get("members", [])

        # --- Projects table ---
        projects.append({
            "project_id": project_id,
            "project_name": inner.get("project_name"),
            "client_name": client_info.get("name"),
            "client_industry": client_info.get("industry"),
            "client_city": location_info.get("city"),
            "client_country": location_info.get("country"),
            "project_manager": team_info.get("project_manager"),
            "status": inner.get("status")
        })

        # --- Technologies table ---
        for tech in inner.get("technologies", []):
            technologies.append({
                "project_id": project_id,
                "technology": tech
            })

        # --- Team Members table ---
        for member in members_info:
            team_members.append({
                "project_id": project_id,
                "name": member.get("name"),
                "role": member.get("role")
            })

        # --- Milestones table ---
        for milestone in inner.get("milestones", []):
            milestones.append({
                "project_id": project_id,
                "milestone_name": milestone.get("name"),
                "due_date": milestone.get("due_date")
            })

    # Convert to DataFrames
    projects_df = pd.DataFrame(projects).drop_duplicates().reset_index(drop=True)
    technologies_df = pd.DataFrame(technologies).drop_duplicates().reset_index(drop=True)
    team_members_df = pd.DataFrame(team_members).drop_duplicates().reset_index(drop=True)
    milestones_df = pd.DataFrame(milestones).drop_duplicates().reset_index(drop=True)

    # Normalize project status
    status_map = {"In Progress": "Active", "Planned": "Pending", "Completed": "Done"}
    projects_df["status"] = projects_df["status"].map(status_map)

    # Convert milestone due_date to datetime
    milestones_df["due_date"] = pd.to_datetime(milestones_df["due_date"], errors='coerce')

    print("📊 Projects Table:")
    print(projects_df)
    print("\n📊 Technologies Table:")
    print(technologies_df)
    print("\n📊 Team Members Table:")
    print(team_members_df)
    print("\n📊 Milestones Table:")
    print(milestones_df)

    return projects_df, technologies_df, team_members_df, milestones_df
