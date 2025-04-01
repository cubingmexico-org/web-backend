from flask import Flask, jsonify, request
import requests
import io
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import secretmanager
import os

app = Flask(__name__)

def get_secret(secret_id, project_id, version_id="latest"):
    """
    Retrieves the secret value from GCP Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Retrieve secret at runtime
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cubing-mexico")
DB_URL = get_secret("db_url", GCP_PROJECT_ID)

engine = create_engine(DB_URL)

def fetch_and_process_wca_data():
    url = "https://www.worldcubeassociation.org/export/results/WCA_export.tsv.zip"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as e:
        return {"error": f"Failed to fetch zip file: {e}"}

    zip_bytes = io.BytesIO(response.content)
    try:
        with zipfile.ZipFile(zip_bytes, "r") as z:
            file_name = "WCA_export_Competitions.tsv"
            if file_name not in z.namelist():
                return {"error": f"{file_name} not found in archive"}

            with z.open(file_name) as file:
                df = pd.read_csv(file, delimiter="\t", na_values=["NULL"])
                competitions = df.to_dict(orient="records")

                with engine.begin() as conn:
                    df.to_sql("competitions", con=conn, if_exists="replace", index=False)

            return {"message": "Database updated successfully", "data_sample": competitions[:3]}

    except zipfile.BadZipFile as e:
        return {"error": f"Error processing zip file: {e}"}

@app.route("/update-database", methods=["POST"])
def update_database():
    result = fetch_and_process_wca_data()
    return jsonify(result)

@app.route("/competitions", methods=["GET"])
def get_competitions():
    query = text("SELECT * FROM competitions LIMIT 10")
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        competitions = [dict(row._mapping) for row in result]
    return jsonify(competitions)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)