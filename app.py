import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
import re
import io
import zipfile
import requests
import logging
import pandas as pd
from datetime import datetime
from flask import Flask, jsonify, request
from google.cloud import secretmanager
import os
from utils import get_state_from_coordinates

EXCLUDED_EVENTS = ["333ft", "333mbo", "magic", "mmagic"]
SINGLE_EVENTS = ["333fm", "333bf", "333mbf", "444bf", "555bf"]

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

def get_secret(secret_id, project_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "cubing-mexico")
DB_URL = get_secret("db_url", GCP_PROJECT_ID)

def get_connection():
    return psycopg2.connect(DB_URL)

# Helper function to extract year from competitionId
def get_year_from_competition_id(competition_id: str):
    """
    Extracts the year from a competition ID string (e.g., "CompetitionName2024" -> 2024).
    Returns the year as an integer, or None if not found or input is invalid.
    """
    if not isinstance(competition_id, str):
        return None
    # Regex to find a 4-digit year (1xxx or 2xxx) at the end of the string.
    match = re.search(r'([12]\d{3})$', competition_id)
    if match:
        return int(match.group(1))
    return None

@app.route("/update-database", methods=["POST"])
def update_full_database():
    url = "https://www.worldcubeassociation.org/export/results/WCA_export.tsv.zip"
    try:
        log.info(f"Fetching data from {url}")
        response = requests.get(url)
        response.raise_for_status()

        content_disposition = response.headers.get('content-disposition')
        if content_disposition:
            filename_match = re.search(r'filename="?([^"]+)"?', content_disposition)
            if filename_match:
                filename = filename_match.group(1)
                log.info(f"Downloaded file with filename: {filename}")
            else:
                log.warning("Could not determine filename from Content-Disposition header.")
        else:
            log.warning("Content-Disposition header not found in response.")

    except requests.HTTPError as e:
        return jsonify({"error": f"Failed to fetch zip file: {e}"}), 500

    zip_bytes = io.BytesIO(response.content)
    try:
        with zipfile.ZipFile(zip_bytes, "r") as z:
            for file_name in z.namelist():
                if file_name == "WCA_export_Competitions.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    cleaned_content = file_content.replace('"', '')
                    df = pd.read_csv(io.StringIO(cleaned_content), delimiter="\t", na_values=["NULL"])
                    competitions = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                            cur.execute("SELECT * FROM states")
                            states = cur.fetchall()

                            cur.execute("SELECT id FROM delegates")
                            delegates = cur.fetchall()

                            cur.execute("SELECT id FROM organisers")
                            organisers = cur.fetchall()

                            cur.execute("SELECT id FROM competitions")
                            existing = cur.fetchall()
                            existing_ids = {row.id for row in existing}

                            for row in competitions:
                                if row["id"] in existing_ids:
                                    continue
                                state_id = None
                                if row["countryId"] == "Mexico":
                                    state_name = get_state_from_coordinates(
                                        row["latitude"] / 1000000,
                                        row["longitude"] / 1000000
                                    )
                                    if state_name:
                                        for s in states:
                                            if s.name == state_name:
                                                state_id = s.id
                                                break
                                start_date = datetime(row["year"], row["month"], row["day"])
                                end_date = datetime(row["year"], row["endMonth"], row["endDay"])
                                cur.execute(
                                    """
                                    INSERT INTO competitions 
                                    (id, name, "cityName", "countryId", information, "startDate", "endDate", cancelled,
                                     venue, "venueAddress", "venueDetails", external_website, "cellName",
                                     latitude, longitude, "stateId")
                                    VALUES (%(id)s, %(name)s, %(cityName)s, %(countryId)s, %(information)s,
                                            %(startDate)s, %(endDate)s, %(cancelled)s, %(venue)s,
                                            %(venueAddress)s, %(venueDetails)s, %(external_website)s,
                                            %(cellName)s, %(latitude)s, %(longitude)s, %(stateId)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "name": row["name"],
                                        "cityName": row["cityName"],
                                        "countryId": row["countryId"],
                                        "information": row["information"],
                                        "startDate": start_date,
                                        "endDate": end_date,
                                        "cancelled": row["cancelled"],
                                        "venue": row["venue"],
                                        "venueAddress": row["venueAddress"],
                                        "venueDetails": row["venueDetails"],
                                        "external_website": row["external_website"],
                                        "cellName": row["cellName"],
                                        "latitude": row["latitude"],
                                        "longitude": row["longitude"],
                                        "stateId": state_id
                                    }
                                )
                                if row["countryId"] == "Mexico":
                                    # competition_events
                                    for event_spec in str(row["eventSpecs"]).split():
                                        cur.execute(
                                            """
                                            INSERT INTO competition_events ("competitionId", "eventId")
                                            VALUES (%(competitionId)s, %(eventId)s)
                                            ON CONFLICT DO NOTHING
                                            """,
                                            {
                                                "competitionId": row["id"],
                                                "eventId": event_spec
                                            }
                                        )
                                    # organisers
                                    organiser_pattern = re.compile(r"\{([^}]+)\}\{mailto:([^}]+)\}")
                                    for match in organiser_pattern.finditer(str(row["organiser"])):
                                        organiser_name = match.group(1)
                                        organiser_email = match.group(2)
                                        exists = any(o.id == organiser_email for o in organisers)
                                        cur.execute(
                                            "SELECT id FROM persons WHERE name = %s",
                                            (organiser_name,)
                                        )
                                        person_res = cur.fetchone()
                                        person_id = person_res.id if person_res else None
                                        if not exists:
                                            cur.execute(
                                                """
                                                INSERT INTO organisers (id, "personId", status)
                                                VALUES (%(id)s, %(personId)s, 'active')
                                                ON CONFLICT DO NOTHING
                                                """,
                                                {"id": organiser_email, "personId": person_id}
                                            )
                                        cur.execute(
                                            """
                                            INSERT INTO competition_organisers ("competitionId", "organiserId")
                                            VALUES (%(competitionId)s, %(organiserId)s)
                                            ON CONFLICT DO NOTHING
                                            """,
                                            {
                                                "competitionId": row["id"],
                                                "organiserId": organiser_email
                                            }
                                        )
                                    # delegates
                                    delegate_pattern = re.compile(r"\{([^}]+)\}\{mailto:([^}]+)\}")
                                    for match in delegate_pattern.finditer(str(row["wcaDelegate"])):
                                        delegate_name = match.group(1)
                                        delegate_email = match.group(2)
                                        exists = any(d.id == delegate_email for d in delegates)
                                        cur.execute(
                                            "SELECT id FROM persons WHERE name = %s",
                                            (delegate_name,)
                                        )
                                        person_res = cur.fetchone()
                                        person_id = person_res.id if person_res else None
                                        if not exists and person_id:
                                            cur.execute(
                                                """
                                                INSERT INTO delegates (id, "personId", status)
                                                VALUES (%(id)s, %(personId)s, 'active')
                                                ON CONFLICT DO NOTHING
                                                """,
                                                {"id": delegate_email, "personId": person_id}
                                            )
                                        if exists or person_id:
                                            cur.execute(
                                                """
                                                INSERT INTO competition_delegates ("competitionId", "delegateId")
                                                VALUES (%(competitionId)s, %(delegateId)s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                {
                                                    "competitionId": row["id"],
                                                    "delegateId": delegate_email
                                                }
                                            )

                elif file_name == "WCA_export_championships.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    cleaned_content = file_content.replace('"', '')
                    df = pd.read_csv(io.StringIO(cleaned_content), delimiter="\t", skip_blank_lines=True, na_values=["NULL"])
                    championships = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in championships:
                                cur.execute(
                                    """
                                    INSERT INTO championships (id, "competitionId", "championshipType")
                                    VALUES (%(id)s, %(competition_id)s, %(championship_type)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "competition_id": row["competition_id"],
                                        "championship_type": row["championship_type"]
                                    }
                                )

                elif file_name == "WCA_export_Events.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True)
                    events = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in events:
                                cur.execute(
                                    """
                                    INSERT INTO events (id, format, name, rank, "cellName")
                                    VALUES (%(id)s, %(format)s, %(name)s, %(rank)s, %(cellName)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "format": row["format"],
                                        "name": row["name"],
                                        "rank": row["rank"],
                                        "cellName": row["cellName"]
                                    }
                                )

                elif file_name == "WCA_export_Persons.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t",
                                     skip_blank_lines=True, na_values=["NULL"])
                    persons = df.to_dict(orient="records")
                    cleaned_persons = []
                    for p in persons:
                        if p["countryId"] == "Mexico":
                            gender = p.get("gender")
                            gender = None if pd.isna(gender) else str(gender)
                            cleaned_persons.append({
                                "id": p["id"],
                                "name": p["name"],
                                "gender": gender
                            })
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in cleaned_persons:
                                cur.execute(
                                    """
                                    INSERT INTO persons (id, name, gender)
                                    VALUES (%(id)s, %(name)s, %(gender)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    row
                                )

                elif file_name == "WCA_export_RanksAverage.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t",
                                    skip_blank_lines=True, low_memory=False)
                    data = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                            cur.execute("SELECT id FROM persons")
                            persons = cur.fetchall()
                            person_ids = {p.id for p in persons}
                            filtered = [d for d in data if d["personId"] in person_ids]
                            cur.execute('DELETE FROM "ranksAverage"')

                            # Prepare rows for batch insert
                            rows_to_insert = [
                                (
                                    row["personId"], row["eventId"], row["best"], row["worldRank"],
                                    row["continentRank"], row["countryRank"]
                                )
                                for row in filtered
                            ]

                            # Use execute_values for batch insert
                            execute_values(
                                cur,
                                """
                                INSERT INTO "ranksAverage"
                                ("personId", "eventId", best, "worldRank", "continentRank", "countryRank")
                                VALUES %s
                                ON CONFLICT DO NOTHING
                                """,
                                rows_to_insert
                            )

                elif file_name == "WCA_export_RanksSingle.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t",
                                    skip_blank_lines=True, low_memory=False)
                    data = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                            cur.execute("SELECT id FROM persons")
                            persons = cur.fetchall()
                            person_ids = {p.id for p in persons}
                            filtered = [d for d in data if d["personId"] in person_ids]
                            cur.execute('DELETE FROM "ranksSingle"')

                            # Prepare rows for batch insert
                            rows_to_insert = [
                                (
                                    row["personId"], row["eventId"], row["best"], row["worldRank"],
                                    row["continentRank"], row["countryRank"]
                                )
                                for row in filtered
                            ]

                            # Use execute_values for batch insert
                            execute_values(
                                cur,
                                """
                                INSERT INTO "ranksSingle"
                                ("personId", "eventId", best, "worldRank", "continentRank", "countryRank")
                                VALUES %s
                                ON CONFLICT DO NOTHING
                                """,
                                rows_to_insert
                            )

                # This is the beginning of your conditional block for WCA_export_Results.tsv
                # Ensure this `elif` statement is correctly placed in your existing file processing loop.
                elif file_name == "WCA_export_Results.tsv":
                    log.info(f"Evaluating file for processing: {file_name}")
                    file_bytes = z.read(file_name) # Read file bytes from the zip archive member

                    # --- Start: Pre-check for missing personIds ---
                    try:
                        log.info(f"Pre-checking personIds in {file_name} against the database.")
                        with get_connection() as conn:
                            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                                cur.execute("SELECT id FROM persons")
                                db_persons = cur.fetchall()
                                db_person_ids = {p.id for p in db_persons}
                        
                        df_results_persons = pd.read_csv(
                            io.BytesIO(file_bytes),
                            delimiter="\t",
                            usecols=['personId', 'personCountryId'],
                            skip_blank_lines=True,
                            na_values=["NULL"],
                            low_memory=False
                        )

                        df_mexico_results = df_results_persons[df_results_persons['personCountryId'] == 'Mexico']
                        file_person_ids = set(df_mexico_results['personId'].unique())
                        
                        missing_person_ids = file_person_ids - db_person_ids
                        
                        if missing_person_ids:
                            log.error(f"SKIPPING update for {file_name} due to corrupted data. "
                                      f"The following personIds from the results file do not exist in the persons table: "
                                      f"{list(missing_person_ids)[:10]} (showing up to 10). "
                                      "This indicates a corrupted export file. The 'results' table will not be modified.")
                            continue # Skip to the next file in the zip
                    except Exception as e:
                        log.error(f"Error during personId pre-check for {file_name}: {e}. "
                                  "Skipping processing of this file to be safe.")
                        continue

                    # --- Start: Pre-check using exportMetadata to determine if Results.tsv should be skipped ---
                    try:
                        with get_connection() as conn:
                            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                                # 1. Get the ID of the last competition that was successfully processed
                                cur.execute("""
                                    SELECT value FROM "exportMetadata" WHERE key = 'last_competition'
                                """)
                                metadata_record = cur.fetchone()
                                last_processed_comp_id = metadata_record.value if metadata_record else None

                                if not last_processed_comp_id:
                                    log.info("No 'last_competition' record in exportMetadata. "
                                            "Proceeding with full processing as this might be the first run.")
                                    # Fall through to the 'else' block to process the file
                                else:
                                    # 2. Find the latest competition in the current results file
                                    df_comp_ids = pd.read_csv(
                                        io.BytesIO(file_bytes),
                                        delimiter="\t",
                                        usecols=['competitionId'],
                                        skip_blank_lines=True,
                                        na_values=["NULL"],
                                        low_memory=False
                                    )
                                    
                                    if df_comp_ids.empty:
                                        log.warning(f"{file_name} is empty or has no competition IDs. Skipping metadata check.")
                                    else:
                                        file_comp_ids = list(df_comp_ids['competitionId'].dropna().unique())
                                        
                                        if not file_comp_ids:
                                            log.warning(f"No valid competition IDs found in {file_name}. Skipping metadata check.")
                                        else:
                                            # Query the DB to find the latest competition among the ones in the file
                                            cur.execute("""
                                                SELECT id, "startDate" FROM competitions
                                                WHERE id = ANY(%s)
                                                ORDER BY "startDate" DESC, id DESC
                                                LIMIT 1
                                            """, (file_comp_ids,))
                                            latest_comp_in_file = cur.fetchone()

                                            if not latest_comp_in_file:
                                                log.warning(f"None of the competition IDs from {file_name} exist in the 'competitions' table. "
                                                            "Cannot determine if file is outdated. Proceeding with caution.")
                                            else:
                                                # 3. Compare with the last processed competition
                                                cur.execute("""
                                                    SELECT "startDate" FROM competitions WHERE id = %s
                                                """, (last_processed_comp_id,))
                                                last_processed_comp = cur.fetchone()

                                                if not last_processed_comp:
                                                    log.warning(f"Last processed competition ID '{last_processed_comp_id}' not found in 'competitions' table. "
                                                                "Proceeding with processing.")
                                                elif latest_comp_in_file.startDate <= last_processed_comp.startDate:
                                                    log.info(f"SKIPPING update for {file_name}. "
                                                            f"The latest competition in this file ('{latest_comp_in_file.id}' on {latest_comp_in_file.startDate.date()}) "
                                                            f"is not newer than the last successfully processed competition ('{last_processed_comp_id}' on {last_processed_comp.startDate.date()}).")
                                                    continue # Skip to the next file in the zip
                                                else:
                                                    log.info(f"Proceeding with {file_name}. Its latest competition ('{latest_comp_in_file.id}') is newer than the last processed one.")
                    except Exception as e:
                        log.error(f"Error during metadata pre-check for {file_name}: {e}. "
                                "Skipping processing of this file to be safe.")
                        continue

                    log.info(f"Starting full processing for {file_name}, including table clear and data insertion.")
                    
                    chunk_size = 10_000_000  # 10MB chunks
                    # Integer ceil division to calculate total_chunks
                    total_chunks = -(-len(file_bytes) // chunk_size) if len(file_bytes) > 0 else 0
                    headers = None

                    if total_chunks == 0:
                        log.info(f"File {file_name} is empty. Clearing 'results' table as per standard procedure, but no data will be inserted.")
                        with get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute('DELETE FROM results')
                        log.info(f"'results' table cleared due to processing empty file {file_name}.")
                    else:
                        # Clear the table BEFORE inserting new data (only if we are processing the file)
                        with get_connection() as conn:
                            with conn.cursor() as cur:
                                log.info(f"Clearing all data from 'results' table before inserting new data from {file_name}.")
                                cur.execute('DELETE FROM results') # Clear the table

                        for i in range(total_chunks):
                            start = i * chunk_size
                            end = (i + 1) * chunk_size
                            # Slice the bytes for the current chunk
                            chunk_bytes = file_bytes[start:end]
                            # Decode the chunk bytes to string
                            chunk_str = chunk_bytes.decode("utf-8", errors="ignore")
                            
                            log.info(f"Processing chunk {i + 1} of {total_chunks} for {file_name}")

                            current_df_chunk = None
                            if i == 0:
                                # First chunk: read it to get headers and data
                                current_df_chunk = pd.read_csv(
                                    io.StringIO(chunk_str),
                                    delimiter="\t",
                                    skip_blank_lines=True,
                                    na_values=["NULL"],
                                    low_memory=False
                                )
                                if not current_df_chunk.empty:
                                    headers = current_df_chunk.columns.tolist()
                                    log.debug(f"Headers extracted from first chunk: {headers}")
                                else:
                                    log.warning(f"First chunk of {file_name} is empty or resulted in an empty DataFrame. "
                                                "Headers might not be determined.")
                                    # If headers are not set, subsequent chunks will fail.
                                    # We break here as processing cannot continue reliably.
                                    if headers is None:
                                        log.error(f"Headers could not be determined from the first chunk of {file_name}. "
                                                "Aborting processing for this file.")
                                        break # Exit the chunk processing loop
                            else:
                                # Subsequent chunks: use previously determined headers
                                if headers is None:
                                    # This should not happen if the first chunk was processed correctly and was not empty.
                                    log.error(f"Headers not available for chunk {i + 1} of {file_name}. "
                                            "Skipping further processing of this file.")
                                    break # Exit the chunk processing loop

                                # Prepend the header string to the current data chunk string
                                # Ensure chunk_str itself does not contain headers.
                                chunk_with_headers_str = "\t".join(headers) + "\n" + chunk_str
                                current_df_chunk = pd.read_csv(
                                    io.StringIO(chunk_with_headers_str),
                                    delimiter="\t",
                                    skip_blank_lines=True,
                                    na_values=["NULL"],
                                    low_memory=False,
                                    header=0,        # The first line of chunk_with_headers_str is the header
                                    names=headers    # Enforce these column names; helps with malformed/empty chunks
                                )
                            
                            if current_df_chunk is None or current_df_chunk.empty:
                                log.info(f"Chunk {i + 1} (after parsing) is empty or resulted in an empty DataFrame. "
                                        "Skipping insertion for this chunk.")
                                continue

                            # Ensure all expected columns are present in the DataFrame (pandas with names=headers usually handles this)
                            # This is a defensive check.
                            for col in headers:
                                if col not in current_df_chunk.columns:
                                    current_df_chunk[col] = pd.NA # Add missing columns as NA

                            df_filtered = current_df_chunk[current_df_chunk["personCountryId"] == "Mexico"]

                            if df_filtered.empty:
                                log.info(f"Chunk {i + 1}: No rows for 'Mexico' after filtering.")
                                continue

                            # Prepare rows for batch insert, using original row["column"] access
                            rows_to_insert = []
                            for _, row in df_filtered.iterrows():
                                try:
                                    rows_to_insert.append((
                                        row["competitionId"], row["eventId"], row["roundTypeId"], row["pos"], row["best"],
                                        row["average"], row["personId"], row["formatId"], row["value1"], row["value2"],
                                        row["value3"], row["value4"], row["value5"], row["regionalSingleRecord"],
                                        row["regionalAverageRecord"]
                                    ))
                                except KeyError as e:
                                    log.error(f"KeyError: Missing column '{e}' in a row from chunk {i+1}. "
                                            f"Headers: {headers}. Row data (first few items): {row.iloc[:5].to_dict()}. Skipping this row.")
                                    continue # Skip this problematic row
                            
                            log.debug(f"Chunk {i + 1}: Prepared {len(rows_to_insert)} rows for insertion")

                            if rows_to_insert:
                                # Perform batch insert
                                with get_connection() as conn_insert: # Get a fresh connection for the insert if needed by your manager
                                    with conn_insert.cursor() as cur_insert:
                                        execute_values(
                                            cur_insert,
                                            """
                                            INSERT INTO results
                                            ("competitionId", "eventId", "roundTypeId", pos, best, average,
                                            "personId", "formatId", value1, value2, value3, value4, value5,
                                            "regionalSingleRecord", "regionalAverageRecord")
                                            VALUES %s
                                            ON CONFLICT DO NOTHING
                                            """,
                                            rows_to_insert
                                        )
                                log.info(f"Chunk {i + 1}: Inserted batch of {len(rows_to_insert)} rows into 'results' table for 'Mexico'.")
                            else:
                                log.info(f"Chunk {i + 1}: No valid rows to insert for 'Mexico' after preparing for batch.")
                        log.info(f"Finished processing all chunks for {file_name}.")

                        # --- Start: Update last competition with results metadata ---
                        try:
                            with get_connection() as conn:
                                with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                                    log.info("Finding the latest competition with results to update metadata.")
                                    # Find the competitionId with the most recent startDate from the results just inserted.
                                    cur.execute("""
                                        SELECT c.id
                                        FROM competitions c
                                        JOIN (SELECT DISTINCT "competitionId" FROM results) r ON c.id = r."competitionId"
                                        ORDER BY c."startDate" DESC
                                        LIMIT 1;
                                    """)
                                    latest_competition = cur.fetchone()

                                    if latest_competition:
                                        latest_competition_id = latest_competition.id
                                        log.info(f"Latest competition with results found: {latest_competition_id}. Updating metadata.")
                                        # Use ON CONFLICT to perform an "upsert"
                                        cur.execute("""
                                            INSERT INTO "exportMetadata" (key, value, "updatedAt")
                                            VALUES (%s, %s, NOW())
                                            ON CONFLICT (key) DO UPDATE
                                            SET value = EXCLUDED.value,
                                                "updatedAt" = EXCLUDED."updatedAt";
                                        """, ('last_competition', latest_competition_id))
                                    else:
                                        log.warning("Could not determine the latest competition from the results table.")
                        except Exception as e:
                            log.error(f"Failed to update 'last_competition' metadata: {e}")
                        # --- End: Update metadata ---
                # --- End of WCA_export_Results.tsv processing logic ---
        log.info("Database updated successfully")
        return jsonify({"success": True, "message": "Database updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating database"}), 500

@app.route("/update-state-ranks", methods=["POST"])
def update_state_ranks():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                # Reset stateRank values
                cur.execute('UPDATE "ranksSingle" SET "stateRank" = NULL')
                cur.execute('UPDATE "ranksAverage" SET "stateRank" = NULL')
                log.info("State ranks reset for ranksSingle and ranksAverage")

                # Fetch all states
                cur.execute('SELECT id, name FROM states')
                states = cur.fetchall()

                # Fetch events excluding EXCLUDED_EVENTS
                if EXCLUDED_EVENTS:
                    placeholders = ",".join(["%s"] * len(EXCLUDED_EVENTS))
                    query = f'SELECT id FROM events WHERE id NOT IN ({placeholders})'
                    cur.execute(query, EXCLUDED_EVENTS)
                else:
                    cur.execute('SELECT id FROM events')
                events = cur.fetchall()

                single_updates = []
                average_updates = []
                log.info("Starting computation of stateRank values for each state and event")

                for state_row in states:
                    state_name = state_row.name
                    log.info(f"Processing state: {state_name}")

                    for event_row in events:
                        # ranksSingle
                        cur.execute(
                            """
                            SELECT rs."personId", rs."eventId"
                            FROM "ranksSingle" rs
                            INNER JOIN persons p ON rs."personId" = p.id
                            LEFT JOIN states st ON p."stateId" = st.id
                            WHERE rs."countryRank" <> 0
                              AND rs."eventId" = %s
                              AND st.name = %s
                            ORDER BY rs."countryRank" ASC
                            """,
                            (event_row.id, state_name)
                        )
                        single_data = cur.fetchall()

                        single_state_rank = 1
                        for record in single_data:
                            single_updates.append({
                                "personId": record.personId,
                                "eventId": record.eventId,
                                "stateRank": single_state_rank
                            })
                            single_state_rank += 1

                        # ranksAverage
                        cur.execute(
                            """
                            SELECT ra."personId", ra."eventId"
                            FROM "ranksAverage" ra
                            INNER JOIN persons p ON ra."personId" = p.id
                            LEFT JOIN states st ON p."stateId" = st.id
                            WHERE ra."countryRank" <> 0
                              AND ra."eventId" = %s
                              AND st.name = %s
                            ORDER BY ra."countryRank" ASC
                            """,
                            (event_row.id, state_name)
                        )
                        average_data = cur.fetchall()

                        average_state_rank = 1
                        for record in average_data:
                            average_updates.append({
                                "personId": record.personId,
                                "eventId": record.eventId,
                                "stateRank": average_state_rank
                            })
                            average_state_rank += 1

                log.info(f"Computed {len(single_updates)} single_updates and {len(average_updates)} average_updates")

        # Apply updates in one transaction
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Create temp table
                cur.execute('CREATE TEMP TABLE tmp_updates (personId text, eventId text, stateRank int)')

                # Insert data in bulk
                psycopg2.extras.execute_values(
                    cur,
                    'INSERT INTO tmp_updates (personId, eventId, stateRank) VALUES %s',
                    [(u["personId"], u["eventId"], u["stateRank"]) for u in single_updates]
                )

                # Perform update
                cur.execute('''
                    UPDATE "ranksSingle" rs
                    SET "stateRank" = tmp_updates.stateRank
                    FROM tmp_updates
                    WHERE rs."personId" = tmp_updates.personId
                    AND rs."eventId" = tmp_updates.eventId
                ''')
                
                # Create temp table
                cur.execute('CREATE TEMP TABLE tmp_avg_updates (personId text, eventId text, stateRank int)')

                # Insert data in bulk
                psycopg2.extras.execute_values(
                    cur,
                    'INSERT INTO tmp_avg_updates (personId, eventId, stateRank) VALUES %s',
                    [(u["personId"], u["eventId"], u["stateRank"]) for u in average_updates]
                )

                # Perform update
                cur.execute('''
                    UPDATE "ranksAverage" ra
                    SET "stateRank" = tmp_avg_updates.stateRank
                    FROM tmp_avg_updates
                    WHERE ra."personId" = tmp_avg_updates.personId
                    AND ra."eventId" = tmp_avg_updates.eventId
                ''')

        log.info("State rankings updated successfully")
        return jsonify({"success": True, "message": "State rankings updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating state rankings"}), 500

@app.route("/update-sum-of-ranks", methods=["POST"])
def update_sum_of_ranks():
    try:
        log.info("Starting sum of ranks update")
        excluded = ",".join(f"'{e}'" for e in EXCLUDED_EVENTS)

        single_query = f"""
        WITH "allEvents" AS (
          SELECT DISTINCT "eventId" FROM "ranksSingle"
          WHERE "eventId" NOT IN ({excluded})
        ),
        "allPeople" AS (
          SELECT DISTINCT id, name FROM persons
        ),
        "peopleEvents" AS (
          SELECT "allPeople".id, "allPeople".name, "allEvents"."eventId"
          FROM "allPeople" CROSS JOIN "allEvents"
        )
        SELECT
          pe.id,
          pe.name,
          json_agg(
            json_build_object(
              'eventId', pe."eventId",
              'countryRank', COALESCE(rs."countryRank", wr."worstRank"),
              'completed', CASE WHEN rs."countryRank" IS NULL THEN false ELSE true END
            )
          ) AS events,
          SUM(COALESCE(rs."countryRank", wr."worstRank")) AS overall
        FROM "peopleEvents" pe
        LEFT JOIN "ranksSingle" rs 
            ON pe.id = rs."personId" AND pe."eventId" = rs."eventId"
        LEFT JOIN (
          SELECT "eventId", MAX("countryRank") + 1 AS "worstRank"
          FROM "ranksSingle"
          GROUP BY "eventId"
        ) AS wr 
            ON wr."eventId" = pe."eventId"
        GROUP BY pe.id, pe.name
        ORDER BY overall
        """

        average_query = f"""
        WITH "allEvents" AS (
          SELECT DISTINCT "eventId" FROM "ranksAverage"
          WHERE "eventId" NOT IN ({excluded})
        ),
        "allPeople" AS (
          SELECT DISTINCT id, name FROM persons
        ),
        "peopleEvents" AS (
          SELECT "allPeople".id, "allPeople".name, "allEvents"."eventId"
          FROM "allPeople" CROSS JOIN "allEvents"
        )
        SELECT
          pe.id,
          pe.name,
          json_agg(
            json_build_object(
              'eventId', pe."eventId",
              'countryRank', COALESCE(ra."countryRank", wr."worstRank"),
              'completed', CASE WHEN ra."countryRank" IS NULL THEN false ELSE true END
            )
          ) AS events,
          SUM(COALESCE(ra."countryRank", wr."worstRank")) AS overall
        FROM "peopleEvents" pe
        LEFT JOIN "ranksAverage" ra 
            ON pe.id = ra."personId" AND pe."eventId" = ra."eventId"
        LEFT JOIN (
          SELECT "eventId", MAX("countryRank") + 1 AS "worstRank"
          FROM "ranksAverage"
          GROUP BY "eventId"
        ) AS wr 
            ON wr."eventId" = pe."eventId"
        GROUP BY pe.id, pe.name
        ORDER BY overall
        """

        # Handle single results
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Deleting existing sumOfRanks records for single results")
                cur.execute('DELETE FROM "sumOfRanks" WHERE "resultType" = %s', ('single',))
                log.info("Executing single query")
                cur.execute(single_query)
                persons = cur.fetchall()
                log.info(f"Fetched {len(persons)} record(s) for single results")
                rank = 1
                for row in persons:
                    cur.execute(
                        """
                        INSERT INTO "sumOfRanks" (rank, "personId", "resultType", overall, events)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (rank, row.id, 'single', row.overall, psycopg2.extras.Json(row.events))
                    )
                    rank += 1

        # Handle average results
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Deleting existing sumOfRanks records for average results")
                cur.execute('DELETE FROM "sumOfRanks" WHERE "resultType" = %s', ('average',))
                log.info("Executing average query")
                cur.execute(average_query)
                persons = cur.fetchall()
                log.info(f"Fetched {len(persons)} record(s) for average results")
                rank = 1
                for row in persons:
                    cur.execute(
                        """
                        INSERT INTO "sumOfRanks" (rank, "personId", "resultType", overall, events)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (rank, row.id, 'average', row.overall, psycopg2.extras.Json(row.events))
                    )
                    rank += 1

        log.info("Sum of ranks updated successfully")
        return jsonify({"success": True, "message": "Sum of ranks updated successfully"})
    except Exception as e:
        log.error(f"Error updating sum of ranks: {e}")
        return jsonify({"success": False, "message": "Error updating sum of ranks"}), 500

@app.route("/update-kinch-ranks", methods=["POST"])
def update_kinch_ranks():
    try:
        log.info("Starting kinch ranks update")

        # Build CSV strings for excluded and single events
        excluded = ",".join(f"'{e}'" for e in EXCLUDED_EVENTS)
        single_events = ",".join(f"'{e}'" for e in SINGLE_EVENTS)

        query = f"""
        WITH PersonalRecords AS (
          SELECT
            "personId",
            "eventId",
            MIN(best) AS personal_best,
            'average' AS type
          FROM "ranksAverage"
          WHERE "eventId" NOT IN ({excluded})
          GROUP BY "personId", "eventId"
          UNION ALL
          SELECT
            "personId",
            "eventId",
            MIN(best) AS personal_best,
            'single' AS type
          FROM "ranksSingle"
          WHERE "eventId" IN ({single_events})
          GROUP BY "personId", "eventId"
        ),
        NationalRecords AS (
          SELECT
            "eventId",
            MIN(best) AS national_best,
            'average' AS type
          FROM "ranksAverage"
          WHERE "countryRank" = 1 AND "eventId" NOT IN ({excluded})
          GROUP BY "eventId"
          UNION ALL
          SELECT
            "eventId",
            MIN(best) AS national_best,
            'single' AS type
          FROM "ranksSingle"
          WHERE "countryRank" = 1 AND "eventId" IN ({single_events})
          GROUP BY "eventId"
        ),
        Persons AS (
          SELECT DISTINCT "personId" FROM "ranksSingle"
        ),
        Events AS (
          SELECT id FROM "events" WHERE id NOT IN ({excluded})
        ),
        Ratios AS (
          SELECT  
            p."personId",
            e.id AS "eventId",
            MAX(
                CASE 
                WHEN e.id = '333mbf' THEN
                    CASE 
                    WHEN COALESCE(pr.personal_best, 0) != 0 THEN 
                        ((99 - CAST(SUBSTRING(CAST(pr.personal_best AS TEXT), 1, 2) AS FLOAT) + 
                        (1 - (CAST(SUBSTRING(CAST(pr.personal_best AS TEXT), 3, 5) AS FLOAT) / 3600))) / 
                        ((99 - CAST(SUBSTRING(CAST(nr.national_best AS TEXT), 1, 2) AS FLOAT)) + 
                        (1 - (CAST(SUBSTRING(CAST(nr.national_best AS TEXT), 3, 5) AS FLOAT) / 3600)))) * 100
                    ELSE 0
                    END
                WHEN COALESCE(pr.personal_best, 0) != 0 THEN 
                    (nr.national_best / COALESCE(pr.personal_best, 0)::FLOAT) * 100
                    ELSE 0
                END
                ) AS best_ratio
          FROM Persons p
          CROSS JOIN Events e
          LEFT JOIN PersonalRecords pr ON p."personId" = pr."personId" AND e.id = pr."eventId"
          LEFT JOIN NationalRecords nr ON e.id = nr."eventId" AND pr.type = nr.type
          GROUP BY p."personId", e.id
        )
        SELECT 
          r."personId" AS id,
          json_agg(
            json_build_object(
              'eventId', r."eventId",
              'ratio', r.best_ratio
            )
          ) AS events,
          AVG(r.best_ratio) AS overall
        FROM Ratios r
        GROUP BY r."personId"
        ORDER BY overall DESC;
        """

        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Deleting existing kinchRanks records")
                # Delete existing kinchRanks records
                cur.execute('DELETE FROM "kinchRanks"')

                log.info("Executing kinch ranks query")
                # Execute the main query
                cur.execute(query)
                persons = cur.fetchall()
                log.info(f"Fetched {len(persons)} record(s) for updating kinch ranks")

                # Insert new results
                for index, row in enumerate(persons):
                    cur.execute(
                        """
                        INSERT INTO "kinchRanks" (rank, "personId", overall, events)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (index + 1, row.id, row.overall, psycopg2.extras.Json(row.events))
                    )

        log.info("Kinch ranks updated successfully")
        return jsonify({"success": True, "message": "Kinch ranks updated successfully"})
    except Exception as e:
        log.error(f"Error updating kinch ranks: {e}")
        return jsonify({"success": False, "message": "Error updating kinch ranks"}), 500

@app.route("/update-all", methods=["POST"])
def update_all():
    try:
        log.info("Starting all updates")
        updates = [
            ("update_full_database", update_full_database),
            ("update_state_ranks", update_state_ranks),
            ("update_sum_of_ranks", update_sum_of_ranks),
            ("update_kinch_ranks", update_kinch_ranks)
        ]
        details = {}
        for name, func in updates:
            log.info(f"Starting update: {name}")
            result = func()
            # Handle both tuple responses and Flask Response objects.
            if isinstance(result, tuple) and len(result) == 2:
                json_data, status_code = result
            else:
                json_data = result.get_json()
                status_code = result.status_code

            details[name] = {"status": status_code, "result": json_data}
            log.info(f"Completed update: {name} with status {status_code}")
            if status_code != 200:
                log.error(f"Error occurred during {name}: {json_data}")
                return jsonify({
                    "success": False,
                    "message": f"Error occurred during {name}",
                    "details": details
                }), status_code

        log.info("All updates executed successfully")
        return jsonify({
            "success": True,
            "message": "All updates executed successfully",
            "details": details
        })
    except Exception as e:
        log.error(f"Unhandled error in update_all: {e}")
        return jsonify({
            "success": False,
            "message": "Error occurred during update_all",
        }), 500

@app.route("/teams", methods=["GET"])
def get_teams():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching all teams")
                cur.execute("SELECT * FROM teams")
                teams = cur.fetchall()
                teams_list = [dict(team._asdict()) for team in teams]
                log.info(f"Fetched {len(teams_list)} team(s)")
        return jsonify({"success": True, "teams": teams_list})
    except Exception as e:
        log.error(f"Error fetching teams: {e}")
        return jsonify({"success": False, "message": "Error fetching teams"}), 500
    
@app.route("/teams/<state_id>", methods=["GET"])
def get_team_by_id(state_id):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info(f"Fetching team with ID: {state_id}")
                cur.execute("""SELECT * FROM teams WHERE "stateId" = %s""", (state_id,))
                team = cur.fetchone()
                if team:
                    team_data = dict(team._asdict())
                    log.info(f"Fetched team: {team_data}")
                    return jsonify({"success": True, "team": team_data})
                else:
                    log.warning(f"No team found with ID: {state_id}")
                    return jsonify({"success": False, "message": "Team not found"}), 404
    except Exception as e:
        log.error(f"Error fetching team by ID: {e}")
        return jsonify({"success": False, "message": "Error fetching team"}), 500

@app.route("/rank/<state_id>/<type>/<event_id>", methods=["GET"])
def get_rank(state_id, type, event_id):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info(f"Fetching ranks for state: {state_id}, type: {type}, event: {event_id}")
                
                # Determine the table to query based on the type
                table_name = "ranksSingle" if type == "single" else "ranksAverage"
                
                # Query to join persons table to get stateId
                cur.execute(f"""
                    SELECT rs."personId", rs."eventId", rs.best, rs."worldRank", rs."continentRank", rs."countryRank", rs."stateRank"
                    FROM "{table_name}" rs
                    INNER JOIN persons p ON rs."personId" = p.id
                    WHERE p."stateId" = %s AND rs."eventId" = %s
                """, (state_id, event_id))
                
                ranks = cur.fetchall()
                if ranks:
                    # Format the response as an array of rank data
                    rank_data = [
                        {
                            "rankType": type,
                            "personId": rank.personId,
                            "eventId": rank.eventId,
                            "best": rank.best,
                            "rank": {
                                "world": rank.worldRank,
                                "continent": rank.continentRank,
                                "country": rank.countryRank,
                                "state": rank.stateRank
                            }
                        }
                        for rank in ranks
                    ]
                    log.info(f"Fetched {len(rank_data)} ranks")
                    return jsonify({"success": True, "ranks": rank_data})
                else:
                    log.warning(f"No ranks found for state: {state_id}, type: {type}, event: {event_id}")
                    return jsonify({"success": False, "message": "Ranks not found"}), 404
    except Exception as e:
        log.error(f"Error fetching ranks: {e}")
        return jsonify({"success": False, "message": "Error fetching ranks"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)