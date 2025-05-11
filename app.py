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

@app.route("/update-database", methods=["POST"])
def update_full_database():
    url = "https://www.worldcubeassociation.org/export/results/WCA_export.tsv.zip"
    try:
        response = requests.get(url)
        response.raise_for_status()
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

                elif file_name == "WCA_export_Results.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_bytes = z.read(file_name)
                    chunk_size = 10_000_000
                    total_chunks = -(-len(file_bytes) // chunk_size)
                    headers = None
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute('DELETE FROM results')  # Clear the table before inserting new data
                        for i in range(total_chunks):
                            start = i * chunk_size
                            end = (i + 1) * chunk_size
                            chunk = file_bytes[start:end].decode("utf-8", errors="ignore")
                            log.info(f"Processing chunk {i + 1} of {total_chunks}")
                            if i == 0:
                                parsed = pd.read_csv(
                                    io.StringIO(chunk),
                                    delimiter="\t",
                                    skip_blank_lines=True,
                                    na_values=["NULL"],
                                    low_memory=False
                                )
                                headers = parsed.columns.tolist()
                            chunk_with_headers = "\t".join(headers) + "\n" + chunk
                            df_chunk = pd.read_csv(
                                io.StringIO(chunk_with_headers),
                                delimiter="\t",
                                skip_blank_lines=True,
                                na_values=["NULL"],
                                low_memory=False
                            )
                            df_filtered = df_chunk[df_chunk["personCountryId"] == "Mexico"]

                            # Prepare rows for batch insert
                            rows_to_insert = [
                                (
                                    row["competitionId"], row["eventId"], row["roundTypeId"], row["pos"], row["best"],
                                    row["average"], row["personId"], row["formatId"], row["value1"], row["value2"],
                                    row["value3"], row["value4"], row["value5"], row["regionalSingleRecord"],
                                    row["regionalAverageRecord"]
                                )
                                for _, row in df_filtered.iterrows()
                            ]
                            log.debug(f"Chunk {i + 1}: Prepared {len(rows_to_insert)} rows for insertion")

                            # Perform batch insert
                            with get_connection() as conn:
                                with conn.cursor() as cur:
                                    execute_values(
                                        cur,
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
                            log.info(f"Chunk {i + 1}: Inserted batch into 'results' table")
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)