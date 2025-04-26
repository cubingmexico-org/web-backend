import re
import io
import zipfile
import requests
import logging
import pandas as pd
from datetime import datetime
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
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
engine = create_engine(DB_URL)

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
                # Process Competitions file
                if file_name == "WCA_export_Competitions.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    cleaned_content = file_content.replace('"', '')
                    df = pd.read_csv(io.StringIO(cleaned_content),
                                     delimiter="\t", 
                                     na_values=["NULL"])
                    competitions = df.to_dict(orient="records")
                    with engine.begin() as conn:
                        states = conn.execute(text("SELECT * FROM states")).fetchall()
                        delegates = conn.execute(text("SELECT id FROM delegates")).fetchall()
                        organisers = conn.execute(text("SELECT id FROM organisers")).fetchall()
                        existing = conn.execute(text("SELECT id FROM competitions")).fetchall()
                        existing_ids = {row.id for row in existing}
                        for row in competitions:
                            if row["id"] in existing_ids:
                                continue
                            state_id = None
                            if row["countryId"] == "Mexico":
                                state_name = get_state_from_coordinates(row["latitude"] / 1000000,
                                                                       row["longitude"] / 1000000)
                                if state_name:
                                    for s in states:
                                        if s.name == state_name:
                                            state_id = s.id
                                            break
                            start_date = datetime(row["year"], row["month"], row["day"])
                            end_date = datetime(row["year"], row["endMonth"], row["endDay"])
                            conn.execute(text("""
                                INSERT INTO competitions 
                                (id, name, "cityName", "countryId", information, "startDate", "endDate", cancelled, venue, "venueAddress", "venueDetails", external_website, "cellName", latitude, longitude, "stateId")
                                VALUES (:id, :name, :cityName, :countryId, :information, :startDate, :endDate, :cancelled, :venue, :venueAddress, :venueDetails, :external_website, :cellName, :latitude, :longitude, :stateId)
                                ON CONFLICT DO NOTHING
                            """), {
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
                            })
                            if row["countryId"] == "Mexico":
                                # Process eventSpecs into competitionEvent table
                                for event_spec in str(row["eventSpecs"]).split():
                                    conn.execute(text("""
                                        INSERT INTO competition_events (competitionId, eventId)
                                        VALUES (:competitionId, :eventId)
                                        ON CONFLICT DO NOTHING
                                    """), {
                                        "competitionId": row["id"],
                                        "eventId": event_spec
                                    })
                                # Process organiser information using regex
                                organiser_pattern = re.compile(r"\{([^}]+)\}\{mailto:([^}]+)\}")
                                for match in organiser_pattern.finditer(str(row["organiser"])):
                                    organiser_name = match.group(1)
                                    organiser_email = match.group(2)
                                    exists = any(o.id == organiser_email for o in organisers)
                                    person_res = conn.execute(text("""
                                        SELECT id FROM persons WHERE name = :name
                                    """), {"name": organiser_name}).fetchone()
                                    person_id = person_res.id if person_res else None
                                    if not exists:
                                        conn.execute(text("""
                                            INSERT INTO organisers (id, "personId", status)
                                            VALUES (:id, :personId, 'active')
                                            ON CONFLICT DO NOTHING
                                        """), {"id": organiser_email, "personId": person_id})
                                    conn.execute(text("""
                                        INSERT INTO competition_organisers ("competitionId", "organiserId")
                                        VALUES (:competitionId, :organiserId)
                                        ON CONFLICT DO NOTHING
                                    """), {"competitionId": row["id"], "organiserId": organiser_email})
                                # Process delegate information using regex
                                delegate_pattern = re.compile(r"\{([^}]+)\}\{mailto:([^}]+)\}")
                                for match in delegate_pattern.finditer(str(row["wcaDelegate"])):
                                    delegate_name = match.group(1)
                                    delegate_email = match.group(2)
                                    exists = any(d.id == delegate_email for d in delegates)
                                    person_res = conn.execute(text("""
                                        SELECT id FROM persons WHERE name = :name
                                    """), {"name": delegate_name}).fetchone()
                                    person_id = person_res.id if person_res else None
                                    if not exists and person_id:
                                        conn.execute(text("""
                                            INSERT INTO delegates (id, "personId", status)
                                            VALUES (:id, :personId, 'active')
                                            ON CONFLICT DO NOTHING
                                        """), {"id": delegate_email, "personId": person_id})
                                    if exists or person_id:
                                        conn.execute(text("""
                                            INSERT INTO competition_delegates ("competitionId", "delegateId")
                                            VALUES (:competitionId, :delegateId)
                                            ON CONFLICT DO NOTHING
                                        """), {"competitionId": row["id"], "delegateId": delegate_email})
                # Process Events file
                elif file_name == "WCA_export_Events.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True)
                    events = df.to_dict(orient="records")
                    with engine.begin() as conn:
                        for row in events:
                            conn.execute(text("""
                                INSERT INTO events (id, format, name, rank, "cellName")
                                VALUES (:id, :format, :name, :rank, :cellName)
                                ON CONFLICT DO NOTHING
                            """), {
                                "id": row["id"],
                                "format": row["format"],
                                "name": row["name"],
                                "rank": row["rank"],
                                "cellName": row["cellName"]
                            })
                # Process Persons file
                elif file_name == "WCA_export_Persons.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True, na_values=["NULL"])
                    persons = df.to_dict(orient="records")
                    cleaned_persons = []
                    for p in persons:
                        if p["countryId"] == "Mexico":
                            gender = p.get("gender")
                            # pandas NaN becomes None, valid genders stay as 1-char strings
                            gender = None if pd.isna(gender) else str(gender)
                            cleaned_persons.append({
                                "id": p["id"],
                                "name": p["name"],
                                "gender": gender
                            })
                    with engine.begin() as conn:
                        for row in cleaned_persons:
                            conn.execute(text("""
                                INSERT INTO persons (id, name, gender)
                                VALUES (:id, :name, :gender)
                                ON CONFLICT DO NOTHING
                            """), {
                                "id": row["id"],
                                "name": row["name"],
                                "gender": row["gender"]
                            })
                # Process RanksAverage file
                elif file_name == "WCA_export_RanksAverage.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True, low_memory=False)
                    data = df.to_dict(orient="records")
                    # Fetch person ids from DB
                    with engine.begin() as conn:
                        persons = conn.execute(text("SELECT id FROM persons")).fetchall()
                        person_ids = {p.id for p in persons}
                        filtered = [d for d in data if d["personId"] in person_ids]
                        conn.execute(text("""
                            DELETE FROM "ranksAverage"
                        """))
                        for row in filtered:
                            conn.execute(text("""
                                INSERT INTO "ranksAverage" ("personId", "eventId", best, "worldRank", "continentRank", "countryRank")
                                VALUES (:personId, :eventId, :best, :worldRank, :continentRank, :countryRank)
                                ON CONFLICT DO NOTHING
                            """), {
                                "personId": row["personId"],
                                "eventId": row["eventId"],
                                "best": row["best"],
                                "worldRank": row["worldRank"],
                                "continentRank": row["continentRank"],
                                "countryRank": row["countryRank"]
                            })
                # Process RanksSingle file
                elif file_name == "WCA_export_RanksSingle.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True, low_memory=False)
                    data = df.to_dict(orient="records")
                    with engine.begin() as conn:
                        persons = conn.execute(text("SELECT id FROM persons")).fetchall()
                        person_ids = {p.id for p in persons}
                        filtered = [d for d in data if d["personId"] in person_ids]
                        conn.execute(text("""
                            DELETE FROM "ranksSingle"
                        """))
                        for row in filtered:
                            conn.execute(text("""
                                INSERT INTO "ranksSingle" ("personId", "eventId", best, "worldRank", "continentRank", "countryRank")
                                VALUES (:personId, :eventId, :best, :worldRank, :continentRank, :countryRank)
                                ON CONFLICT DO NOTHING
                            """), {
                                "personId": row["personId"],
                                "eventId": row["eventId"],
                                "best": row["best"],
                                "worldRank": row["worldRank"],
                                "continentRank": row["continentRank"],
                                "countryRank": row["countryRank"]
                            })
                # Process Results file
                elif file_name == "WCA_export_Results.tsv":
                    log.info(f"Processing file: {file_name}")
                    file_bytes = z.read(file_name)
                    chunk_size = 10_000_000
                    total_chunks = -(-len(file_bytes) // chunk_size)  # ceiling division
                    headers = None
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM results"))
                        for i in range(total_chunks):
                            start = i * chunk_size
                            end = (i + 1) * chunk_size
                            chunk = file_bytes[start:end].decode("utf-8", errors="ignore")
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
                            for _, row in df_filtered.iterrows():
                                conn.execute(text("""
                                    INSERT INTO results 
                                    ("competitionId", "eventId", "roundTypeId", pos, best, average, "personId", "formatId", value1, value2, value3, value4, value5, "regionalSingleRecord", "regionalAverageRecord")
                                    VALUES (:competitionId, :eventId, :roundTypeId, :pos, :best, :average, :personId, :formatId, :value1, :value2, :value3, :value4, :value5, :regionalSingleRecord, :regionalAverageRecord)
                                    ON CONFLICT DO NOTHING
                                """), {
                                    "competitionId": row["competitionId"],
                                    "eventId": row["eventId"],
                                    "roundTypeId": row["roundTypeId"],
                                    "pos": row["pos"],
                                    "best": row["best"],
                                    "average": row["average"],
                                    "personId": row["personId"],
                                    "formatId": row["formatId"],
                                    "value1": row["value1"],
                                    "value2": row["value2"],
                                    "value3": row["value3"],
                                    "value4": row["value4"],
                                    "value5": row["value5"],
                                    "regionalSingleRecord": row["regionalSingleRecord"],
                                    "regionalAverageRecord": row["regionalAverageRecord"]
                                })
        log.info("Database updated successfully")
        return jsonify({"success": True, "message": "Database updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating database"}), 500

@app.route("/update-state-ranks", methods=["POST"])
def update_state_ranks():
    try:
        with engine.begin() as conn:
            # Reset stateRank values
            conn.execute(text("""UPDATE "ranksSingle" SET "stateRank" = NULL"""))
            conn.execute(text("""UPDATE "ranksAverage" SET "stateRank" = NULL"""))
            log.info("State ranks reset for ranksSingle and ranksAverage")

            # Fetch all states
            states = conn.execute(text("SELECT id, name FROM states")).fetchall()
            log.info(f"Fetched {len(states)} states")

            # Fetch events excluding EXCLUDED_EVENTS
            # Use a tuple for parameter binding; if empty, fetch all events.
            if EXCLUDED_EVENTS:
                events = conn.execute(
                    text("SELECT id FROM events WHERE id NOT IN :excluded")
                    .bindparams(excluded=tuple(EXCLUDED_EVENTS))
                ).fetchall()
            else:
                events = conn.execute(text("SELECT id FROM events")).fetchall()
            log.info(f"Fetched {len(events)} events (excluded: {EXCLUDED_EVENTS})")

            # Prepare lists to hold update data for single and average ranks
            single_updates = []
            average_updates = []
            log.info("Starting computation of stateRank values for each state and event")

            # Loop through each state and event
            for state_row in states:
                log.info(f"Processing state: {state_row.name}")
                state_name = state_row.name
                for event_row in events:
                    log.info(f"  - Event: {event_row.id}")
                    # Process ranksSingle updates
                    single_data = conn.execute(text("""
                        SELECT rs."personId", rs."eventId"
                        FROM "ranksSingle" rs
                        INNER JOIN persons p ON rs."personId" = p.id
                        LEFT JOIN states st ON p."stateId" = st.id
                        WHERE rs."countryRank" <> 0
                          AND rs."eventId" = :event_id
                          AND st.name = :state_name
                        ORDER BY rs."countryRank" ASC
                    """), {"event_id": event_row.id, "state_name": state_name}).fetchall()

                    single_state_rank = 1
                    for record in single_data:
                        single_updates.append({
                            "personId": record.personId,
                            "eventId": record.eventId,
                            "stateRank": single_state_rank
                        })
                        single_state_rank += 1

                    # Process ranksAverage updates
                    average_data = conn.execute(text("""
                        SELECT ra."personId", ra."eventId"
                        FROM "ranksAverage" ra
                        INNER JOIN persons p ON ra."personId" = p.id
                        LEFT JOIN states st ON p."stateId" = st.id
                        WHERE ra."countryRank" <> 0
                          AND ra."eventId" = :event_id
                          AND st.name = :state_name
                        ORDER BY ra."countryRank" ASC
                    """), {"event_id": event_row.id, "state_name": state_name}).fetchall()

                    average_state_rank = 1
                    for record in average_data:
                        average_updates.append({
                            "personId": record.personId,
                            "eventId": record.eventId,
                            "stateRank": average_state_rank
                        })
                        average_state_rank += 1

            log.info(f"Computed {len(single_updates)} single_updates and {len(average_updates)} average_updates")

            # Execute updates in a transaction
            with engine.begin() as conn_tx:
                log.info("Applying single stateRank updates")
                if single_updates:
                    single_values = ", ".join(
                        f"('{u['personId']}', '{u['eventId']}', {u['stateRank']})"
                        for u in single_updates
                    )
                    conn_tx.execute(text(f'''
                        UPDATE "ranksSingle" rs
                        SET "stateRank" = updates."stateRank"
                        FROM (
                            VALUES {single_values}
                        ) AS updates("personId", "eventId", "stateRank")
                        WHERE rs."personId" = updates."personId"
                        AND rs."eventId" = updates."eventId"
                    '''))
                log.info("Applying average stateRank updates")
                if average_updates:
                    average_values = ", ".join(
                        f"('{u['personId']}', '{u['eventId']}', {u['stateRank']})"
                        for u in average_updates
                    )
                    conn_tx.execute(text(f'''
                        UPDATE "ranksAverage" ra
                        SET "stateRank" = updates."stateRank"
                        FROM (
                            VALUES {average_values}
                        ) AS updates("personId", "eventId", "stateRank")
                        WHERE ra."personId" = updates."personId"
                        AND ra."eventId" = updates."eventId"
                    '''))

        log.info("State rankings updated successfully")
        return jsonify({"success": True, "message": "State rankings updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating state rankings"}), 500

@app.route("/update-sum-of-ranks", methods=["POST"])
def update_sum_of_ranks():
    try:
        # Build a CSV string for the excluded events
        excluded = ",".join(f"'{e}'" for e in EXCLUDED_EVENTS)

        # Query for single results
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
          SELECT "eventId", MAX("countryRank") + 1 as "worstRank"
          FROM public."ranksSingle"
          GROUP BY "eventId"
        ) AS wr 
            ON wr."eventId" = pe."eventId"
        GROUP BY pe.id, pe.name
        ORDER BY overall
        """

        # Query for average results
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
          SELECT "eventId", MAX("countryRank") + 1 as "worstRank"
          FROM public."ranksAverage"
          GROUP BY "eventId"
        ) AS wr 
            ON wr."eventId" = pe."eventId"
        GROUP BY pe.id, pe.name
        ORDER BY overall
        """

        # Process single results in a transaction
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM sumOfRanks WHERE resultType = 'single'"))
            result = conn.execute(text(single_query))
            persons = result.fetchall()
            for index, row in enumerate(persons):
                conn.execute(text("""
                    INSERT INTO sumOfRanks (rank, personId, resultType, overall, events)
                    VALUES (:rank, :personId, :resultType, :overall, :events)
                """), {
                    "rank": index + 1,
                    "personId": row.id,
                    "resultType": "single",
                    "overall": row.overall,
                    "events": row.events
                })

        # Process average results in a transaction
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM sumOfRanks WHERE resultType = 'average'"))
            result = conn.execute(text(average_query))
            persons = result.fetchall()
            for index, row in enumerate(persons):
                conn.execute(text("""
                    INSERT INTO sumOfRanks (rank, personId, resultType, overall, events)
                    VALUES (:rank, :personId, :resultType, :overall, :events)
                """), {
                    "rank": index + 1,
                    "personId": row.id,
                    "resultType": "average",
                    "overall": row.overall,
                    "events": row.events
                })

        log.info("Sum of ranks updated successfully")
        return jsonify({"success": True, "message": "Sum of ranks updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating sum of ranks"}), 500

@app.route("/update-kinch-ranks", methods=["POST"])
def update_kinch_ranks():
    try:
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
                      (1 - (CAST(SUBSTRING(CAST(nr.national_best AS TEXT), 3, 5) AS FLOAT) / 3600))) * 100
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

        with engine.begin() as conn:
            # Delete all existing kinchRanks records
            conn.execute(text("DELETE FROM kinchRanks"))

            result = conn.execute(text(query))
            persons = result.fetchall()

            for index, row in enumerate(persons):
                conn.execute(text("""
                    INSERT INTO kinchRanks (rank, personId, overall, events)
                    VALUES (:rank, :personId, :overall, :events)
                """), {
                    "rank": index + 1,
                    "personId": row.id,
                    "overall": row.overall,
                    "events": row.events
                })

        log.info("Kinch ranks updated successfully")
        return jsonify({"success": True, "message": "Kinch ranks updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating kinch ranks"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)