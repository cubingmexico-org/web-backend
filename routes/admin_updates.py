import io
import re
import zipfile
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from flask import Blueprint, jsonify
from psycopg2.extras import execute_values

from common import EXCLUDED_EVENTS, SINGLE_EVENTS, get_connection, log, require_cron_auth
from utils import get_state_from_coordinates

admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/update-database", methods=["POST"])
@require_cron_auth
def update_full_database():
    url = "https://www.worldcubeassociation.org/export/results/v2/tsv"
    try:
        log.info("Fetching data from %s", url)
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as e:
        return jsonify({"error": f"Failed to fetch zip file: {e}"}), 500

    zip_bytes = io.BytesIO(response.content)
    try:
        with zipfile.ZipFile(zip_bytes, "r") as z:
            file_processing_order = [
                "WCA_export_events.tsv",
                "WCA_export_formats.tsv",
                "WCA_export_round_types.tsv",
                "WCA_export_persons.tsv",
                "WCA_export_competitions.tsv",
                "WCA_export_championships.tsv",
                "WCA_export_ranks_single.tsv",
                "WCA_export_ranks_average.tsv",
                "WCA_export_results.tsv",
                "WCA_export_result_attempts.tsv",
            ]

            available_files = set(z.namelist())

            for file_name in file_processing_order:
                if file_name not in available_files:
                    log.warning("Expected file %s not found in zip archive. Skipping.", file_name)
                    continue
                if file_name == "WCA_export_competitions.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")
                    cleaned_content = file_content.replace('"', "")
                    df = pd.read_csv(io.StringIO(cleaned_content), delimiter="\t", na_values=["NULL"])
                    competitions = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                            cur.execute("SELECT * FROM states")
                            states = cur.fetchall()

                            cur.execute("SELECT id FROM delegates")
                            delegates = cur.fetchall()

                            cur.execute("SELECT id FROM organizers")
                            organizers = cur.fetchall()

                            cur.execute("SELECT id FROM competitions")
                            existing = cur.fetchall()
                            existing_ids = {row.id for row in existing}

                            for row in competitions:
                                if row["id"] in existing_ids:
                                    continue
                                state_id = None
                                if row["country_id"] == "Mexico":
                                    state_name = get_state_from_coordinates(
                                        row["latitude_microdegrees"] / 1000000,
                                        row["longitude_microdegrees"] / 1000000,
                                    )
                                    if state_name:
                                        for s in states:
                                            if s.name == state_name:
                                                state_id = s.id
                                                break
                                start_date = datetime(row["year"], row["month"], row["day"])
                                end_date = datetime(row["year"], row["end_month"], row["end_day"])
                                cur.execute(
                                    """
                                    INSERT INTO competitions
                                    (id, name, city_name, country_id, information, start_date, end_date, cancelled,
                                     venue, venue_address, venue_details, external_website, cell_name,
                                     latitude_microdegrees, longitude_microdegrees, state_id)
                                    VALUES (%(id)s, %(name)s, %(city_name)s, %(country_id)s, %(information)s,
                                            %(start_date)s, %(end_date)s, %(cancelled)s, %(venue)s,
                                            %(venue_address)s, %(venue_details)s, %(external_website)s,
                                            %(cell_name)s, %(latitude_microdegrees)s, %(longitude_microdegrees)s, %(state_id)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "name": row["name"],
                                        "city_name": row["city_name"],
                                        "country_id": row["country_id"],
                                        "information": row["information"],
                                        "start_date": start_date,
                                        "end_date": end_date,
                                        "cancelled": bool(row["cancelled"]),
                                        "venue": row["venue"],
                                        "venue_address": row["venue_address"],
                                        "venue_details": row["venue_details"],
                                        "external_website": row["external_website"],
                                        "cell_name": row["cell_name"],
                                        "latitude_microdegrees": row["latitude_microdegrees"],
                                        "longitude_microdegrees": row["longitude_microdegrees"],
                                        "state_id": state_id,
                                    },
                                )
                                if row["country_id"] == "Mexico":
                                    for event_spec in str(row["event_specs"]).split():
                                        cur.execute(
                                            """
                                            INSERT INTO competition_events (competition_id, event_id)
                                            VALUES (%(competition_id)s, %(event_id)s)
                                            ON CONFLICT DO NOTHING
                                            """,
                                            {
                                                "competition_id": row["id"],
                                                "event_id": event_spec,
                                            },
                                        )

                                    organizer_pattern = re.compile(r"\{([^}]+)\}\{mailto:([^}]+)\}")
                                    for match in organizer_pattern.finditer(str(row["organizers"])):
                                        organizer_name = match.group(1)
                                        organizer_email = match.group(2)
                                        exists = any(o.id == organizer_email for o in organizers)
                                        cur.execute(
                                            "SELECT wca_id FROM persons WHERE name = %s",
                                            (organizer_name,),
                                        )
                                        person_res = cur.fetchone()
                                        person_id = person_res.wca_id if person_res else None
                                        if not exists:
                                            cur.execute(
                                                """
                                                INSERT INTO organizers (id, person_id, status)
                                                VALUES (%(id)s, %(person_id)s, 'active')
                                                ON CONFLICT DO NOTHING
                                                """,
                                                {"id": organizer_email, "person_id": person_id},
                                            )
                                        cur.execute(
                                            """
                                            INSERT INTO competition_organizers (competition_id, organizer_id)
                                            VALUES (%(competition_id)s, %(organizer_id)s)
                                            ON CONFLICT DO NOTHING
                                            """,
                                            {
                                                "competition_id": row["id"],
                                                "organizer_id": organizer_email,
                                            },
                                        )

                                    delegate_pattern = re.compile(r"\{([^}]+)\}\{mailto:([^}]+)\}")
                                    for match in delegate_pattern.finditer(str(row["delegates"])):
                                        delegate_name = match.group(1)
                                        delegate_email = match.group(2)
                                        exists = any(d.id == delegate_email for d in delegates)
                                        cur.execute(
                                            "SELECT wca_id FROM persons WHERE name = %s",
                                            (delegate_name,),
                                        )
                                        person_res = cur.fetchone()
                                        person_id = person_res.wca_id if person_res else None
                                        if not exists and person_id:
                                            cur.execute(
                                                """
                                                INSERT INTO delegates (id, person_id, status)
                                                VALUES (%(id)s, %(person_id)s, 'active')
                                                ON CONFLICT DO NOTHING
                                                """,
                                                {"id": delegate_email, "person_id": person_id},
                                            )
                                        if exists or person_id:
                                            cur.execute(
                                                """
                                                INSERT INTO competition_delegates (competition_id, delegate_id)
                                                VALUES (%(competition_id)s, %(delegate_id)s)
                                                ON CONFLICT DO NOTHING
                                                """,
                                                {
                                                    "competition_id": row["id"],
                                                    "delegate_id": delegate_email,
                                                },
                                            )

                elif file_name == "WCA_export_championships.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")
                    cleaned_content = file_content.replace('"', "")
                    df = pd.read_csv(
                        io.StringIO(cleaned_content), delimiter="\t", skip_blank_lines=True, na_values=["NULL"]
                    )
                    championships = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in championships:
                                cur.execute(
                                    """
                                    INSERT INTO championships (id, competition_id, championship_type)
                                    VALUES (%(id)s, %(competition_id)s, %(championship_type)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "competition_id": row["competition_id"],
                                        "championship_type": row["championship_type"],
                                    },
                                )

                elif file_name == "WCA_export_events.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True)
                    events = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in events:
                                cur.execute(
                                    """
                                    INSERT INTO events (id, format, name, rank)
                                    VALUES (%(id)s, %(format)s, %(name)s, %(rank)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "format": row["format"],
                                        "name": row["name"],
                                        "rank": row["rank"],
                                    },
                                )

                elif file_name == "WCA_export_round_types.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True)
                    round_types = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in round_types:
                                cur.execute(
                                    """
                                    INSERT INTO round_types (id, final, name, rank, cell_name)
                                    VALUES (%(id)s, %(final)s, %(name)s, %(rank)s, %(cell_name)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "final": bool(row["final"]),
                                        "name": row["name"],
                                        "rank": row["rank"],
                                        "cell_name": row["cell_name"],
                                    },
                                )

                elif file_name == "WCA_export_formats.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(io.StringIO(file_content), delimiter="\t", skip_blank_lines=True)
                    formats = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in formats:
                                cur.execute(
                                    """
                                    INSERT INTO formats (id, expected_solve_count, name, sort_by, sort_by_second, trim_fastest_n, trim_slowest_n)
                                    VALUES (%(id)s, %(expected_solve_count)s, %(name)s, %(sort_by)s, %(sort_by_second)s, %(trim_fastest_n)s, %(trim_slowest_n)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    {
                                        "id": row["id"],
                                        "expected_solve_count": row["expected_solve_count"],
                                        "name": row["name"],
                                        "sort_by": row["sort_by"],
                                        "sort_by_second": row["sort_by_second"],
                                        "trim_fastest_n": bool(row["trim_fastest_n"]),
                                        "trim_slowest_n": bool(row["trim_slowest_n"]),
                                    },
                                )

                elif file_name == "WCA_export_persons.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")
                    df = pd.read_csv(
                        io.StringIO(file_content), delimiter="\t", skip_blank_lines=True, na_values=["NULL"]
                    )
                    persons = df.to_dict(orient="records")
                    cleaned_persons = []
                    for p in persons:
                        if p["country_id"] == "Mexico":
                            gender = p.get("gender")
                            gender = None if pd.isna(gender) else str(gender)
                            cleaned_persons.append(
                                {"wca_id": p["wca_id"], "name": p["name"], "gender": gender}
                            )
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            for row in cleaned_persons:
                                cur.execute(
                                    """
                                    INSERT INTO persons (wca_id, name, gender)
                                    VALUES (%(wca_id)s, %(name)s, %(gender)s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    row,
                                )

                elif file_name == "WCA_export_ranks_average.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")

                    if not file_content.strip() or len(file_content.strip().split("\n")) <= 1:
                        log.info("File %s is empty or contains only headers. Skipping processing.", file_name)
                        continue

                    df = pd.read_csv(
                        io.StringIO(file_content), delimiter="\t", skip_blank_lines=True, low_memory=False
                    )

                    if df.empty:
                        log.info("File %s resulted in empty DataFrame. Skipping processing.", file_name)
                        continue

                    data = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                            cur.execute("SELECT wca_id FROM persons")
                            persons = cur.fetchall()
                            person_ids = {p.wca_id for p in persons}
                            filtered = [d for d in data if d["person_id"] in person_ids]
                            cur.execute("DELETE FROM ranks_average")

                            rows_to_insert = [
                                (
                                    row["person_id"],
                                    row["event_id"],
                                    row["best"],
                                    row["world_rank"],
                                    row["continent_rank"],
                                    row["country_rank"],
                                )
                                for row in filtered
                            ]

                            execute_values(
                                cur,
                                """
                                INSERT INTO ranks_average
                                (person_id, event_id, best, world_rank, continent_rank, country_rank)
                                VALUES %s
                                ON CONFLICT DO NOTHING
                                """,
                                rows_to_insert,
                            )

                elif file_name == "WCA_export_ranks_single.tsv":
                    log.info("Processing file: %s", file_name)
                    file_content = z.read(file_name).decode("utf-8")

                    if not file_content.strip() or len(file_content.strip().split("\n")) <= 1:
                        log.info("File %s is empty or contains only headers. Skipping processing.", file_name)
                        continue

                    df = pd.read_csv(
                        io.StringIO(file_content), delimiter="\t", skip_blank_lines=True, low_memory=False
                    )

                    if df.empty:
                        log.info("File %s resulted in empty DataFrame. Skipping processing.", file_name)
                        continue

                    data = df.to_dict(orient="records")
                    with get_connection() as conn:
                        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                            cur.execute("SELECT wca_id FROM persons")
                            persons = cur.fetchall()
                            person_ids = {p.wca_id for p in persons}
                            filtered = [d for d in data if d["person_id"] in person_ids]
                            cur.execute("DELETE FROM ranks_single")

                            rows_to_insert = [
                                (
                                    row["person_id"],
                                    row["event_id"],
                                    row["best"],
                                    row["world_rank"],
                                    row["continent_rank"],
                                    row["country_rank"],
                                )
                                for row in filtered
                            ]

                            execute_values(
                                cur,
                                """
                                INSERT INTO ranks_single
                                (person_id, event_id, best, world_rank, continent_rank, country_rank)
                                VALUES %s
                                ON CONFLICT DO NOTHING
                                """,
                                rows_to_insert,
                            )

                elif file_name == "WCA_export_results.tsv":
                    log.info("Evaluating file for processing: %s", file_name)
                    file_bytes = z.read(file_name)

                    try:
                        log.info("Pre-checking personIds in %s against the database.", file_name)
                        with get_connection() as conn:
                            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                                cur.execute("SELECT wca_id FROM persons")
                                db_persons = cur.fetchall()
                                db_person_ids = {p.wca_id for p in db_persons}

                        df_results_persons = pd.read_csv(
                            io.BytesIO(file_bytes),
                            delimiter="\t",
                            usecols=["person_id", "person_country_id"],
                            skip_blank_lines=True,
                            na_values=["NULL"],
                            low_memory=False,
                        )

                        df_mexico_results = df_results_persons[
                            df_results_persons["person_country_id"] == "Mexico"
                        ]
                        file_person_ids = set(df_mexico_results["person_id"].unique())

                        missing_person_ids = file_person_ids - db_person_ids

                        if missing_person_ids:
                            log.error(
                                "SKIPPING update for %s due to corrupted data. The following person_ids "
                                "from the results file do not exist in the persons table: %s (showing up "
                                "to 10). This indicates a corrupted export file. The 'results' table "
                                "will not be modified.",
                                file_name,
                                list(missing_person_ids)[:10],
                            )
                            continue
                    except Exception as e:
                        log.error(
                            "Error during person_id pre-check for %s: %s. Skipping processing of this "
                            "file to be safe.",
                            file_name,
                            e,
                        )
                        continue

                    try:
                        with get_connection() as conn:
                            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                                cur.execute("""
                                    SELECT value FROM export_metadata WHERE key = 'last_competition'
                                """)
                                metadata_record = cur.fetchone()
                                last_processed_comp_id = metadata_record.value if metadata_record else None

                                if not last_processed_comp_id:
                                    log.info(
                                        "No 'last_competition' record in export_metadata. Proceeding with "
                                        "full processing as this might be the first run."
                                    )
                                else:
                                    df_comp_ids = pd.read_csv(
                                        io.BytesIO(file_bytes),
                                        delimiter="\t",
                                        usecols=["competition_id"],
                                        skip_blank_lines=True,
                                        na_values=["NULL"],
                                        low_memory=False,
                                    )

                                    if df_comp_ids.empty:
                                        log.warning(
                                            "%s is empty or has no competition IDs. Skipping metadata check.",
                                            file_name,
                                        )
                                    else:
                                        file_comp_ids = list(df_comp_ids["competition_id"].dropna().unique())

                                        if not file_comp_ids:
                                            log.warning(
                                                "No valid competition IDs found in %s. Skipping metadata "
                                                "check.",
                                                file_name,
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                SELECT id, start_date FROM competitions
                                                WHERE id = ANY(%s)
                                                ORDER BY start_date DESC, id DESC
                                                LIMIT 1
                                            """,
                                                (file_comp_ids,),
                                            )
                                            latest_comp_in_file = cur.fetchone()

                                            if not latest_comp_in_file:
                                                log.warning(
                                                    "None of the competition IDs from %s exist in the "
                                                    "'competitions' table. Cannot determine if file is outdated. "
                                                    "Proceeding with caution.",
                                                    file_name,
                                                )
                                            else:
                                                cur.execute(
                                                    """
                                                    SELECT start_date FROM competitions WHERE id = %s
                                                """,
                                                    (last_processed_comp_id,),
                                                )
                                                last_processed_comp = cur.fetchone()

                                                if not last_processed_comp:
                                                    log.warning(
                                                        "Last processed competition ID '%s' not found in "
                                                        "'competitions' table. Proceeding with processing.",
                                                        last_processed_comp_id,
                                                    )
                                                elif (
                                                    latest_comp_in_file.start_date
                                                    <= last_processed_comp.start_date
                                                ):
                                                    log.info(
                                                        "SKIPPING update for %s. The latest competition in this "
                                                        "file ('%s' on %s) is not newer than the last successfully "
                                                        "processed competition ('%s' on %s).",
                                                        file_name,
                                                        latest_comp_in_file.id,
                                                        latest_comp_in_file.start_date.date(),
                                                        last_processed_comp_id,
                                                        last_processed_comp.start_date.date(),
                                                    )
                                                    continue
                                                else:
                                                    log.info(
                                                        "Proceeding with %s. Its latest competition ('%s') is newer "
                                                        "than the last processed one.",
                                                        file_name,
                                                        latest_comp_in_file.id,
                                                    )
                    except Exception as e:
                        log.error(
                            "Error during metadata pre-check for %s: %s. Skipping processing of this file "
                            "to be safe.",
                            file_name,
                            e,
                        )
                        continue

                    log.info(
                        "Starting full processing for %s, including data validation and insertion.",
                        file_name,
                    )

                    chunk_size = 10_000_000
                    total_chunks = -(-len(file_bytes) // chunk_size) if len(file_bytes) > 0 else 0
                    headers = None
                    all_rows_to_insert = []
                    is_data_corrupt = False

                    if total_chunks == 0:
                        log.info(
                            "File %s is empty. Clearing 'results' table as per standard procedure, but "
                            "no data will be inserted.",
                            file_name,
                        )
                        with get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("DELETE FROM results")
                        log.info("'results' table cleared due to processing empty file %s.", file_name)
                    else:
                        for i in range(total_chunks):
                            if is_data_corrupt:
                                break

                            start = i * chunk_size
                            end = (i + 1) * chunk_size
                            chunk_bytes = file_bytes[start:end]
                            chunk_str = chunk_bytes.decode("utf-8", errors="ignore")

                            log.info("Validating chunk %s of %s for %s", i + 1, total_chunks, file_name)

                            current_df_chunk = None
                            if i == 0:
                                current_df_chunk = pd.read_csv(
                                    io.StringIO(chunk_str),
                                    delimiter="\t",
                                    skip_blank_lines=True,
                                    na_values=["NULL"],
                                    low_memory=False,
                                    dtype={"id": str},
                                )
                                if not current_df_chunk.empty:
                                    headers = current_df_chunk.columns.tolist()
                                    log.debug("Headers extracted from first chunk: %s", headers)
                                else:
                                    log.warning(
                                        "First chunk of %s is empty. Aborting processing.",
                                        file_name,
                                    )
                                    break
                            else:
                                if headers is None:
                                    log.error(
                                        "Headers not available for chunk %s. Aborting processing.",
                                        i + 1,
                                    )
                                    break
                                chunk_with_headers_str = "\t".join(headers) + "\n" + chunk_str
                                current_df_chunk = pd.read_csv(
                                    io.StringIO(chunk_with_headers_str),
                                    delimiter="\t",
                                    skip_blank_lines=True,
                                    na_values=["NULL"],
                                    low_memory=False,
                                    header=0,
                                    names=headers,
                                    dtype={"id": str},
                                )

                            if current_df_chunk is None or current_df_chunk.empty:
                                log.info("Chunk %s is empty. Skipping.", i + 1)
                                continue

                            for col in headers:
                                if col not in current_df_chunk.columns:
                                    current_df_chunk[col] = pd.NA

                            df_filtered = current_df_chunk[
                                current_df_chunk["person_country_id"] == "Mexico"
                            ]

                            if df_filtered.empty:
                                continue

                            for _, row in df_filtered.iterrows():
                                try:
                                    pos_val = row["pos"]
                                    if pd.isna(pos_val):
                                        pos = 0
                                    else:
                                        pos = int(pos_val)
                                        if not -32768 <= pos <= 32767:
                                            log.error(
                                                "CORRUPTED DATA DETECTED: 'pos' value %s is out of smallint "
                                                "range. Problematic row: %s. Aborting update for %s. The "
                                                "'results' table will not be modified.",
                                                pos,
                                                row.to_dict(),
                                                file_name,
                                            )
                                            is_data_corrupt = True
                                            break

                                    all_rows_to_insert.append(
                                        (
                                            row["id"],
                                            row["competition_id"],
                                            row["event_id"],
                                            row["round_type_id"],
                                            pos,
                                            row["best"],
                                            row["average"],
                                            row["person_id"],
                                            row["format_id"],
                                            row["regional_single_record"],
                                            row["regional_average_record"],
                                        )
                                    )
                                except (KeyError, ValueError, TypeError) as e:
                                    log.error(
                                        "CORRUPTED DATA DETECTED: Error processing row in chunk %s: %s. "
                                        "Problematic row: %s. Aborting update for %s. The 'results' table "
                                        "will not be modified.",
                                        i + 1,
                                        e,
                                        row.to_dict(),
                                        file_name,
                                    )
                                    is_data_corrupt = True
                                    break

                        if is_data_corrupt:
                            log.warning(
                                "Skipping database update for %s due to corrupted data. 'results' table "
                                "remains untouched.",
                                file_name,
                            )
                            continue

                        log.info(
                            "All chunks for %s validated successfully. Total rows to insert for Mexico: %s.",
                            file_name,
                            len(all_rows_to_insert),
                        )

                        if all_rows_to_insert:
                            log.info("Proceeding with atomic update for 'results' table.")
                            with get_connection() as conn:
                                with conn.cursor() as cur:
                                    try:
                                        log.info("Clearing all data from 'results' table.")
                                        cur.execute("DELETE FROM results")

                                        log.info(
                                            "Inserting %s rows into 'results' table.",
                                            len(all_rows_to_insert),
                                        )
                                        execute_values(
                                            cur,
                                            """
                                            INSERT INTO results
                                            (id, competition_id, event_id, round_type_id, pos, best, average,
                                            person_id, format_id, regional_single_record, regional_average_record)
                                            VALUES %s
                                            ON CONFLICT DO NOTHING
                                            """,
                                            all_rows_to_insert,
                                        )
                                        conn.commit()
                                        log.info(
                                            "Successfully committed changes to 'results' table for %s.",
                                            file_name,
                                        )
                                    except Exception as e:
                                        log.error(
                                            "An error occurred during the atomic update for %s: %s. Rolling "
                                            "back transaction.",
                                            file_name,
                                            e,
                                        )
                                        conn.rollback()
                                        continue
                        else:
                            log.info(
                                "No rows for 'Mexico' found in %s. Clearing 'results' table as no new data "
                                "is available.",
                                file_name,
                            )
                            with get_connection() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("DELETE FROM results")
                            log.info("'results' table cleared.")

                        log.info("Finished processing all chunks for %s.", file_name)

                        try:
                            with get_connection() as conn:
                                with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                                    log.info(
                                        "Finding the latest competition with results to update metadata."
                                    )
                                    cur.execute(
                                        """
                                        SELECT c.id
                                        FROM competitions c
                                        JOIN (SELECT DISTINCT competition_id FROM results) r ON c.id = r.competition_id
                                        ORDER BY c.start_date DESC
                                        LIMIT 1;
                                    """
                                    )
                                    latest_competition = cur.fetchone()

                                    if latest_competition:
                                        latest_competition_id = latest_competition.id
                                        log.info(
                                            "Latest competition with results found: %s. Updating metadata.",
                                            latest_competition_id,
                                        )
                                        cur.execute(
                                            """
                                            INSERT INTO export_metadata (key, value, updated_at)
                                            VALUES (%s, %s, NOW())
                                            ON CONFLICT (key) DO UPDATE
                                            SET value = EXCLUDED.value,
                                                updated_at = EXCLUDED.updated_at;
                                        """,
                                            ("last_competition", latest_competition_id),
                                        )
                                    else:
                                        log.warning(
                                            "Could not determine the latest competition from the results table."
                                        )
                        except Exception as e:
                            log.error("Failed to update 'last_competition' metadata: %s", e)

        log.info("Database updated successfully")
        return jsonify({"success": True, "message": "Database updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating database"}), 500


@admin_bp.route("/update-state-ranks", methods=["POST"])
@require_cron_auth
def update_state_ranks():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                cur.execute("UPDATE ranks_single SET state_rank = NULL")
                cur.execute("UPDATE ranks_average SET state_rank = NULL")
                log.info("State ranks reset for ranksSingle and ranksAverage")

                cur.execute("SELECT id, name FROM states")
                states = cur.fetchall()

                if EXCLUDED_EVENTS:
                    placeholders = ",".join(["%s"] * len(EXCLUDED_EVENTS))
                    query = f"SELECT id FROM events WHERE id NOT IN ({placeholders})"
                    cur.execute(query, EXCLUDED_EVENTS)
                else:
                    cur.execute("SELECT id FROM events")
                events = cur.fetchall()

                single_updates = []
                average_updates = []
                log.info("Starting computation of stateRank values for each state and event")

                for state_row in states:
                    state_name = state_row.name
                    log.info("Processing state: %s", state_name)

                    for event_row in events:
                        cur.execute(
                            """
                            SELECT rs.person_id, rs.event_id
                            FROM ranks_single rs
                            INNER JOIN persons p ON rs.person_id = p.wca_id
                            LEFT JOIN states st ON p.state_id = st.id
                            WHERE rs.country_rank <> 0
                              AND rs.event_id = %s
                              AND st.name = %s
                            ORDER BY rs.country_rank ASC
                            """,
                            (event_row.id, state_name),
                        )
                        single_data = cur.fetchall()

                        single_state_rank = 1
                        for record in single_data:
                            single_updates.append(
                                {
                                    "person_id": record.person_id,
                                    "event_id": record.event_id,
                                    "state_rank": single_state_rank,
                                }
                            )
                            single_state_rank += 1

                        cur.execute(
                            """
                            SELECT ra.person_id, ra.event_id
                            FROM ranks_average ra
                            INNER JOIN persons p ON ra.person_id = p.wca_id
                            LEFT JOIN states st ON p.state_id = st.id
                            WHERE ra.country_rank <> 0
                              AND ra.event_id = %s
                              AND st.name = %s
                            ORDER BY ra.country_rank ASC
                            """,
                            (event_row.id, state_name),
                        )
                        average_data = cur.fetchall()

                        average_state_rank = 1
                        for record in average_data:
                            average_updates.append(
                                {
                                    "person_id": record.person_id,
                                    "event_id": record.event_id,
                                    "state_rank": average_state_rank,
                                }
                            )
                            average_state_rank += 1

                log.info(
                    "Computed %s single_updates and %s average_updates",
                    len(single_updates),
                    len(average_updates),
                )

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE tmp_updates (person_id text, event_id text, state_rank int)")

                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO tmp_updates (person_id, event_id, state_rank) VALUES %s",
                    [(u["person_id"], u["event_id"], u["state_rank"]) for u in single_updates],
                )

                cur.execute(
                    """
                    UPDATE ranks_single rs
                    SET state_rank = tmp_updates.state_rank
                    FROM tmp_updates
                    WHERE rs.person_id = tmp_updates.person_id
                    AND rs.event_id = tmp_updates.event_id
                """
                )

                cur.execute("CREATE TEMP TABLE tmp_avg_updates (person_id text, event_id text, state_rank int)")
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO tmp_avg_updates (person_id, event_id, state_rank) VALUES %s",
                    [(u["person_id"], u["event_id"], u["state_rank"]) for u in average_updates],
                )

                cur.execute(
                    """
                    UPDATE ranks_average ra
                    SET state_rank = tmp_avg_updates.state_rank
                    FROM tmp_avg_updates
                    WHERE ra.person_id = tmp_avg_updates.person_id
                    AND ra.event_id = tmp_avg_updates.event_id
                """
                )

        log.info("State rankings updated successfully")
        return jsonify({"success": True, "message": "State rankings updated successfully"})
    except Exception as e:
        log.error(e)
        return jsonify({"success": False, "message": "Error updating state rankings"}), 500


@admin_bp.route("/update-sum-of-ranks", methods=["POST"])
@require_cron_auth
def update_sum_of_ranks():
    try:
        log.info("Starting sum of ranks update")
        excluded = ",".join(f"'{e}'" for e in EXCLUDED_EVENTS)

        single_query = f"""
        WITH all_events AS (
          SELECT DISTINCT event_id FROM ranks_single
          WHERE event_id NOT IN ({excluded})
        ),
        all_people AS (
          SELECT DISTINCT wca_id, name FROM persons
        ),
        people_events AS (
          SELECT all_people.wca_id, all_people.name, all_events.event_id
          FROM all_people CROSS JOIN all_events
        )
        SELECT
          pe.wca_id,
          pe.name,
          json_agg(
            json_build_object(
              'eventId', pe.event_id,
              'countryRank', COALESCE(rs.country_rank, wr.worst_rank),
              'completed', CASE WHEN rs.country_rank IS NULL THEN false ELSE true END
            )
          ) AS events,
          SUM(COALESCE(rs.country_rank, wr.worst_rank)) AS overall
        FROM people_events pe
        LEFT JOIN ranks_single rs
            ON pe.wca_id = rs.person_id AND pe.event_id = rs.event_id
        LEFT JOIN (
          SELECT event_id, MAX(country_rank) + 1 AS worst_rank
          FROM ranks_single
          GROUP BY event_id
        ) AS wr
            ON wr.event_id = pe.event_id
        GROUP BY pe.wca_id, pe.name
        ORDER BY overall
        """

        average_query = f"""
        WITH all_events AS (
          SELECT DISTINCT event_id FROM ranks_average
          WHERE event_id NOT IN ({excluded})
        ),
        all_people AS (
          SELECT DISTINCT wca_id, name FROM persons
        ),
        people_events AS (
          SELECT all_people.wca_id, all_people.name, all_events.event_id
          FROM all_people CROSS JOIN all_events
        )
        SELECT
          pe.wca_id,
          pe.name,
          json_agg(
            json_build_object(
              'eventId', pe.event_id,
              'countryRank', COALESCE(ra.country_rank, wr.worst_rank),
              'completed', CASE WHEN ra.country_rank IS NULL THEN false ELSE true END
            )
          ) AS events,
          SUM(COALESCE(ra.country_rank, wr.worst_rank)) AS overall
        FROM people_events pe
        LEFT JOIN ranks_average ra
            ON pe.wca_id = ra.person_id AND pe.event_id = ra.event_id
        LEFT JOIN (
          SELECT event_id, MAX(country_rank) + 1 AS worst_rank
          FROM ranks_average
          GROUP BY event_id
        ) AS wr
            ON wr.event_id = pe.event_id
        GROUP BY pe.wca_id, pe.name
        ORDER BY overall
        """

        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Deleting existing sum_of_ranks records for single results")
                cur.execute("DELETE FROM sum_of_ranks WHERE result_type = %s", ("single",))
                log.info("Executing single query")
                cur.execute(single_query)
                persons = cur.fetchall()
                log.info("Fetched %s record(s) for single results", len(persons))
                rank = 1
                for row in persons:
                    cur.execute(
                        """
                        INSERT INTO sum_of_ranks (rank, person_id, result_type, overall, events)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (rank, row.wca_id, "single", row.overall, psycopg2.extras.Json(row.events)),
                    )
                    rank += 1
                conn.commit()

        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Deleting existing sum_of_ranks records for average results")
                cur.execute("DELETE FROM sum_of_ranks WHERE result_type = %s", ("average",))
                log.info("Executing average query")
                cur.execute(average_query)
                persons = cur.fetchall()
                log.info("Fetched %s record(s) for average results", len(persons))
                rank = 1
                for row in persons:
                    cur.execute(
                        """
                        INSERT INTO sum_of_ranks (rank, person_id, result_type, overall, events)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (rank, row.wca_id, "average", row.overall, psycopg2.extras.Json(row.events)),
                    )
                    rank += 1
                conn.commit()

        log.info("Sum of ranks updated successfully")
        return jsonify({"success": True, "message": "Sum of ranks updated successfully"})
    except Exception as e:
        log.error("Error updating sum of ranks: %s", e)
        return jsonify({"success": False, "message": "Error updating sum of ranks"}), 500


@admin_bp.route("/update-kinch-ranks", methods=["POST"])
@require_cron_auth
def update_kinch_ranks():
    try:
        log.info("Starting kinch ranks update")

        excluded = ",".join(f"'{e}'" for e in EXCLUDED_EVENTS)
        single_events = ",".join(f"'{e}'" for e in SINGLE_EVENTS)

        query = f"""
        WITH PersonalRecords AS (
          SELECT
            person_id,
            event_id,
            MIN(best) AS personal_best,
            'average' AS type
          FROM ranks_average
          WHERE event_id NOT IN ({excluded})
          GROUP BY person_id, event_id
          UNION ALL
          SELECT
            person_id,
            event_id,
            MIN(best) AS personal_best,
            'single' AS type
          FROM ranks_single
          WHERE event_id IN ({single_events})
          GROUP BY person_id, event_id
        ),
        NationalRecords AS (
          SELECT
            event_id,
            MIN(best) AS national_best,
            'average' AS type
          FROM ranks_average
          WHERE country_rank = 1 AND event_id NOT IN ({excluded})
          GROUP BY event_id
          UNION ALL
          SELECT
            event_id,
            MIN(best) AS national_best,
            'single' AS type
          FROM ranks_single
          WHERE country_rank = 1 AND event_id IN ({single_events})
          GROUP BY event_id
        ),
        Persons AS (
          SELECT DISTINCT person_id FROM ranks_single
        ),
        Events AS (
          SELECT id FROM events WHERE id NOT IN ({excluded})
        ),
        Ratios AS (
          SELECT
            p.person_id,
            e.id AS event_id,
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
          LEFT JOIN PersonalRecords pr ON p.person_id = pr.person_id AND e.id = pr.event_id
          LEFT JOIN NationalRecords nr ON e.id = nr.event_id AND pr.type = nr.type
          GROUP BY p.person_id, e.id
        )
        SELECT
          r.person_id AS id,
          json_agg(
            json_build_object(
              'eventId', r.event_id,
              'ratio', r.best_ratio
            )
          ) AS events,
          AVG(r.best_ratio) AS overall
        FROM Ratios r
        GROUP BY r.person_id
        ORDER BY overall DESC;
        """

        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Deleting existing kinch_ranks records")
                cur.execute("DELETE FROM kinch_ranks")

                log.info("Executing kinch ranks query")
                cur.execute(query)
                persons = cur.fetchall()
                log.info("Fetched %s record(s) for updating kinch ranks", len(persons))

                for index, row in enumerate(persons):
                    cur.execute(
                        """
                        INSERT INTO kinch_ranks (rank, person_id, overall, events)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (index + 1, row.id, row.overall, psycopg2.extras.Json(row.events)),
                    )

        log.info("Kinch ranks updated successfully")
        return jsonify({"success": True, "message": "Kinch ranks updated successfully"})
    except Exception as e:
        log.error("Error updating kinch ranks: %s", e)
        return jsonify({"success": False, "message": "Error updating kinch ranks"}), 500


@admin_bp.route("/update-all", methods=["POST"])
@require_cron_auth
def update_all():
    try:
        log.info("Starting all updates")
        updates = [
            ("update_full_database", update_full_database),
            ("update_state_ranks", update_state_ranks),
            ("update_sum_of_ranks", update_sum_of_ranks),
            ("update_kinch_ranks", update_kinch_ranks),
        ]
        details = {}
        for name, func in updates:
            log.info("Starting update: %s", name)
            result = func()
            if isinstance(result, tuple) and len(result) == 2:
                json_data, status_code = result
            else:
                json_data = result.get_json()
                status_code = result.status_code

            details[name] = {"status": status_code, "result": json_data}
            log.info("Completed update: %s with status %s", name, status_code)
            if status_code != 200:
                log.error("Error occurred during %s: %s", name, json_data)
                return (
                    jsonify(
                        {
                            "success": False,
                            "message": f"Error occurred during {name}",
                            "details": details,
                        }
                    ),
                    status_code,
                )

        log.info("All updates executed successfully")
        return jsonify(
            {
                "success": True,
                "message": "All updates executed successfully",
                "details": details,
            }
        )
    except Exception as e:
        log.error("Unhandled error in update_all: %s", e)
        return jsonify({"success": False, "message": "Error occurred during update_all"}), 500
