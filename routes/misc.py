import psycopg2.extras
from flask import Blueprint, jsonify

from common import EXCLUDED_EVENTS, get_connection, log
from utils import convert_keys_to_camel_case

misc_bp = Blueprint("misc", __name__)


@misc_bp.route("/teams", methods=["GET"])
def get_teams():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching all teams")
                cur.execute("SELECT * FROM teams")
                teams = cur.fetchall()
                teams_list = [convert_keys_to_camel_case(dict(team._asdict())) for team in teams]
                log.info("Fetched %s team(s)", len(teams_list))
        return jsonify(teams_list)
    except Exception as e:
        log.error("Error fetching teams: %s", e)
        return jsonify({"success": False, "message": "Error fetching teams"}), 500


@misc_bp.route("/teams/<state_id>", methods=["GET"])
def get_team_by_id(state_id):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching team with ID: %s", state_id)
                cur.execute("SELECT * FROM teams WHERE state_id = %s", (state_id,))
                team = cur.fetchone()
                if team:
                    team_data = convert_keys_to_camel_case(dict(team._asdict()))
                    log.info("Fetched team: %s", team_data)
                    return jsonify(team_data)
                log.warning("No team found with ID: %s", state_id)
                return jsonify({"success": False, "message": "Team not found"}), 404
    except Exception as e:
        log.error("Error fetching team by ID: %s", e)
        return jsonify({"success": False, "message": "Error fetching team"}), 500


@misc_bp.route("/states", methods=["GET"])
def get_states():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching all states")
                cur.execute("SELECT * FROM states")
                states = cur.fetchall()
                states_list = [dict(state._asdict()) for state in states]
                log.info("Fetched %s state(s)", len(states_list))
        return jsonify(states_list)
    except Exception as e:
        log.error("Error fetching states: %s", e)
        return jsonify({"success": False, "message": "Error fetching states"}), 500


@misc_bp.route("/rank/<state_id>/<type>/<event_id>", methods=["GET"])
def get_rank(state_id, type, event_id):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching ranks for state: %s, type: %s, event: %s", state_id, type, event_id)

                table_name = "ranks_single" if type == "single" else "ranks_average"

                cur.execute(
                    f"""
                    SELECT
                        rs.person_id,
                        p.name,
                        rs.event_id,
                        rs.best,
                        rs.world_rank,
                        rs.continent_rank,
                        rs.country_rank,
                        rs.state_rank
                    FROM {table_name} rs
                    INNER JOIN persons p ON rs.person_id = p.wca_id
                    WHERE p.state_id = %s AND rs.event_id = %s
                    """,
                    (state_id, event_id),
                )

                ranks = cur.fetchall()
                if ranks:
                    rank_data = [
                        {
                            "rankType": type,
                            "personId": rank.person_id,
                            "personName": rank.name,
                            "eventId": rank.event_id,
                            "best": rank.best,
                            "rank": {
                                "world": rank.world_rank,
                                "continent": rank.continent_rank,
                                "country": rank.country_rank,
                                "state": rank.state_rank,
                            },
                        }
                        for rank in ranks
                    ]
                    log.info("Fetched %s ranks", len(rank_data))
                    return jsonify(rank_data)

                log.warning("No ranks found for state: %s, type: %s, event: %s", state_id, type, event_id)
                return jsonify({"success": False, "message": "Ranks not found"}), 404
    except Exception as e:
        log.error("Error fetching ranks: %s", e)
        return jsonify({"success": False, "message": "Error fetching ranks"}), 500


@misc_bp.route("/records/<state_id>", methods=["GET"])
def get_records(state_id):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching records for state: %s", state_id)

                excluded_placeholders = ""
                if EXCLUDED_EVENTS:
                    placeholders = ",".join(["%s"] * len(EXCLUDED_EVENTS))
                    excluded_placeholders = placeholders

                single_query = f"""
                    SELECT
                        rs.person_id,
                        rs.event_id,
                        rs.best,
                        rs.world_rank,
                        rs.continent_rank,
                        rs.country_rank,
                        rs.state_rank,
                        p.name as person_name,
                        e.name as event_name
                    FROM ranks_single rs
                    INNER JOIN persons p ON rs.person_id = p.wca_id
                    INNER JOIN events e ON rs.event_id = e.id
                    WHERE p.state_id = %s
                    AND rs.state_rank = 1
                    {"AND rs.event_id NOT IN (" + excluded_placeholders + ")" if EXCLUDED_EVENTS else ""}
                    ORDER BY e.rank
                """

                average_query = f"""
                    SELECT
                        ra.person_id,
                        ra.event_id,
                        ra.best,
                        ra.world_rank,
                        ra.continent_rank,
                        ra.country_rank,
                        ra.state_rank,
                        p.name as person_name,
                        e.name as event_name
                    FROM ranks_average ra
                    INNER JOIN persons p ON ra.person_id = p.wca_id
                    INNER JOIN events e ON ra.event_id = e.id
                    WHERE p.state_id = %s
                    AND ra.state_rank = 1
                    {"AND ra.event_id NOT IN (" + excluded_placeholders + ")" if EXCLUDED_EVENTS else ""}
                    ORDER BY e.rank
                """

                query_params = [state_id] + EXCLUDED_EVENTS if EXCLUDED_EVENTS else [state_id]

                cur.execute(single_query, query_params)
                single_records = cur.fetchall()

                cur.execute(average_query, query_params)
                average_records = cur.fetchall()

                records_data = {
                    "single": [
                        {
                            "personId": record.person_id,
                            "personName": record.person_name,
                            "eventId": record.event_id,
                            "eventName": record.event_name,
                            "best": record.best,
                            "rank": {
                                "world": record.world_rank,
                                "continent": record.continent_rank,
                                "country": record.country_rank,
                                "state": record.state_rank,
                            },
                        }
                        for record in single_records
                    ],
                    "average": [
                        {
                            "personId": record.person_id,
                            "personName": record.person_name,
                            "eventId": record.event_id,
                            "eventName": record.event_name,
                            "best": record.best,
                            "rank": {
                                "world": record.world_rank,
                                "continent": record.continent_rank,
                                "country": record.country_rank,
                                "state": record.state_rank,
                            },
                        }
                        for record in average_records
                    ],
                }

                log.info(
                    "Fetched %s single records and %s average records for state: %s",
                    len(single_records),
                    len(average_records),
                    state_id,
                )
                return jsonify(records_data)

    except Exception as e:
        log.error("Error fetching records: %s", e)
        return jsonify({"success": False, "message": "Error fetching records"}), 500
