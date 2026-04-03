import psycopg2.extras
from flask import Blueprint, jsonify

from common import get_connection, log
from utils import parse_int_query_param_or_default

persons_bp = Blueprint("persons", __name__)


@persons_bp.route("/persons", methods=["GET"])
def get_persons():
    try:
        page = parse_int_query_param_or_default("page", 1, min_value=1)
        size = parse_int_query_param_or_default("size", 100, min_value=1, max_value=100)
        offset = (page - 1) * size

        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                log.info("Fetching persons with pagination")
                cur.execute("SELECT COUNT(*) FROM persons")
                total = cur.fetchone()[0]

                cur.execute(
                    "SELECT wca_id, name, state_id FROM persons ORDER BY wca_id LIMIT %s OFFSET %s",
                    (size, offset),
                )
                persons = cur.fetchall()

                cur.execute("SELECT person_id, competition_id FROM results GROUP BY person_id, competition_id")
                comp_rows = cur.fetchall()
                person_competitions = {}
                for row in comp_rows:
                    person_competitions.setdefault(row.person_id, set()).add(row.competition_id)

                cur.execute(
                    "SELECT p.wca_id AS person_id, ch.id AS championship_id FROM persons p "
                    "JOIN championships ch ON ch.competition_id IN ("
                    "SELECT competition_id FROM results WHERE person_id = p.wca_id)"
                )
                champ_rows = cur.fetchall()
                person_championships = {}
                for row in champ_rows:
                    person_championships.setdefault(row.person_id, set()).add(row.championship_id)

                cur.execute(
                    "SELECT person_id, event_id, best, world_rank, continent_rank, country_rank, state_rank "
                    "FROM ranks_single"
                )
                single_ranks = cur.fetchall()
                person_single_ranks = {}
                for r in single_ranks:
                    person_single_ranks.setdefault(r.person_id, []).append(
                        {
                            "eventId": r.event_id,
                            "best": r.best,
                            "rank": {
                                "world": r.world_rank,
                                "continent": r.continent_rank,
                                "country": r.country_rank,
                                "state": r.state_rank,
                            },
                        }
                    )

                cur.execute(
                    "SELECT person_id, event_id, best, world_rank, continent_rank, country_rank, state_rank "
                    "FROM ranks_average"
                )
                average_ranks = cur.fetchall()
                person_average_ranks = {}
                for r in average_ranks:
                    person_average_ranks.setdefault(r.person_id, []).append(
                        {
                            "eventId": r.event_id,
                            "best": r.best,
                            "rank": {
                                "world": r.world_rank,
                                "continent": r.continent_rank,
                                "country": r.country_rank,
                                "state": r.state_rank,
                            },
                        }
                    )

                items = []
                for person in persons:
                    wca_id = person.wca_id
                    competitions = list(person_competitions.get(wca_id, []))
                    championships = list(person_championships.get(wca_id, []))
                    items.append(
                        {
                            "id": wca_id,
                            "name": person.name,
                            "state": person.state_id,
                            "numberOfCompetitions": len(competitions),
                            "competitionIds": competitions,
                            "numberOfChampionships": len(championships),
                            "championshipIds": championships,
                            "rank": {
                                "singles": person_single_ranks.get(wca_id, []),
                                "averages": person_average_ranks.get(wca_id, []),
                            },
                        }
                    )
                log.info("Fetched %s person(s) for page %s size %s", len(items), page, size)
        return jsonify({"pagination": {"page": page, "size": size}, "total": total, "items": items})
    except Exception as e:
        log.error("Error fetching persons: %s", e)
        return jsonify({"success": False, "message": "Error fetching persons"}), 500


@persons_bp.route("/persons/<wca_id>", methods=["GET"])
def get_person(wca_id):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cur:
                cur.execute("SELECT wca_id, name, state_id FROM persons WHERE wca_id = %s", (wca_id,))
                person = cur.fetchone()
                if not person:
                    return jsonify({"success": False, "message": "Person not found"}), 404

                cur.execute(
                    "SELECT competition_id FROM results WHERE person_id = %s GROUP BY competition_id",
                    (wca_id,),
                )
                competitions = [row.competition_id for row in cur.fetchall()]

                cur.execute(
                    "SELECT ch.id AS championship_id FROM championships ch WHERE ch.competition_id IN "
                    "(SELECT competition_id FROM results WHERE person_id = %s)",
                    (wca_id,),
                )
                championships = [row.championship_id for row in cur.fetchall()]

                cur.execute(
                    "SELECT event_id, best, world_rank, continent_rank, country_rank, state_rank "
                    "FROM ranks_single WHERE person_id = %s",
                    (wca_id,),
                )
                singles = [
                    {
                        "eventId": r.event_id,
                        "best": r.best,
                        "rank": {
                            "world": r.world_rank,
                            "continent": r.continent_rank,
                            "country": r.country_rank,
                            "state": r.state_rank,
                        },
                    }
                    for r in cur.fetchall()
                ]

                cur.execute(
                    "SELECT event_id, best, world_rank, continent_rank, country_rank, state_rank "
                    "FROM ranks_average WHERE person_id = %s",
                    (wca_id,),
                )
                averages = [
                    {
                        "eventId": r.event_id,
                        "best": r.best,
                        "rank": {
                            "world": r.world_rank,
                            "continent": r.continent_rank,
                            "country": r.country_rank,
                            "state": r.state_rank,
                        },
                    }
                    for r in cur.fetchall()
                ]

                item = {
                    "id": person.wca_id,
                    "name": person.name,
                    "state": person.state_id,
                    "numberOfCompetitions": len(competitions),
                    "competitionIds": competitions,
                    "numberOfChampionships": len(championships),
                    "championshipIds": championships,
                    "rank": {"singles": singles, "averages": averages},
                }
        return jsonify(item)
    except Exception as e:
        log.error("Error fetching person: %s", e)
        return jsonify({"success": False, "message": "Error fetching person"}), 500
