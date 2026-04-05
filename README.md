# Cubing México — Web Backend

Flask backend that imports World Cube Association (WCA) export data, maintains competition and competitor records (focused on Mexico), and exposes endpoints for rankings and competition data.

## Features
- Import and process official WCA TSV exports (competitions, persons, results, attempts, ranks).
- Atomic updates and corruption checks for large TSV files.
- State-level, national, sum-of-ranks and Kinch ranking computations.
- Endpoints to fetch teams, states, ranks, and competitor state info via WCA WCIF.

## Requirements
- Python 3.10+
- PostgreSQL
- Google Cloud project with Secret Manager access
- pip dependencies (see requirements.txt)

## Configuration
Environment variables:
- GCP_PROJECT_ID — Google Cloud project id (default: `cubing-mexico`)
Secrets:
- `db_url` stored in GCP Secret Manager (used by get_secret)

## Quickstart (local)
1. Clone:
   git clone <repo-url>
   cd web-backend

2. Install:
   pip install -r requirements.txt

3. Set env:
   Windows PowerShell:
   $env:GCP_PROJECT_ID = "your-gcp-project-id"

4. Run:
   python app.py
   App listens on 0.0.0.0:5000 by default.

## Docker
Build and run:
docker build -t cubing-mexico-backend .
docker run -p 5000:5000 -e GCP_PROJECT_ID=your-gcp-project-id cubing-mexico-backend

## Important API endpoints

- **Competitions**
   - `GET /competitions` — List competitions (Mexico only) with pagination and filters
   - `GET /competitions/<competition_id>` — Get one competition (Mexico only) with related events, organizers, delegates, and championships
   - `GET /competitor-states/<competition_id>` — Get state info for competitors in a competition (via WCIF)

- **Teams & States**
   - `GET /teams` — List all teams
   - `GET /teams/<state_id>` — Get team by state ID
   - `GET /states` — List all states

- **Ranks & Records**
   - `GET /rank/<state_id>/<type>/<event_id>` — Get ranks for a state, type (`single`|`average`), and event
   - `GET /records/<state_id>` — Get state records (single and average)

- **Competitors**
   - `GET /persons` — List persons (with pagination, optional state filter)
   - `GET /persons/<wca_id>` — Get person by WCA ID

- **Database & Rankings (admin/cron)**
   - `POST /update-database` — Update the full database from WCA exports
   - `POST /update-state-ranks` — Update state ranks
   - `POST /update-sum-of-ranks` — Update sum of ranks
   - `POST /update-kinch-ranks` — Update Kinch ranks

### Competitions API

GET /competitions query params:
- page: integer, default 1
- size: integer, default 100, max 100
- stateId or state_id: filter by Mexican state id
- eventId or event_id: include competitions that contain a given event
- year: filter by start_date year
- start_date: YYYY-MM-DD, minimum competition start date
- end_date: YYYY-MM-DD, maximum competition end date
- search: case-insensitive text search over competition name and city
- cancelled: boolean (true/false, 1/0, yes/no)

GET /competitions response includes:
- pagination: page, size
- total
- items

GET /competitions/<competition_id> behavior:
- Returns the competition object with related arrays (`events`, `organizers`, `delegates`, `championships`) when the competition is in Mexico.
- Returns HTTP 200 with `{"success": false, "message": "Competition not available for Mexico"}` when the competition does not exist or is not available for Mexico.

### Persons API

GET /persons query params:
- page: integer, default 1
- size: integer, default 100, max 100
- stateId or state_id: filter by state id

GET /persons response includes:
- pagination: page, size
- total
- items

## Database (overview)
Main tables used:
- persons, competitions, results, result_attempts
- ranks_single, ranks_average
- sum_of_ranks, kinch_ranks
- states, teams, events, export_metadata

## Notes
- The app filters and stores data primarily for Mexico.
- Large TSV handling uses chunked validation and execute_values for bulk inserts.
- Corruption checks prevent partial/invalid updates; metadata prevents reprocessing older exports.

## Development
- utils.py contains helpers: get_state_from_coordinates and convert_keys_to_camel_case.
- Logging uses standard Python logging at INFO level.

## License
MIT - See LICENSE for details

## Contributing
This project is maintained by Cubing México. For questions or contributions, please contact the development team.

## Acknowledgments
- World Cube Association for providing official competition data
- The Mexican cubing community