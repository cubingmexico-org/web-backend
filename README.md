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
- POST /update-database — ingest WCA export TSV zip and update DB
- POST /update-state-ranks — compute stateRank values
- POST /update-sum-of-ranks — compute sum_of_ranks
- POST /update-kinch-ranks — compute kinch_ranks
- POST /update-all — run all updates sequentially
- GET /teams
- GET /teams/<state_id>
- GET /states
- GET /rank/<state_id>/<type>/<event_id> (`type` = single|average)
- GET /competitor-states/<competition_id>

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