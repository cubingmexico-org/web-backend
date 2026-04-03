from flask import Flask

from routes.admin_updates import admin_bp
from routes.competitions import competitions_bp
from routes.misc import misc_bp
from routes.persons import persons_bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(admin_bp)
    app.register_blueprint(competitions_bp)
    app.register_blueprint(persons_bp)
    app.register_blueprint(misc_bp)
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
