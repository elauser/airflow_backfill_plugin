from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin
from airflow.version import version as airflow_version
from backfill.main import Backfill

airflow_major_version = int(airflow_version.split(".")[0])
# Init the plugin in Webserver's "Admin" Menu with Menu Item as "Backfill"

if airflow_major_version == 1:
    backfill_admin_view = Backfill(category="Admin", name="Backfill")
else:
    backfill_admin_view = {
        "category": "Admin",
        "name": "Backfill",
        "view": Backfill(),
    }


backfill_blueprint = Blueprint(
    "backfill_blueprint", __name__,
    template_folder='templates')


class AirflowBackfillPlugin(AirflowPlugin):
    name = "backfill_plugin"
    admin_views = [backfill_admin_view]
    flask_blueprints = [backfill_blueprint]
    appbuilder_views = [backfill_admin_view]