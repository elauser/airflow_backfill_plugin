# -*- coding: utf-8 -*-

import os
from datetime import datetime, timezone
from multiprocessing import Process

import flask
from airflow.configuration import conf
from airflow.models import DAG, DagModel, DagBag
from airflow.version import version as airflow_version

from flask import request, current_app, jsonify
from airflow.jobs.base_job import BaseJob

# For backwards compatibility with airflow 1.x.x
# https://airflow.apache.org/docs/apache-airflow/stable/upgrading-from-1-10/index.html#changes-to-airflow-plugins
airflow_major_version = int(airflow_version.split(".")[0])
if airflow_major_version == 1:
    from flask_admin import BaseView, expose
    from airflow import settings
    import contextlib

    @contextlib.contextmanager
    def create_session():
        """Contextmanager that will create and teardown a session."""
        session = settings.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
else:
    from airflow.utils.session import create_session
    from flask_appbuilder import expose, BaseView
    BaseView.render = BaseView.render_template

# Inspired by
# https://github.com/AnkurChoraywal/airflow-backfill-util
# https://github.com/miliar/airflow-backfill-plugin

# Local file where history will be stored with fallback for local development
airflow_home_path = os.environ['AIRFLOW_HOME']
FILE = airflow_home_path + '/logs/backfill_history.txt'


class Backfill(BaseView):
    route_base = "/admin/backfill/"

    @expose('/')
    def list(self):
        """ Render the backfill page """
        dags = self.get_dags()
        return self.render("backfill_page.html",
                           dags=sorted([dag.dag_id for dag in dags]),
                           airflow_major_version=airflow_major_version, )

    @expose('/submit_backfill')
    def submit_backfill(self):
        """ Starts the backfill process """
        dag_name = request.args.get("dag_name")
        start_date = request.args.get("start_date").split('-')
        end_date = request.args.get("end_date").split('-')
        rerun_failed_tasks = request.args.get('rerun_failed_tasks') == 'true'
        reset_dagruns = request.args.get("reset_dagruns") == 'true'
        user_name = request.args.get("user_name")

        # DagBag does not always work, fallback to current_app... catches the rest
        dag = DagBag(conf.get("core", 'dags_folder')).get_dag(dag_name) or current_app.dag_bag.get_dag(dag_name)

        # Execute Backfill in a new process. 
        # Threads or blocking execution does not work as the web server will not be pingable anymore and terminate itself.
        process = Process(target=self.execute_backfill,
                          args=(
                              dag,
                              datetime(int(start_date[0]), int(start_date[1]), int(start_date[2]), 0, 0, tzinfo=timezone.utc),
                              datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]), 0, 0, tzinfo=timezone.utc),
                              reset_dagruns,
                              rerun_failed_tasks,
                          ))
        process.start()

        # Log command history
        args = [
            'dags', 'backfill', dag_name,
            '--start-date', '-'.join(start_date),
            '--end-date', '-'.join(end_date),
            '--reset-dagruns' if reset_dagruns else '',
            '--rerun-failed-tasks' if rerun_failed_tasks else ''
        ]
        file_ops('w', f'[{user_name}] -> {" ".join(args)}')

        return jsonify({'submitted': True, 'command': args})

    @expose('/history')
    def history(self):
        """ Outputs recent user request history """
        return flask.Response(file_ops('r'), mimetype='text/txt')

    @expose('/backfill_jobs')
    def backfill_jobs(self):
        """ Outputs recent backfill jobs """
        with create_session() as session:
            jobs_query = session.query(BaseJob).filter(BaseJob.job_type == 'BackfillJob')
            jobs = jobs_query.all()

        backfill_jobs = [{
            "id": job.id,
            "dag_id": job.dag_id,
            "state": job.state,
            "job_type": job.job_type,
            "start_date": job.start_date,
            "end_date": job.end_date,
            "latest_heartbeat": job.latest_heartbeat,
            "executor_class": job.executor_class,
            "hostname": job.hostname,
            "unixname": job.unixname,
        } for job in jobs]
        backfill_jobs = sorted(backfill_jobs, key=lambda x: x['id'], reverse=True)
        return jsonify(backfill_jobs)

    def get_dags(self):
        with create_session() as session:
            dags_query = session.query(DagModel).filter(~DagModel.is_subdag, DagModel.is_active)
            dags = dags_query.all()
        return dags

    def execute_backfill(self, dag, start_date, end_date, reset_dagruns, rerun_failed_tasks):
        if reset_dagruns:
            DAG.clear_dags(
                [dag],
                start_date=start_date,
                end_date=end_date,
            )
        try:
            dag.run(
                start_date=start_date,
                end_date=end_date,
                rerun_failed_tasks=rerun_failed_tasks,
            )
        except ValueError as vr:
            print(str(vr))


def file_ops(mode, data=None):
    """ File operators - logging/writing and reading user request """
    if mode == 'r':
        try:
            with open(FILE, 'r') as f:
                return f.read()
        except IOError:
            with open(FILE, 'w') as f:
                return f.close()

    elif mode == 'w' and data:
        today = datetime.now()
        print(os.getcwd())
        with open(FILE, 'a+') as f:
            file_data = f'{data},{today}\n'
            f.write(file_data)
            return 1
