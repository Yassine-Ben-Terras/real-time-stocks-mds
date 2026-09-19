"""Import-time validation for every DAG.

This is the cheapest high-value test in the repo: it catches typos, bad
imports, cycles and missing defaults before they reach the scheduler, where
they would otherwise appear only as a broken DAG banner in the UI.
"""

from __future__ import annotations

import pathlib

import pytest
from airflow.models import DagBag

DAG_FOLDER = pathlib.Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_no_import_errors(dagbag: DagBag):
    assert dagbag.import_errors == {}, f"DAG import failures: {dagbag.import_errors}"


def test_dags_were_discovered(dagbag: DagBag):
    assert len(dagbag.dags) >= 3


def test_every_dag_has_tags_and_an_owner(dagbag: DagBag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.default_args.get("owner"), f"{dag_id} has no owner"


def test_every_dag_retries(dagbag: DagBag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.default_args.get("retries", 0) >= 1, f"{dag_id} does not retry"


def test_no_dag_backfills_on_deploy(dagbag: DagBag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} would backfill on first deploy"


def test_no_cycles(dagbag: DagBag):
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag in dagbag.dags.values():
        check_cycle(dag)


def test_dbt_command_builder_is_well_formed():
    import sys

    sys.path.insert(0, str(DAG_FOLDER))
    from dbt_helpers import dbt_command

    cmd = dbt_command("test", select="silver")
    assert "dbt test" in cmd
    assert "--select silver" in cmd
    assert "--profiles-dir" in cmd
    assert "--target" in cmd
