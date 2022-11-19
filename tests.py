import uuid
from pathlib import Path
import pytest

import tasks
from job import Job
from scheduler import Scheduler
from utils import TaskState

FILE_NAME = 'tmp_test.txt'


@pytest.fixture()
def cleanup(tmp_path):
    yield
    for f in tmp_path.iterdir():
        if f.is_dir(): f.rmdir()
        else: f.unlink()
    Path(tmp_path).rmdir()


@pytest.fixture
def create_tmp_file(tmp_path):
    with open(tmp_path / FILE_NAME, "w") as f:
        pass
    yield
    Path(tmp_path / FILE_NAME).unlink()
    Path(tmp_path).rmdir()


def test_write_to_file(create_tmp_file):
    scheduler = Scheduler()
    j = Job(target_function=tasks.write_to_file, job_id="write_to_file", file_name=FILE_NAME,
            lines=["line1", "line2"])
    scheduler.add_task(j)
    scheduler.run()
    with open(FILE_NAME) as f:
        result = f.read()
    assert result.split() == ["line1", "line2"]
    assert j.state == TaskState.completed


def test_create_file(tmp_path, cleanup):
    scheduler = Scheduler()
    file_name = tmp_path / "tmp.txt"
    j = Job(target_function=tasks.create_file, job_id="create_file", file_name=file_name)
    scheduler.add_task(j)
    scheduler.run()
    assert len(list(tmp_path.iterdir())) == 1
    assert j.state == TaskState.completed


def test_create_folder(tmp_path, cleanup):
    scheduler = Scheduler()
    j = Job(target_function=tasks.create_folder, job_id="job_create_dir", dir_name=tmp_path / "SomeDir")
    scheduler.add_task(j)
    scheduler.run()
    assert len(list(tmp_path.iterdir())) == 1
    assert j.state == TaskState.completed


def test_delete_folder(tmp_path, cleanup):
    Path(tmp_path/"SomeDir").mkdir(exist_ok=True)
    assert len(list(tmp_path.iterdir())) == 1
    scheduler = Scheduler()
    j = Job(target_function=tasks.delete_folder, job_id="job_rm_dir", dir_name=tmp_path / "SomeDir")
    scheduler.add_task(j)
    scheduler.run()
    assert Path(tmp_path / "SomeDir").exists() is False
    assert j.state == TaskState.completed


def test_rename_folder(tmp_path, cleanup):
    scheduler = Scheduler()
    j1 = Job(target_function=tasks.create_folder, job_id="job_create_dir", dir_name=tmp_path / "SomeDir")
    j2 = Job(target_function=tasks.rename_object, job_id="job_rename_dir", obj_name=tmp_path / "SomeDir",
            target_name=tmp_path / "SomeDir6", dependencies={j1.uuid})
    scheduler.add_task(j2)
    scheduler.add_task(j1)
    scheduler.run()
    assert Path(tmp_path / "SomeDir6").is_dir()
    assert Path(tmp_path / "SomeDir").exists() is False
    assert j1.state == TaskState.completed
    assert j2.state == TaskState.completed


def test_bad_dependency(tmp_path, cleanup):
    scheduler = Scheduler()
    j = Job(target_function=tasks.rename_object, job_id="job_rename_dir", obj_name=tmp_path / "SomeDir",
            target_name=tmp_path / "SomeDir6", dependencies={uuid.uuid4()})
    scheduler.add_task(j)
    scheduler.run()
    assert j.state == TaskState.failed


def test_scheduler_queue_full(tmp_path, cleanup):
    scheduler = Scheduler(pool_size=1)
    file_name1 = tmp_path / "tmp1.txt"
    file_name2 = tmp_path / "tmp2.txt"
    j = Job(target_function=tasks.create_file, job_id="create_file", file_name=file_name1)
    scheduler.add_task(j)
    j2 = Job(target_function=tasks.create_file, job_id="create_file", file_name=file_name2)
    scheduler.add_task(j2)
    scheduler.run()
    assert j.state == TaskState.completed
    assert j2.state == TaskState.not_scheduled

