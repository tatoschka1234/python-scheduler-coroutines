import os
import time
import datetime as dt
from collections import deque
import uuid
import pickle
from pathlib import Path
from typing import Generator, Optional

from job import Job
import tasks
from utils import get_logger, SAVED_TASKS_FOLDER, TaskState
logger = get_logger()


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self.dq = deque(maxlen=pool_size)
        self.scheduler_gen = None
        self.job_states = {}
        self.jobs_uuid_map = {}
        Path(SAVED_TASKS_FOLDER).mkdir(exist_ok=True)
        self.check_saved_tasks_folder()

        # I suppose I need some lock, but I have no idea how to do it without threading or asyncio :)
        self.files_in_use = {}

    def add_task(self, task: Job) -> None:
        if len(self.dq) == self.dq.maxlen:
            logger.error(f"Queue is full: failed to add job {task.id}")
            task.state = TaskState.not_scheduled
            return
        self.dq.append(task)
        task.init_generators()
        self.job_states[task.uuid] = TaskState.waiting
        self.jobs_uuid_map[task.uuid] = task
        task.state = TaskState.waiting

    def handle_used_files_on_complete(self, job: Job) -> None:
        if job.kwargs and 'file_name' in job.kwargs:
            f = job.kwargs['file_name']
            if (f in self.files_in_use) and (self.files_in_use[f] == job.uuid):
                logger.info(f"Job {job.id} finished using file {f}")
                del self.files_in_use[f]

    def get_job_name_by_uuid(self, job_uuid: uuid.UUID) -> str | Exception:
        try:
            return self.jobs_uuid_map[job_uuid].id
        except KeyError as ex:
            logger.exception(ex)
            raise ValueError(str(ex))

    def get_job_by_uuid(self, job_uuid: uuid.UUID) -> Job | Exception:
        try:
            return self.jobs_uuid_map[job_uuid]
        except KeyError as ex:
            logger.exception(ex)
            raise ValueError(str(ex))

    def handle_job_after_error(self, job: Job) -> None:
        if job.tries and job.tries > 0:
            self.job_states[job.uuid] = TaskState.retrying
            job.state = TaskState.retrying
            self.job_retry(job)
        else:
            self.job_states[job.uuid] = TaskState.failed
            job.state = TaskState.failed

    def check_saved_tasks_folder(self) -> None:
        p = Path(SAVED_TASKS_FOLDER)
        paths = []
        for x in p.iterdir():
            if x.is_dir():
                for saved_job in x.iterdir():
                    if saved_job.is_file():
                        logger.debug(f"Tasks {saved_job} will be restored")
                        paths.append(saved_job)
        if not paths:
            return
        for file_name in paths:
            try:
                with open(file_name, 'rb') as f:
                    recovered_job = pickle.load(f)
                recovered_job.id += "_recovered"
                recovered_job.started_at = None
                self.add_task(recovered_job)
            except FileNotFoundError as ex:
                logger.exception("failed to restore saved jobs" + str(ex))

    def save_job_on_exit(self, job: Job, state: TaskState) -> None:
        logger.info(f"task={self.get_job_name_by_uuid(job.uuid)} (state {state}) will be resumed after restart")
        root_folder_path = os.path.join(SAVED_TASKS_FOLDER, str(job.uuid))
        Path(root_folder_path).mkdir(exist_ok=True)
        file_name = os.path.join(root_folder_path, str(job.id)) # try
        job.gen = None
        job.target_gen = None
        if job.dependencies:
            deps_completed = set()
            for dep in job.dependencies:
                dep_job_state = self.job_states[dep]
                if dep_job_state in [TaskState.completed, TaskState.failed]:
                    deps_completed.add(dep)
            job.dependencies = job.dependencies - deps_completed

        with open(file_name, 'wb') as f:
            pickle.dump(job, f, protocol=pickle.HIGHEST_PROTOCOL)

    def get_task(self) -> None | Job:
        if len(self.dq) == 0:
            logger.error("Queue is empty")
            return
        return self.dq.popleft()

    def schedule(self) -> Generator[None, Optional[float], None]:
        debug_loop_counter = 0
        while True:
            time.sleep(0.2)
            yield
            debug_loop_counter += 1
            logger.debug(f"************{debug_loop_counter}: new scheduler loop*************")
            job = self.get_task()
            if job:
                current_time = dt.datetime.utcnow()
                if not job.start_time or (current_time > job.start_time):
                    delta = 0
                else:
                    delta = (job.start_time - current_time).seconds

                if not job.started_at and delta > 0:
                    logger.debug(f"Job {job.id} will start in {delta} seconds")
                    self.dq.append(job)
                else:
                    if job.started_at and job.max_working_time and (time.time() - job.started_at > job.max_working_time):
                        logger.warning(f"Job {job.id} run time exceeds {job.max_working_time}. Task will be stopped")
                        job.target_gen.close()
                        job.gen.close()
                        self.handle_job_after_error(job)
                        continue

                    try:
                        if job.dependencies:
                            existing_jobs = set(self.job_states.keys())
                            if not job.dependencies.issubset(existing_jobs):
                                logger.error(f"Job {job.id} was not started: missing dependencies")
                                self.handle_job_after_error(job)
                                continue

                            received_dep_states = set([self.job_states[d] for d in job.dependencies])
                            if TaskState.waiting in received_dep_states:
                                logger.debug(f"Job {job.id} is waiting, as some dependencies are still waiting")
                                self.dq.append(job)
                                continue
                            if TaskState.failed in received_dep_states:
                                # no retry as dep job is failed
                                logger.warning(f"Job {job.id} is failed, as some of dependencies are failed")
                                self.job_states[job.uuid] = TaskState.failed
                                job.state = TaskState.failed
                                continue

                        if job.kwargs and 'file_name' in job.kwargs:
                            f = job.kwargs['file_name']
                            if (f in self.files_in_use) and (self.files_in_use[f] != job.uuid):
                                logger.info(f"Job {job.id} is going to use file {f} which is already in use. "
                                            f"Back to queue")
                                self.dq.append(job)
                                continue
                            self.files_in_use[f] = job.uuid
                        job.gen.send(time.time())
                    except ValueError as ex:
                        logger.error(f"Job {job.id} has stopped running due to errors: {str(ex)}")
                        logger.info(f"Job {job.id} is finished with state 'failed'")
                        self.handle_used_files_on_complete(job)
                        self.handle_job_after_error(job)
                    except StopIteration:
                        logger.info(f"Job {job.id} is finished with state 'completed'")
                        self.job_states[job.uuid] = TaskState.completed
                        self.handle_used_files_on_complete(job)
                        job.state = TaskState.completed
                    else:
                        self.dq.append(job)

    def job_retry(self, job: Job) -> None:
        logger.info(f"Job '{job.id}' will be restarted. Tries left: {job.tries}")
        self.dq.append(job)
        job.init_generators()
        job.tries -= 1
        job.started_at = None
        self.job_states[job.uuid] = TaskState.waiting
        job.state = TaskState.waiting

    def run(self) -> None:
        self.scheduler_gen = self.schedule()
        next(self.scheduler_gen)
        try:
            while len(self.dq) != 0:
                next(self.scheduler_gen)
        except KeyboardInterrupt:
            logger.warning("Scheduler is going to stop...")
            for job_uuid, state in self.job_states.items():
                job_name = self.get_job_name_by_uuid(job_uuid)
                logger.debug(f"task={job_name} in state={state}")
                if state not in [TaskState.completed, TaskState.failed]:
                    job = self.jobs_uuid_map[job_uuid]
                    self.save_job_on_exit(job, state)
        finally:
            self.cleanup_state_folders()

    def cleanup_state_folders(self) -> None:
        logger.debug("Saved folders cleanup")
        p = Path(SAVED_TASKS_FOLDER)
        for x in p.iterdir():
            if x.is_dir():
                job_uuid = uuid.UUID(x.name)
                job_state = self.job_states[job_uuid]
                if job_state in [TaskState.completed, TaskState.failed]:
                    job_name = self.get_job_name_by_uuid(job_uuid)
                    logger.info(f"Folder {x} for job {job_name} (state {job_state}) is going to be removed. ")
                    job_name = job_name.replace("_recovered", "")
                    job_file_path = os.path.join(x, str(job_name))
                    logger.debug(f"File {job_file_path} for job {job_name} (state {job_state}) is going to be removed. ")
                    try:
                        Path(job_file_path).unlink()
                        x.rmdir()
                    except FileNotFoundError as ex:
                        logger.exception(ex)


if __name__ == "__main__":
    scheduler = Scheduler()

    create_dir = Job(target_function=tasks.create_folder, job_id="job_create_dir", dir_name="SomeDir")
    create_dir.start_time = dt.datetime.utcnow() + dt.timedelta(seconds=5)

    rename_folder = Job(target_function=tasks.rename_object, job_id="job_rename_folder",
                 obj_name="SomeDir", target_name="SomeDir2", dependencies={create_dir.uuid})

    rm_dir = Job(target_function=tasks.delete_folder, job_id="job_rm_dir", dir_name="SomeDir2",
                 dependencies={rename_folder.uuid})
    scheduler.add_task(rm_dir)
    scheduler.add_task(rename_folder)
    scheduler.add_task(create_dir)

    file_write = Job(target_function=tasks.write_to_file, max_working_time=15, job_id="job_file_write",
                     file_name="1.txt", lines=["job1" for _ in range(10)])
    create_dir.start_time = dt.datetime.utcnow() + dt.timedelta(seconds=7)
    scheduler.add_task(file_write)

    scheduler.run()
    # states are saved on KeyboardInterrupt only. Looks like it's not necessary to save them on empty Q?

