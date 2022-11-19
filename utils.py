import logging

SAVED_TASKS_FOLDER = "SavedTasks"


def get_logger():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(module)s %(levelname)s]: %(message)s',
                       datefmt='%m/%d/%Y %I:%M:%S%p')
    return logging.getLogger()


class TaskState:
    completed = "completed"
    failed = "failed"
    waiting = "waiting"
    retrying = "retrying"
    not_scheduled = "not_scheduled"

