import uuid
from datetime import datetime
from typing import Callable, Generator
from utils import get_logger
logger = get_logger()


class Job:
    def __init__(self, target_function: Callable, start_at: datetime = None, max_working_time: int = 0,
                 tries: int = 0, dependencies: set = None, job_id: str = None, **kwargs):
        self.start_time = start_at
        self.max_working_time = max_working_time
        self.target_function = target_function
        self.tries = tries
        self.dependencies = dependencies
        self.id = job_id
        self.uuid = uuid.uuid4()
        self.kwargs = kwargs
        self.state = None
        self.target_gen = None
        self.gen = None
        self.started_at = None
        logger.debug(f"Job created: {self}")

    def init_generators(self) -> None:
        self.target_gen = self.target_function()
        next(self.target_gen)
        self.gen = self.run()
        next(self.gen)

    def reinit_target_generator(self) -> None:
        self.target_gen = self.target_function()
        next(self.target_gen)

    def run(self) -> Generator[None, float, None]:
        while True:
            started_at = yield
            logger.debug(f"{self.id} started_at {datetime.fromtimestamp(started_at)}")
            try:
                if self.started_at is None:
                    self.started_at = started_at
                    logger.info(f"Job {self.id} with uuid {self.uuid} starting...")
                    self.target_gen.send(self.kwargs)

                logger.info(f"Job {self.id} with uuid {self.uuid} running...")

                next(self.target_gen)
            except StopIteration:
                return

    def stop(self) -> None:
        self.target_function().close()

    def __repr__(self) -> str:
        return f"{self.id}: uuid={self.uuid}"
