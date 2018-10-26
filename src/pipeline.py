from history import ActionType
from history import Record

from stats import GeneratorStats
from stats import QueueStats
from stats import ProcessorStats
from stats import SinkStats
from stats import WorkStats


class Batch(object):

    sequential = 0

    def __init__(self, items=1):

        self.id = Batch.sequential
        self.items = items

        Batch.sequential += 1


class Work(object):

    sequential = 0

    def __init__(self, batch, size=1.0):

        self.id = Work.sequential
        self.batch = batch
        self.size = size

        Work.sequential += 1


class PipelineItem(object):

    def __init__(self):
        self.history = []

    def stats(self):
        raise NotImplementedError


class Generator(PipelineItem):

    def __init__(self, batch_producer, work_producer, batch_timer, work_timer):

        super().__init__()
        self._batch_producer = batch_producer
        self._work_producer = work_producer
        self._batch_timer = batch_timer
        self._work_timer = work_timer

        self._current_batch = []
        self._batch_exhausted = False

    def stats(self):
        return GeneratorStats(self)

    def generate_work(self, current_time):

        if not self._current_batch:
            self._current_batch = self._batch_producer.generate_batch(self._work_producer)
            self._batch_exhausted = False

        work = self._current_batch.pop()
        if not self._current_batch:
            self._batch_exhausted = True

        self.history.append(Record(current_time, work, ActionType.WORK_GENERATED))

        return work

    def get_work_generation_time(self):

        if self._batch_exhausted:
            return self._batch_timer.get_work_generation_time()
        else:
            return self._work_timer.get_work_generation_time()


class Queue(PipelineItem):

    def __init__(self):

        super().__init__()
        self._queue = []

    def stats(self):
        return QueueStats(self)

    def add(self, current_time, work):

        self._queue.append(work)
        self.history.append(Record(current_time, work, ActionType.WORK_ENQUEUED))

    def has_work(self, blocked_batches):
        return any(work.batch not in blocked_batches for work in self._queue)

    def next_work(self, current_time, blocked_batches):

        idx = None
        for idx, work in enumerate(self._queue):
            if work.batch not in blocked_batches:
                break

        work = self._queue.pop(idx)
        self.history.append(Record(current_time, work, ActionType.WORK_DEQUEUED))

        return work


class Processor(PipelineItem):

    def __init__(self, slots=1, rate=1.0, sequential_batches=False):

        super().__init__()

        self.slots = slots
        self.rate = rate
        self._sequential_batches = sequential_batches
        self._processing = []

    def stats(self):
        return ProcessorStats(self)

    def has_free_slots(self):
        return len(self._processing) < self.slots

    def rejects_batches(self):

        if self._sequential_batches:
            return [work.batch for _, work in self._processing]
        else:
            return []

    def start_processing(self, current_time, work):

        finish_time = current_time + work.size / self.rate

        self._processing.append((finish_time, work))
        self.history.append(Record(current_time, work, ActionType.PROCESSING_STARTED))

        return finish_time

    def stop_processing(self, current_time):

        # find work item that is being finished just now
        idx = [finish_time for finish_time, _ in self._processing].index(current_time)
        _, work = self._processing.pop(idx)

        self.history.append(Record(current_time, work, ActionType.PROCESSING_STOPPED))

        return work


class Sink(PipelineItem):

    def __init__(self):
        super().__init__()

    def stats(self):
        return SinkStats(self)

    def sink(self, current_time, work):
        self.history.append(Record(current_time, work, ActionType.WORK_FINISHED))


class Pipeline(object):

    def __init__(self, *stages):

        self.stages = stages
        self.sink = Sink()

    def stats(self):
        return WorkStats(self.stages[0], self.sink)

    def get_upstream(self, stage):

        current = self.stages.index(stage)
        if current == 0:
            return None  # start of the pipeline
        else:
            return self.stages[current - 1]

    def get_downstream(self, stage):

        current = self.stages.index(stage)
        if current == len(self.stages) - 1:
            return None  # end of the pipeline
        else:
            return self.stages[current + 1]
