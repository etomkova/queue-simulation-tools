import random

from pipeline import Batch
from pipeline import Work


class AbstractBatchProducer(object):

    def generate_batch(self, work_producer):
        raise NotImplementedError


class AbstractWorkProducer(object):

    def generate_work(self, batch):
        raise NotImplementedError


class AbstractTimer(object):

    def get_work_generation_time(self):
        raise NotImplementedError


class ConstantCountBatchProducer(AbstractBatchProducer):

    def __init__(self, count=1):
        self._count = count

    def generate_batch(self, work_producer):

        batch = Batch(items=self._count)
        work = ([work_producer.generate_work(batch) for _ in range(self._count)])

        return work


class ConstantSizeWorkProducer(AbstractWorkProducer):

    def __init__(self, size=1.0):
        self._size = size

    def generate_work(self, batch):
        return Work(batch, self._size)


class ConstantDelayTimer(AbstractTimer):

    def __init__(self, time=1.0):
        self._time = time

    def get_work_generation_time(self):
        return self._time


class RandomDelayTimer(AbstractTimer):

    def __init__(self, mean, diff):

        self._min_time = mean - diff
        self._max_time = mean + diff

    def get_work_generation_time(self):
        return random.uniform(self._min_time, self._max_time)

