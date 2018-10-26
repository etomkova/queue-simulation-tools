import enum


@enum.unique
class EventType(enum.Enum):

    WORK_GENERATED = 0
    WORK_QUEUED = 1
    WORK_FINISHED = 2


class Event(object):

    def __init__(self, time, source, type_):

        self.time = time
        self.source = source
        self.type_ = type_


class EventQueue(object):

    def __init__(self):
        self._items = []

    def add(self, event):

        self._items.append(event)
        self._items.sort(key=lambda event: event.time)

    def next_event(self):
        return self._items.pop(0)


class EventProcessor(object):

    def __init__(self, event_queue, pipeline):

        self.event_queue = event_queue
        self.pipeline = pipeline

    def process(self, event):

        current_time = event.time

        # dispatch
        if event.type_ == EventType.WORK_GENERATED:

            generator = event.source
            work = self._generate_work(current_time, generator)
            self._push_downstream(current_time, generator, work)

        elif event.type_ == EventType.WORK_QUEUED:

            queue = event.source
            processor = self.pipeline.get_downstream(queue)
            if processor.has_free_slots():
                blacklist = processor.rejects_batches()
                if queue.has_work(blacklist):
                    work = self._dequeue_work(current_time, queue, blacklist)
                    self._start_processing(current_time, processor, work)

        elif event.type_ == EventType.WORK_FINISHED:

            processor = event.source
            work = self._stop_processing(current_time, processor)
            self._push_downstream(current_time, processor, work)

            upstream_queue = self.pipeline.get_upstream(processor)
            blacklist = processor.rejects_batches()
            if upstream_queue.has_work(blacklist):
                upstream_item = self._dequeue_work(current_time, upstream_queue, blacklist)
                self._start_processing(current_time, processor, upstream_item)

        else:
            raise ValueError("Unknown event type: {}".format(event.type_))

    def _generate_work(self, current_time, generator):

        work = generator.generate_work(current_time)
        self.event_queue.add(Event(
            current_time + generator.get_work_generation_time(),
            generator, EventType.WORK_GENERATED))

        return work

    def _finalize_work(self, current_time, work):
        self.pipeline.sink.sink(current_time, work)

    def _start_processing(self, current_time, processor, work):

        finish_time = processor.start_processing(current_time, work)
        self.event_queue.add(Event(finish_time, processor, EventType.WORK_FINISHED))

    def _stop_processing(self, current_time, processor):
        return processor.stop_processing(current_time)

    def _enqueue_work(self, current_time, queue, work):

        queue.add(current_time, work)
        self.event_queue.add(Event(current_time, queue, EventType.WORK_QUEUED))

    def _dequeue_work(self, current_time, queue, blacklist):
        return queue.next_work(current_time, blacklist)

    def _push_downstream(self, current_time, processor, work):

        downstream_queue = self.pipeline.get_downstream(processor)
        if downstream_queue is not None:
            self._enqueue_work(current_time, downstream_queue, work)
        else:
            self._finalize_work(current_time, work)


class EventLoop(object):

    def __init__(self, event_queue, event_processor):

        self.event_queue = event_queue
        self.event_processor = event_processor

    def run(self, max_time):

        while True:
            event = self.event_queue.next_event()
            time = event.time
            if time > max_time:
                break
            self.event_processor.process(event)


def run(pipeline, max_time):

    queue = EventQueue()
    processor = EventProcessor(queue, pipeline)
    loop = EventLoop(queue, processor)

    queue.add(Event(0.0, pipeline.stages[0], EventType.WORK_GENERATED))
    loop.run(max_time=max_time)
