import collections
import math
import itertools
import statistics

from history import ActionType


class SpikeFunction(object):

    def __init__(self, time, data):

        self._time = time
        self._change = data

    def integral(self, t_min, t_max, num_windows, cumulative=False):

        dt = (t_max - t_min) / num_windows

        window_time = [t_min + (i + 1) * dt for i in range(num_windows)]
        window_data = len(window_time) * [0]

        for t in self._time:

            idx = int(math.floor((t - t_min) / (t_max - t_min) * num_windows))
            if 0 <= idx < num_windows:
                window_data[idx] += 1

        if cumulative:
            return window_time, _cumulative(window_data)
        else:
            return window_data, window_data

    def mean(self, t_min, t_max, num_windows):

        window_time, integrals = self.integral(t_min, t_max, num_windows)

        dt = (t_max - t_min) / num_windows
        window_data = [integral / dt for integral in integrals]

        return window_time, window_data


class PiecewiseConstantFunction(object):

    def __init__(self, time, data):

        self._time = time
        self._data = data

    def min(self, t_min, t_max, num_windows):

        def aggregate(_, increment_value, aggregated_value):

            if aggregated_value is None:
                return increment_value
            else:
                return min(increment_value, aggregated_value)

        return self._process_constant_pieces((t_min, t_max), num_windows, aggregate)

    def mean(self, t_min, t_max, num_windows):

        dt = (t_max - t_min) / num_windows

        def aggregate(overlap, increment_value, aggregated_value):

            if aggregated_value is None:
                return overlap * increment_value / dt
            else:
                return aggregated_value + overlap * increment_value / dt

        return self._process_constant_pieces((t_min, t_max), num_windows, aggregate)

    def _process_constant_pieces(self, over_time_interval, num_windows, aggregate_fn):

        t_min = over_time_interval[0]
        t_max = over_time_interval[1]
        dt = (t_max - t_min) / num_windows

        window_time = [t_min + (i + 1) * dt for i in range(num_windows)]
        window_data = len(window_time) * [None]

        for constant_interval, constant_value in self._constant_pieces(over_time_interval):

            for window_idx in self._find_overlapping_windows(
                    constant_interval, over_time_interval, num_windows):

                window_interval = (window_time[window_idx] - dt, window_time[window_idx])
                overlap = self._overlap(constant_interval, window_interval)

                window_data[window_idx] = aggregate_fn(overlap, constant_value, window_data[window_idx])

        return window_time, window_data

    def _constant_pieces(self, over_time_interval):

        # if necessary, pad on the beginning by prepending zero value
        head = []
        if self._time[0] > over_time_interval[0]:
            head = [((0.0, over_time_interval[0]), 0.0)]

        # if necessary, pad on the end by repeating last data value
        tail = []
        if self._time[-1] < over_time_interval[1]:
            tail = [((self._time[-1], over_time_interval[1]), self._data[-1])]

        body = []
        for rt_idx in range(len(self._time) - 1):

            rt_min = self._time[rt_idx]
            rt_max = self._time[rt_idx + 1]
            rt_val = self._data[rt_idx]

            body.append(((rt_min, rt_max), rt_val))

        return itertools.chain(head, body, tail)

    def _find_overlapping_windows(self, rt, wt, num_windows):

        # Given an arbitrary semi-open time interval rt [rt_min, rt_max) and a target semi-open time
        # interval wt [wt_min, wt_max) divided into num_windows equally sized time windows,
        # return a list of zero-based indices of time windows in wt that at least partially overlap with rt.

        # Check that there is any overlap at all.
        if self._overlap(rt, wt) == 0.0:
            return []

        wt_idx_min = int(math.floor(num_windows * (rt[0] - wt[0]) / (wt[1] - wt[0])))
        wt_idx_max = int(math.floor(num_windows * (rt[1] - wt[0]) / (wt[1] - wt[0])))

        # Clip rt to wt.
        wt_idx_min = max(0, wt_idx_min)
        wt_idx_max = min(num_windows - 1, wt_idx_max)

        return list(range(wt_idx_min, wt_idx_max + 1))

    @staticmethod
    def _overlap(a, b):

        c0 = max(a[0], b[0])
        c1 = min(a[1], b[1])

        return max(c1 - c0, 0.0)


def _cumulative(increments):

    window_data = [increments[0]]
    for increment in increments[1:]:
        window_data.append(window_data[-1] + increment)

    return window_data


def work_items_registered(history, action):

    action_times = sorted([record.time for record in history if record.action == action])
    action_counts = len(action_times) * [1.0]

    return SpikeFunction(action_times, action_counts)


def batches_registered(history, action):

    pending = {}

    batch_finish_times = []

    for record in history:

        batch = record.work.batch

        if record.action == action:

            # start of new batch
            if batch not in pending:
                pending[batch] = batch.items

            # batch item finished
            pending[batch] -= 1

            # check if whole batch is finished
            if pending[batch] == 0:
                pending.pop(batch)
                batch_finish_times.append(record.time)

    return SpikeFunction(batch_finish_times, len(batch_finish_times) * [1.0])


def work_history(object_history, start_on, finish_on):

    pending = {}
    finished = []

    for record in object_history:

        if record.action == start_on:
            pending[record.work] = record.time

        if record.action == finish_on:
            finished.append((record.work, pending.pop(record.work), record.time))

    for work, start_time in pending.items():
        finished.append((work, start_time, None))

    return finished


def work_items_in_process(work_history):

    changes = collections.defaultdict(int)

    for work, start_on, finish_on in work_history:

        changes[start_on] += 1
        if finish_on is not None:
            changes[finish_on] -= 1

    time = []
    work_count = []

    current_work_count = 0
    for current_time in sorted(changes.keys()):
        current_work_count += changes[current_time]
        time.append(current_time)
        work_count.append(current_work_count)

    return PiecewiseConstantFunction(time, work_count)


def cycle_times(work_history):

    return [t_finish - t_start for work, t_start, t_finish in work_history if t_finish is not None]


class GeneratorStats(object):

    def __init__(self, generator):
        self._generator = generator

    def mean_work_items_generated(self, t_min, t_max, num_intervals):

        fn = work_items_registered(self._generator.history, ActionType.WORK_GENERATED)

        return fn.mean(t_min, t_max, num_intervals)


class QueueStats(object):

    def __init__(self, queue):
        self._queue = queue

    def mean_work_items_in_queue(self,t_min, t_max, num_intervals):

        wh = work_history(self._queue.history, ActionType.WORK_ENQUEUED, ActionType.WORK_DEQUEUED)
        fn = work_items_in_process(wh)

        return fn.mean(t_min, t_max, num_intervals)


class ProcessorStats(object):

    def __init__(self, processor):
        self._processor = processor

    def mean_processed_items(self, t_min, t_max, num_intervals):

        wh = work_history(self._processor.history, ActionType.PROCESSING_STARTED, ActionType.PROCESSING_STOPPED)
        fn = work_items_in_process(wh)

        return fn.mean(t_min, t_max, num_intervals)

    def mean_processor_utilization(self, t_min, t_max, num_intervals):

        time, data = self.mean_processed_items(t_min, t_max, num_intervals)
        total_processor_rate = self._processor.slots * self._processor.rate

        return time, [mean / total_processor_rate for mean in data]


class SinkStats(object):

    def __init__(self, sink):
        self._sink = sink

    def total_work_items_completed(self, t_min, t_max, num_intervals):

        fn = work_items_registered(self._sink.history, ActionType.WORK_FINISHED)

        return fn.integral(t_min, t_max, num_intervals, cumulative=True)

    def total_batches_completed(self, t_min, t_max, num_intervals):

        fn = batches_registered(self._sink.history, ActionType.WORK_FINISHED)

        return fn.integral(t_min, t_max, num_intervals, cumulative=True)


class WorkStats(object):

    def __init__(self, generator, sink):

        self._generator = generator
        self._sink = sink

    def _calculate_item_cycle_times(self):

        wh = work_history(
            itertools.chain(self._generator.history, self._sink.history),
            ActionType.WORK_GENERATED, ActionType.WORK_FINISHED)

        return cycle_times(wh)

    def item_mean_cycle_time(self):
        return statistics.mean(self._calculate_item_cycle_times())

    def item_cycle_time_histogram(self, time_resolution=1.0):

        counts = collections.defaultdict(int)
        for cycle_time in self._calculate_item_cycle_times():
            cycle_time = time_resolution * math.floor(cycle_time / time_resolution)
            counts[cycle_time] += 1

        return sorted(counts.keys()), [counts[time] for time in sorted(counts.keys())]
