import enum


@enum.unique
class ActionType(enum.Enum):

    WORK_GENERATED = "WorkGenerated"
    WORK_FINISHED = "WorkFinished"
    PROCESSING_STARTED = "StartedProcessing"
    PROCESSING_STOPPED = "StoppedProcessing"
    WORK_ENQUEUED = "WorkEnqueued"
    WORK_DEQUEUED = "WorkDequeued"


class Record(object):

    def __init__(self, time, work, action):

        self.time = time
        self.work = work
        self.action = action
