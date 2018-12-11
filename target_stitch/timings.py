from contextlib import contextmanager
import time
import singer

LOGGER = singer.get_logger().getChild('target_stitch')

class Timings:
    '''Gathers timing information for the three main steps of the Tap.'''
    def __init__(self):
        self.last_time = time.time()
        self.reset_timings()

    @contextmanager
    def mode(self, mode):
        '''We wrap the big steps of the Tap in this context manager to accumulate
        timing info.'''
        if mode not in self.timings:
            self.timings[mode] = 0.0

        start = time.time()
        yield
        end = time.time()
        self.timings["unspecified"] += start - self.last_time
        self.timings[mode] += end - start
        self.last_time = end

    def reset_timings(self):
        self.timings = {"unspecified": 0.0}

    def log_timings(self):
        '''We call this with every flush to print out the accumulated timings'''
        LOGGER.info('Timings: %s', "; ".join("{}: {:0.3f}".format(k, v) for k, v in self.timings.items()))
