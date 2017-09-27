import numpy as np
import pandas as pd
import datetime
import sys
from retrying import retry
from datetime import timedelta
from queue import Queue
from threading import Thread

from functools import wraps
def API_log(func):
    '''
    Decorator that keeps track of what succeeds and what fails
    '''

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            print('S', end='|')
            return result
        except:
            e2 = sys.exc_info()[1]
            e2 = e2.args[0]['error_code']
            failure_log.append(tuple(args) + (e2,))
            print(e2, end='|')
    return wrapper

def refer_categorize(x):
    """
    """
    if 'facebook' in x:
        return 'facebook'
    elif 'google' in x:
        return 'google'
    elif 'flipboard' in x:
        return 'flipboard'
    elif 'getpocket' in x:
        return 'pocket'
    elif 'linkedin' in x or 'lnkd.in' in x:
        return 'linkedin'
    elif 'qz.com' in x:
        return 'qz'
    elif x == '':
        return 'none'
    elif 't.co' in x:
        return 'twitter'
    elif 'yahoo' in x:
        return 'yahoo'
    elif 'digg' in x:
        return 'digg'
    elif 'ycombinator' in x:
        return 'ycombinator'
    elif 'bing' in x:
        return 'bing'
    elif 'outlook' in x:
        return 'outlook'
    else:
        return 'other'



def timeframe_gen(start, end, hour_interval=24, tz='US/Eastern'):

    freq = str(hour_interval) + 'H'
    start_dates = pd.date_range(start, end, freq=freq, tz=tz)
    start_dates = start_dates.tz_convert('UTC')
    end_dates = start_dates.shift(1)

    start_times = [datetime.datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in start_dates]
    end_times = [datetime.datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in end_dates]
    timeframe = [(start_times[i], end_times[i]) for i in range(len(start_times))]
    return timeframe

class DownloadWorker1(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            func, start, end, kwargs = self.queue.get()
            run_func(func, start, end, kwargs)
            self.queue.task_done()

def run_func(func, start, end, kwargs):
    """
    """
    key = func.__name__ + '-' + str(start)
    thread_storage[key] = func(start, end, **kwargs)


def run_thread(func, timeframe, kwargs):
    """
    """
    global thread_storage
    thread_storage = {}
    queue = Queue()
    for x in range(8):
        worker = DownloadWorker1(queue)
        worker.daemon = True
        worker.start()

    for start,end in timeframe:
        queue.put((func, start, end, kwargs))

    queue.join()
    return thread_storage