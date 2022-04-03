from time import time
import numpy as np

def normal_delay(function):
    def wrapper(*args, **kwargs):
        expected_val, sigma= 2, 0.15
        seconds = np.random.normal(expected_val, sigma)
        time.sleep(seconds)
        return function(*args, **kwargs)
    return wrapper
