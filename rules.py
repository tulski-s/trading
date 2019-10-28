# 3rd party
import numpy as np

def trend(arr):
    """
    Finds trend in 'arr'. Returns trading signals (1,0,-1) based on slope 
    of the fitted straight line.
    """
    x = np.array(range(1,len(arr)+1))
    A = np.vstack([x, np.ones(len(x))]).T
    # fits: y = ax + b
    a, b = np.linalg.lstsq(A, arr, rcond=None)[0]
    # arbitrary threshold of 0.25. no science here.
    if a >= .25:
        return 1
    elif a < -.25:
        return -1
    return 0