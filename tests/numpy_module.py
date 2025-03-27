def generateArray(start: float, stop: float, step: float = None):
    import numpy
    return numpy.arange(start, stop, step).tolist()