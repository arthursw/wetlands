from .getting_started import downloadImageAndSegment
from wetlands import config

if __name__ == "__main__":
    
    config.set_debug(True)
    downloadImageAndSegment()