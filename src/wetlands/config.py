debugPorts: dict[str, int] = {}

def setDebugPorts(dps: dict[str, int]):
    global debugPorts
    debugPorts = dps
    