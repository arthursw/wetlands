from typing import TypedDict, NotRequired, Literal

type Platform = Literal['osx-64', 'osx-arm64', 'win-64', 'win-arm64', 'linux-64', 'linux-arm64']

class Dependency(TypedDict):
    name: str
    platforms: NotRequired[list[Platform]]
    optional: bool
    dependencies: bool

class Dependencies(TypedDict):
    python: str
    conda: NotRequired[list[str | Dependency]]
    pip: NotRequired[list[str | Dependency]]
