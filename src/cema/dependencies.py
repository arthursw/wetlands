from typing import TypedDict, NotRequired


class Dependency(TypedDict):
    name: str
    platforms: NotRequired[list[str]]
    optional: bool
    dependencies: bool


class Dependencies(TypedDict):
    python: str
    conda: NotRequired[list[str | Dependency]]
    pip: NotRequired[list[str | Dependency]]
