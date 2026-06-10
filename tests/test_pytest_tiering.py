from unittest.mock import MagicMock

import pytest

import conftest


class DummyItem:
    def __init__(self, nodeid, fixturenames=("env_manager",)):
        self.nodeid = nodeid
        self.fixturenames = fixturenames
        self.added_markers = []

    def add_marker(self, marker):
        self.added_markers.append(marker)


def test_backend_filter_keeps_all_backends_by_default():
    config = MagicMock()
    config.getoption.side_effect = lambda option: {"--backend": "all", "--skip-micromamba": False}[option]
    items = [
        DummyItem("tests/test_wetlands.py::test_case[micromamba_root/]"),
        DummyItem("tests/test_wetlands.py::test_case[pixi_root/]"),
    ]

    conftest.pytest_collection_modifyitems(config, items)

    assert items[0].added_markers == []
    assert items[1].added_markers == []


def test_backend_filter_skips_micromamba_when_pixi_selected():
    config = MagicMock()
    config.getoption.side_effect = lambda option: {"--backend": "pixi", "--skip-micromamba": False}[option]
    items = [
        DummyItem("tests/test_wetlands.py::test_case[micromamba_root/]"),
        DummyItem("tests/test_wetlands.py::test_case[pixi_root/]"),
    ]

    conftest.pytest_collection_modifyitems(config, items)

    assert len(items[0].added_markers) == 1
    assert items[0].added_markers[0].name == "skip"
    assert items[1].added_markers == []


def test_backend_filter_skips_pixi_when_micromamba_selected():
    config = MagicMock()
    config.getoption.side_effect = lambda option: {"--backend": "micromamba", "--skip-micromamba": False}[option]
    items = [
        DummyItem("tests/test_wetlands.py::test_case[micromamba_root/]"),
        DummyItem("tests/test_wetlands.py::test_case[pixi_root/]"),
    ]

    conftest.pytest_collection_modifyitems(config, items)

    assert items[0].added_markers == []
    assert len(items[1].added_markers) == 1
    assert items[1].added_markers[0].name == "skip"


def test_skip_micromamba_alias_selects_pixi_backend():
    config = MagicMock()
    config.getoption.side_effect = lambda option: {"--backend": "all", "--skip-micromamba": True}[option]
    items = [
        DummyItem("tests/test_wetlands.py::test_case[micromamba_root/]"),
        DummyItem("tests/test_wetlands.py::test_case[pixi_root/]"),
    ]

    conftest.pytest_collection_modifyitems(config, items)

    assert len(items[0].added_markers) == 1
    assert items[0].added_markers[0].name == "skip"
    assert items[1].added_markers == []


def test_backend_filter_does_not_skip_non_env_manager_parameter_ids():
    config = MagicMock()
    config.getoption.side_effect = lambda option: {"--backend": "pixi", "--skip-micromamba": False}[option]
    item = DummyItem(
        "tests/test_pytest_tiering.py::test_backend_from_nodeid[tests/test_wetlands.py::test_case[micromamba_root/]]",
        fixturenames=("nodeid", "expected"),
    )

    conftest.pytest_collection_modifyitems(config, [item])

    assert item.added_markers == []


@pytest.mark.parametrize(
    ("nodeid", "expected"),
    [
        ("tests/test_wetlands.py::test_case[micromamba_root/]", "micromamba"),
        ("tests/test_wetlands.py::test_case[pixi_root/]", "pixi"),
        ("tests/test_other.py::test_case", None),
    ],
)
def test_backend_from_nodeid(nodeid, expected):
    assert conftest._backend_from_nodeid(nodeid) == expected
