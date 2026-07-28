def pytest_configure(config):
    config.addinivalue_line("markers", "integration: tests using a real Pixi installation or environment")
    config.addinivalue_line(
        "markers",
        "agent_integration: representative real-Pixi tests suitable for routine agent validation",
    )
    config.addinivalue_line("markers", "compat: cross-Python compatibility tests")
    config.addinivalue_line("markers", "manual: expensive tests intended for explicit local or scheduled runs")
    config.addinivalue_line("markers", "slow: tests expected to take noticeably longer than unit tests")
