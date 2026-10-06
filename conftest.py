"""
Root conftest.py

pytest configuration lives in pyproject.toml ([tool.pytest.ini_options], asyncio_mode = auto).

Integration tests (marked with @pytest.mark.integration) are automatically
deselected from the default run.  To run them explicitly:

    pytest -m integration
"""


def pytest_collection_modifyitems(config, items):
    """Deselect @pytest.mark.integration tests unless -m integration is used."""
    mark_expr = getattr(config.option, "markexpr", "") or ""
    if "integration" in mark_expr:
        return  # user explicitly requested integration tests — leave items alone

    deselected = [item for item in items if item.get_closest_marker("integration")]
    if not deselected:
        return

    items[:] = [item for item in items if not item.get_closest_marker("integration")]
    config.hook.pytest_deselected(items=deselected)
