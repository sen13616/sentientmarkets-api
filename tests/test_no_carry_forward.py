"""
tests/test_no_carry_forward.py — Regression guard against re-introduction
of _carry_forward_layer.

Sprint 3 removed _carry_forward_layer from orchestrator.py.  This test
ensures the function is never silently re-introduced.
"""

from __future__ import annotations

import inspect


def test_carry_forward_layer_does_not_exist():
    """_carry_forward_layer must not be importable or present in source."""
    import pipeline.orchestrator as mod

    # Must not be importable as an attribute
    assert not hasattr(mod, "_carry_forward_layer"), (
        "_carry_forward_layer was re-introduced in pipeline.orchestrator"
    )

    # Must not appear in the module source at all
    source = inspect.getsource(mod)
    assert "_carry_forward_layer" not in source, (
        "_carry_forward_layer string found in pipeline/orchestrator.py source"
    )
