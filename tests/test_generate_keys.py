"""Unit tests for the pure helpers in scripts/tools/generate_keys.py."""

from __future__ import annotations

import hashlib

import pytest

from scripts.tools.generate_keys import _parse_args, generate_key, hash_key


class TestKeyGeneration:
    def test_pro_prefix(self):
        assert generate_key("pro").startswith("sk-sm-prod-")

    def test_free_prefix(self):
        assert generate_key("free").startswith("sk-sm-dev-")

    def test_keys_are_unique(self):
        assert generate_key("pro") != generate_key("pro")

    def test_hash_is_sha256_hex(self):
        key = "sk-sm-prod-abc"
        assert hash_key(key) == hashlib.sha256(key.encode()).hexdigest()
        assert len(hash_key(key)) == 64


class TestArgParsing:
    def test_create_requires_label(self):
        with pytest.raises(SystemExit):
            _parse_args(["create", "--tier", "pro"])

    def test_create_rejects_unknown_tier(self):
        with pytest.raises(SystemExit):
            _parse_args(["create", "--tier", "gold", "--label", "x"])

    def test_create_parses(self):
        args = _parse_args(["create", "--tier", "free", "--label", "research"])
        assert (args.command, args.tier, args.label) == ("create", "free", "research")
        assert args.owner  # default applied

    def test_revoke_requires_int_id(self):
        with pytest.raises(SystemExit):
            _parse_args(["revoke", "--id", "abc"])
        assert _parse_args(["revoke", "--id", "7"]).id == 7
