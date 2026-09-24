"""Ensure OAuth renewal evidence survives a full Keycloak events page."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from tests.e2e.registry_oauth import RegistryOAuthFailure, _client_login_ids


class TestRegistryOAuthEvents(unittest.TestCase):
    @patch("tests.e2e.registry_oauth.ssl.create_default_context")
    @patch("tests.e2e.registry_oauth._admin_json")
    def test_new_issuance_is_visible_when_both_pages_have_100_events(
        self, admin_json: MagicMock, create_context: MagicMock
    ) -> None:
        del create_context
        old = [{"id": f"old-{index}"} for index in range(100)]
        new = [{"id": "fresh"}, *old[:99]]
        admin_json.side_effect = (old, new)
        token = lambda credentials, context: "synthetic-admin-token"

        before = _client_login_ids({}, "synthetic-client", token)
        after = _client_login_ids({}, "synthetic-client", token)

        self.assertEqual(100, len(before))
        self.assertEqual(100, len(after))
        self.assertEqual({"fresh"}, after.difference(before))

    @patch("tests.e2e.registry_oauth.ssl.create_default_context")
    @patch("tests.e2e.registry_oauth._admin_json", return_value=[{"id": None}])
    def test_invalid_event_id_fails_closed(
        self, admin_json: MagicMock, create_context: MagicMock
    ) -> None:
        del admin_json, create_context
        token = lambda credentials, context: "synthetic-admin-token"

        with self.assertRaisesRegex(RegistryOAuthFailure, "invalid client login events"):
            _client_login_ids({}, "synthetic-client", token)


if __name__ == "__main__":
    unittest.main()
