"""Command authorization matrix (CG-26) — who may send which command type."""
import pytest

from src.commands.service import COMMAND_TYPES, effective_role, may_send

_OPERATOR_OK = {"pair_zero_power", "pair_safe_stop", "pair_abort", "safe_stop_all", "automation_disable"}


def test_effective_role_prefers_highest():
    assert effective_role({"site_operator", "org_admin"}) == "org_admin"
    assert effective_role({"super_admin", "site_operator"}) == "super_admin"
    assert effective_role({"site_operator"}) == "site_operator"
    assert effective_role(set()) is None


@pytest.mark.parametrize("role", ["super_admin", "org_admin"])
@pytest.mark.parametrize("ctype", sorted(COMMAND_TYPES))
def test_elevated_roles_send_anything(role, ctype):
    assert may_send(role, ctype) is True


@pytest.mark.parametrize("ctype", sorted(COMMAND_TYPES))
def test_operator_held_to_safe_subset(ctype):
    assert may_send("site_operator", ctype) is (ctype in _OPERATOR_OK)


def test_unknown_type_always_refused():
    for role in ("super_admin", "org_admin", "site_operator"):
        assert may_send(role, "drop_everything") is False


def test_no_command_role_refused():
    assert may_send(None, "safe_stop_all") is False
