from pathlib import Path

import pytest

from ui_frontend import get_index_html


@pytest.mark.parametrize("admin,write", [(True, False), (False, True), (False, False)])
def test_maintenance_page_remains_admin_only(admin, write):
    html = get_index_html(include_admin=admin, user={"permissions": {"write": write}})
    assert ('id="page-maintenance"' in html) is admin
    if admin:
        assert 'onclick="openMaintenance()"' in html


def test_maintenance_uses_page_module_and_removes_modal_registration():
    root = Path(__file__).resolve().parents[1]
    modals = (root / "assets/js/modals.js").read_text("utf-8")
    router = (root / "assets/helpers/core.js").read_text("utf-8")
    assert "ModalRegistry.register('maintenance'" not in modals
    assert '"/assets/js/maintenance/index.js", "MaintenancePage"' in router
    assert 'byId("page-maintenance")?.classList.toggle("hidden", tab !== "maintenance")' in router
