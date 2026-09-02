from pathlib import Path


def test_sticky_save_disabled_state_does_not_show_blocked_cursor() -> None:
    css = Path("assets/ui-shell.css").read_text(encoding="utf-8")

    assert "#save-fab .btn:disabled{" in css
    assert "pointer-events:none" in css
    assert "cursor:default" in css
    assert "cursor:not-allowed" not in css.split("#save-fab .btn:disabled{", 1)[1].split("}", 1)[0]
