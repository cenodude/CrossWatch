from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_docs_provider_wall_includes_punchplay_and_scrob() -> None:
    readme = (ROOT / "docs" / "README.md").read_text(encoding="utf-8")
    css = (ROOT / "docs" / "assets" / "css" / "style.css").read_text(encoding="utf-8")

    for provider in ("PUNCHPLAY", "SCROB"):
        assert (ROOT / "docs" / "images" / "providers" / f"{provider}.png").exists()
        assert f'images/providers/{provider}.png' in readme

    assert '<strong>PunchPlay</strong>' in readme
    assert '<strong>SCROB</strong>' in readme
    assert ".cw-provider-card.is-punchplay" in css
    assert ".cw-provider-card.is-scrob" in css
