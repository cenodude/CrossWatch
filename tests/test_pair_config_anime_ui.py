from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_pair_config_anime_mapping_requires_global_setting() -> None:
    js = (ROOT / "assets" / "js" / "modals" / "pair-config" / "index.js").read_text("utf-8")
    help_js = (ROOT / "assets" / "js" / "modals" / "pair-config" / "help.js").read_text("utf-8")

    assert "Enable global Anime ID Mapping first." in js
    assert "globalAnimeMappingEnabled(state)" in js
    assert 'block.use_anime_mapping=!!block.use_anime_mapping&&globalAnimeMappingEnabled(state);' in js
    assert 'opts.use_anime_mapping=!!opts.use_anime_mapping&&globalAnimeMappingEnabled(state);' in js
    assert '${animeMapDisabled?"disabled":""}' in js
    assert '${animeBlocked ? "disabled" : ""}' in js
    assert "/api/anime-mapping/settings" not in js
    assert "When enabled from here, global Anime ID Mapping is enabled too." not in js
    assert "When enabled from here, global Anime ID Mapping is enabled too." not in help_js
