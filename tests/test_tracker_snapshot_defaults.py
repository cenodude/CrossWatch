# tests/test_tracker_snapshot_defaults.py
# CrossWatch - Snapshot default migration and inherited settings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import copy
import json

import pytest

from cw_platform import config_base


@pytest.mark.parametrize(('base_days', 'instance', 'expected'), [
    (30, {'retention_days': 60}, 64),
    (30, {'retention_days': 0}, 64),
    (60, {'retention_days': 30}, 14),
    (30, {}, 14),
    (30, {'max_snapshots': 0}, 0),
    (30, {'max_snapshots': 21}, 21),
    (30, {'max_snapshots': 64, '_snapshot_defaults_v2': True}, 64),
])
@pytest.mark.parametrize('key', ['crosswatch', 'CrossWatch'])
def test_original_effective_settings_are_preserved_or_migrated(base_days, instance, expected, key):
    cfg = {key: {'max_snapshots': 64, 'retention_days': base_days, 'instances': {'CW-P01': copy.deepcopy(instance)}}}
    config_base._normalize_tracker_snapshot_defaults(cfg)
    base = cfg[key]
    profile = base['instances']['CW-P01']
    assert profile.get('max_snapshots', base['max_snapshots']) == expected
    assert profile.get('retention_days', base['retention_days']) == instance.get('retention_days', base_days)
    first = copy.deepcopy(cfg)
    config_base._normalize_tracker_snapshot_defaults(cfg)
    assert cfg == first


def test_load_save_and_override_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(config_base, 'CONFIG', tmp_path)
    config_base.invalidate_config_cache()
    path = tmp_path / 'config.json'
    path.write_text(json.dumps({'crosswatch': {'max_snapshots': 64, 'retention_days': 30,
        'instances': {'CW-P01': {'retention_days': 60}, 'CW-P02': {}}}}), encoding='utf-8')
    cfg = config_base.load_config()
    assert cfg['crosswatch']['max_snapshots'] == 14
    assert cfg['crosswatch']['instances']['CW-P01']['max_snapshots'] == 64
    assert cfg['crosswatch']['instances']['CW-P02'].get('max_snapshots', 14) == 14
    config_base.save_config(cfg)
    config_base.invalidate_config_cache()
    loaded = config_base.load_config()
    assert loaded['crosswatch']['instances']['CW-P01']['max_snapshots'] == 64
    loaded['crosswatch']['max_snapshots'] = 64
    config_base.save_config(loaded)
    config_base.invalidate_config_cache()
    assert config_base.load_config()['crosswatch']['max_snapshots'] == 64


def test_upgrade_migration_does_not_mutate_input():
    old = {'crosswatch': {'max_snapshots': 64, 'retention_days': 60,
                         'instances': {'CW-P01': {'retention_days': 30}}}}
    migrated, _ = config_base.apply_migration_overrides(old)
    assert migrated['crosswatch']['instances']['CW-P01']['max_snapshots'] == 14
    assert old['crosswatch']['instances']['CW-P01'] == {'retention_days': 30}
