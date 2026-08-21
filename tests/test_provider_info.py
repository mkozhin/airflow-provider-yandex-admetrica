"""Проверки метаданных провайдера, которые Airflow читает через entry point."""

import re
from pathlib import Path

import airflow_provider_yandex_admetrica as provider
from airflow_provider_yandex_admetrica import get_provider_info

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"

# Модули из `operators`/`hooks` здесь не импортируются: набор проверок повторяет
# `airflow-provider-avito/tests/test_provider_info.py` и ограничен самим словарём
# метаданных. Импортируемость объявленных модулей проверяется отдельно.


def _distribution_name() -> str:
    for line in PYPROJECT.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r'name\s*=\s*"([^"]+)"', line.strip())
        if match:
            return match.group(1)
    raise AssertionError("в pyproject.toml нет строки с именем дистрибутива")


def test_provider_info_keys():
    info = get_provider_info()
    for key in ("package-name", "name", "description", "versions", "integrations", "operators", "hooks"):
        assert key in info, f"Missing key: {key}"


def test_provider_info_values():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-yandex-admetrica"
    assert info["name"] == "Yandex AdMetrica"
    assert isinstance(info["description"], str)
    assert info["description"]
    assert isinstance(info["versions"], list)
    assert len(info["versions"]) > 0
    assert isinstance(info["integrations"], list)
    assert isinstance(info["operators"], list)
    assert isinstance(info["hooks"], list)


def test_package_name_matches_distribution():
    assert get_provider_info()["package-name"] == _distribution_name()


def test_versions_come_from_version_module():
    assert get_provider_info()["versions"] == [provider.__version__]
    assert isinstance(provider.__version__, str)
    assert provider.__version__


def test_integration_entries():
    info = get_provider_info()
    assert len(info["integrations"]) > 0
    for entry in info["integrations"]:
        assert entry["integration-name"] == "Yandex AdMetrica"
        assert entry["external-doc-url"] == "https://yandex.ru/dev/admetrica/doc/ru/"
        assert isinstance(entry["tags"], list)


def test_operators_and_hooks_declare_modules():
    info = get_provider_info()
    for key in ("operators", "hooks"):
        assert len(info[key]) > 0, f"пустой список {key}"
        for entry in info[key]:
            assert entry["integration-name"] == "Yandex AdMetrica"
            modules = entry["python-modules"]
            assert isinstance(modules, list)
            assert modules
            for module in modules:
                assert module.startswith("airflow_provider_yandex_admetrica.")
