try:
    from airflow_provider_yandex_admetrica._version import __version__
except ImportError:  # pragma: no cover - clean checkout without a build
    __version__ = "0.0.0"


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-yandex-admetrica",
        "name": "Yandex AdMetrica",
        "description": "Airflow provider for the Yandex Metrica for Display Advertising (AdMetrica) report API — collect display campaign statistics",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Yandex AdMetrica",
                "external-doc-url": "https://yandex.ru/dev/admetrica/doc/ru/",
                "tags": ["service"],
            },
        ],
        "operators": [
            {
                "integration-name": "Yandex AdMetrica",
                "python-modules": ["airflow_provider_yandex_admetrica.operators.stats"],
            },
        ],
        "hooks": [
            {
                "integration-name": "Yandex AdMetrica",
                "python-modules": ["airflow_provider_yandex_admetrica.hooks.yandex_admetrica"],
            },
        ],
    }
