# Airflow 3 et processeurs structlog custom

> Synthèse au 2026-02-16. À revérifier lors des mises à jour d'Airflow.

## Contexte

Airflow 3 a migré son logging vers structlog (AIP-72). Les logs des
tâches sont émis en JSON vers le processus superviseur puis écrits
dans des fichiers.

## Peut-on injecter des processeurs structlog custom ?

**Non, pas de manière officielle ni stable.**

- `logging_config_class` (mécanisme Airflow 2) est **ignoré** dans
  Airflow 3 ([discussion #53006][1]).
- La configuration structlog est **hard-codée** dans
  `airflow.sdk.log.configure_logging()` ([issue #53442][2]).
- Seuls les **logging providers** (ex. CloudWatch) peuvent exposer
  une propriété `processors`, mais c'est destiné aux handlers de
  stockage distant, pas à la transformation des logs ([doc
  providers][3]).

## Approche recommandée

Normaliser les données **au call site** avant de les passer au
logger :

```python
logger.info(
    "Dataset loaded",
    **dataset.model_dump(mode="json", exclude={"name"}),
)
```

`model_dump(mode="json")` convertit les enums en strings, les Path
en strings, etc. Cela fonctionne indépendamment de la chaîne de
processeurs configurée par Airflow.

## Notre architecture de logging

- **Mode tty/plain** : un processeur custom `_normalize_and_flatten`
  (dans `core/logger.py`) aplatit les dicts en clés pointées, filtre
  les `None`, et convertit Path/Enum.
- **Mode airflow** : on ne touche pas à `structlog.configure()` pour
  ne pas écraser la config interne d'Airflow. La normalisation se
  fait au call site.
- **Mode json** : pas de normalisation (les dicts imbriqués sont
  naturels en JSON).

## Sources

- [Discussion #53006 - configure_logging ignore logging_config_class][1]
- [Issue #53442 - Logging configs ignored in Airflow 3.x][2]
- [Doc providers Amazon - CloudWatch handler][3]
- [Advanced logging configuration - Airflow 3.1.7][4]
- [Configuration Reference - Airflow 3.1.7][5]

[1]: https://github.com/apache/airflow/discussions/53006

[2]: https://github.com/apache/airflow/issues/53442

[3]: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/log/cloudwatch_task_handler/index.html

[4]: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/advanced-logging-configuration.html

[5]: https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
