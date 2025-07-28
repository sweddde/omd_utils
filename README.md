# OMD Airflow Utils - Data Lineage Operations

Библиотека кастомных Airflow-операторов для работы с data lineage в OpenMetadata. Поддерживает регистрацию, удаление и автоматическую синхронизацию lineage на основе graph структур.

## Архитектура

### Core Components

**Операторы:**
- `RegisterLineageOperator`: регистрация lineage по заданным парам
- `OmdLineageDeleteOperator`: удаление lineage по заданным парам  
- `MGraphToOMDLineageOperator`: автоматическая синхронизация lineage из PostgreSQL graph структур

**Сервисы (Business Logic):**
- `LineageService`: основной facade для всех lineage операций
- `LineageMetadataService`: валидация, кеширование и управление метаданными
- `LineagePairGenerationService`: генерация пар entities с различными стратегиями
- `LineageGraphService`: работа с graph структурами (построение, поиск путей)
- `LineageSyncService`: diff операции и синхронизация состояний

**Адаптеры (Infrastructure):**
- `LineageAPIClient`: HTTP клиент для OpenMetadata API с retry и rate limiting
- `PostgresClient`: клиент для работы с PostgreSQL с connection pooling
- `NodeRepository`: data access layer для извлечения nodes/edges
- `ConfigManager`: управление конфигурацией через Airflow Variables

**Domain Models:**
- `EntityType`, `TypedFQN`, `MappingType`: core типы и enum'ы
- `EntityRef`, `LineageEdge`, `LineagePayload`: модели для API взаимодействия
- `Node`, `EntityPair`: domain модели для lineage операций

## Возможности

### Основные Features
- Поддержка 1:1, 1:N, N:1, N:N отображений entities
- Автоматическая генерация lineage из PostgreSQL graph структур
- Incremental vs Full sync режимы
- HTTP retry с exponential backoff и rate limiting
- Comprehensive кеширование entities
- Differential sync (добавление только изменений)
- Синхронизация описаний таблиц и колонок

### Enterprise Features
- Configuration management через Airflow Variables
- Connection pooling и resource management
- Detailed logging на всех уровнях
- Graceful error handling в bulk операциях
- Context managers для resource cleanup

## Операторы

### RegisterLineageOperator

Регистрация lineage по явно заданным парам entities.

| Параметр           | Тип                  | Описание                                      |
|--------------------|----------------------|-----------------------------------------------|
| `metadata_conn_id` | `str`                | ID подключения Airflow к OpenMetadata        |
| `source_entities`  | `List[TypedFQN]`     | Список исходных сущностей                    |
| `target_entities`  | `List[TypedFQN]`     | Список целевых сущностей                     |
| `mapping`          | `MappingType`        | Стратегия отображения                        |
| `config`           | `LineageConfig`      | Конфигурация клиента и оператора (optional) |

### OmdLineageDeleteOperator

Удаление lineage по заданным парам entities.

| Параметр           | Тип                  | Описание                                      |
|--------------------|----------------------|-----------------------------------------------|
| `metadata_conn_id` | `str`                | ID подключения Airflow к OpenMetadata        |
| `source_entities`  | `List[TypedFQN]`     | Список исходных сущностей для удаления       |
| `target_entities`  | `List[TypedFQN]`     | Список целевых сущностей для удаления        |
| `mapping`          | `MappingType`        | Стратегия отображения                        |

### MGraphToOMDLineageOperator

Автоматическая синхронизация lineage из PostgreSQL graph структур.

| Параметр              | Тип                  | Описание                                      |
|-----------------------|----------------------|-----------------------------------------------|
| `metadata_conn_id`    | `str`                | ID подключения к OpenMetadata                |
| `database_conn_id`    | `str`                | ID подключения к PostgreSQL                  |
| `schema_filter`       | `List[str]`          | Фильтр схем для обработки                    |
| `sync_descriptions`   | `bool`               | Синхронизировать описания (default: False)   |
| `config_variable_name`| `str`                | Имя Airflow Variable с конфигурацией         |

## Типы и Enum'ы

```python
# Core типы
TypedFQN(type=EntityType.TABLE, fqn='service.db.schema.table')

# Стратегии маппинга
MappingType.ONE_TO_ONE    # 1:1 отображение по индексу
MappingType.ONE_TO_MANY   # каждый source → все targets
MappingType.MANY_TO_ONE   # все sources → каждый target  
MappingType.MANY_TO_MANY  # декартово произведение

# Типы сущностей
EntityType.TABLE
EntityType.DATABASE_SCHEMA
EntityType.DATABASE
```

## Примеры использования

### Простая регистрация lineage

```python
from airflow import DAG
from datetime import datetime
from omd_airflow_utils.operators.omd_lineage_register import RegisterLineageOperator
from omd_airflow_utils.lineage_core.domain.types import TypedFQN, EntityType, MappingType

with DAG(
    dag_id='simple_lineage_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    RegisterLineageOperator(
        task_id='register_lineage',
        metadata_conn_id='openmetadata',
        source_entities=[
            TypedFQN(EntityType.TABLE, 'sacristy.raw.users'),
            TypedFQN(EntityType.TABLE, 'sacristy.raw.orders'),
        ],
        target_entities=[
            TypedFQN(EntityType.TABLE, 'sacristy.marts.user_orders'),
        ],
        mapping=MappingType.MANY_TO_ONE,
    )
```

### Автоматическая синхронизация из PostgreSQL

```python
from omd_airflow_utils.operators.mgraph_to_omd_lineage_register import (
    MGraphToOMDLineageOperator,
)

with DAG(
    dag_id='auto_lineage_sync',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    MGraphToOMDLineageOperator(
        task_id='sync_lineage_from_db',
        metadata_conn_id='openmetadata',
        database_conn_id='postgres_lineage',
        schema_filter=['sp_raw', 'sp_stage', 'sp_marts'],
        sync_descriptions=True,
        config_variable_name='lineage_sync_config',
    )
```

### Конфигурация через Airflow Variables

Создайте Airflow Variable `lineage_sync_config`:

```json
{
  "load_type": "incremental",
  "clean_before_update": false,
  "tag_id": 60,
  "operator_id": 14,
  "schema_filter": ["sp_raw", "sp_stage", "sp_marts"],
  "context": {
    "service_name": "Sacristy",
    "database_name": "sacristy"
  }
}
```

## Конфигурация

### LineageConfig (опционально)

```python
from omd_airflow_utils.lineage_core.adapters.config.config import (
    LineageConfig, HttpxClientConfig, RetryConfig
)

config = LineageConfig(
    http_client=HttpxClientConfig(
        timeout=30.0,
        verify_ssl=True,
        retry=RetryConfig(
            total=5,
            backoff_factor=0.5,
            status_codes=[500, 502, 503, 504]
        )
    )
)
```

## Тестирование

```bash
# Запуск всех тестов
pytest tests/ -v

```
