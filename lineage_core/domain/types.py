from dataclasses import dataclass
from enum import Enum


class EntityType(str, Enum):
    TABLE = 'table'
    DASHBOARD = 'dashboard'
    PIPELINE = 'pipeline'
    TOPIC = 'topic'


class MappingType(str, Enum):
    ONE_TO_ONE = 'one_to_one'
    ONE_TO_MANY = 'one_to_many'
    MANY_TO_ONE = 'many_to_one'
    MANY_TO_MANY = 'many_to_many'

class LineageLoadType(str, Enum):
    INIT = 'init'
    INCREMENTAL = 'incremental'

@dataclass(frozen=True)
class TypedFQN:
    type: EntityType
    fqn: str


@dataclass(frozen=True)
class EntityPair:
    source: TypedFQN
    target: TypedFQN
