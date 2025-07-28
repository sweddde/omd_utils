from datetime import datetime
from typing import Optional

from dateutil.parser import isoparse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from omd_airflow_utils.lineage_core.domain.types import (
    EntityType,
    LineageLoadType,
)


class EntityRef(BaseModel):
    id: str
    type: EntityType


class LineageEdge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_entity: EntityRef = Field(alias='fromEntity')
    to_entity: EntityRef = Field(alias='toEntity')


class LineagePayload(BaseModel):
    edge: LineageEdge


class Node(BaseModel):
    id: int
    name: str
    namespace_id: int
    db_schema: str
    updated: Optional[datetime]
    state: str = 'accepted'
    description: Optional[str] = None


class DatabaseContext(BaseModel):
    service_name: str = Field(default='Sacristy')
    database_name: str = Field(default='sacristy')

    def fqn(self, schema: str, name: str) -> str:
        return f'{self.service_name}.{self.database_name}.{schema}.{name}'


class Settings(BaseModel):
    tag_id: int = 60
    schema_filter: list[str] = Field(
        default=['sp_raw', 'sp_stage', 'sp_marts', 'sp_features']
    )
    state: str = 'accepted'
    operator_id: int = 14
    last_executed: Optional[datetime] = None
    clean_before_update: bool = False
    load_type: LineageLoadType = LineageLoadType.INCREMENTAL
    context: DatabaseContext = Field(default_factory=DatabaseContext)

    @field_validator('last_executed', mode='before')
    def parse_datetime_string(cls, value):
        if isinstance(value, str):
            try:
                return isoparse(value)
            except ValueError:
                raise ValueError(f'Invalid datetime format: {value}')
        return value