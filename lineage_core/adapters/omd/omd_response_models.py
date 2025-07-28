from typing import (
    Optional,
    List,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class OMDColumn(BaseModel):
    """Represents a column within an OMD table entity."""
    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: Optional[str] = None
    fully_qualified_name: str = Field(alias='fullyQualifiedName')


class OMDResponseEntity(BaseModel):
    """Represents a minimal OpenMetadata entity response."""
    model_config = ConfigDict(
        populate_by_name=True,
        extra='ignore',
    )
    id: str
    type: Optional[str] = Field(default=None, alias='entityType')
    fully_qualified_name: Optional[str] = Field(default=None, alias='fullyQualifiedName')
    name: Optional[str] = None
    description: Optional[str] = Field(default=None, alias='description')
    columns: List[OMDColumn] = Field(default_factory=list)