
from pydantic import (
    BaseModel,
    Field,
)

from omd_airflow_utils.lineage_core.domain.types import MappingType


class RetryConfig(BaseModel):
    total: int = 3
    backoff_factor: float = 1.5
    status_codes: list[int] = [500, 502, 503, 504]


class HttpxClientConfig(BaseModel):
    retry: RetryConfig = Field(default_factory=RetryConfig)
    timeout: float = 10.0
    verify_ssl: bool = True


class APIPathsConfig(BaseModel):
    lineage: str = '/lineage'


class APIHeadersConfig(BaseModel):
    content_type: str = 'application/json'


class APIConfig(BaseModel):
    version: str = 'v1'
    headers: APIHeadersConfig = Field(default_factory=APIHeadersConfig)
    paths: APIPathsConfig = Field(default_factory=APIPathsConfig)


class OperatorDefaultsConfig(BaseModel):
    verify_ssl: bool = True
    fail_silently: bool = False
    mapping: MappingType = MappingType.ONE_TO_ONE


class OperatorConfig(BaseModel):
    defaults: OperatorDefaultsConfig = Field(
        default_factory=OperatorDefaultsConfig
    )


class LineageConfig(BaseModel):
    http_client: HttpxClientConfig = Field(default_factory=HttpxClientConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    operator: OperatorConfig = Field(default_factory=OperatorConfig)

