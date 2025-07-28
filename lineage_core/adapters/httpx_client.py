import logging
from typing import (
    Any,
    Optional,
)

import backoff
from httpx import (
    BaseTransport,
    Client,
    HTTPTransport,
    Request,
    RequestError,
    Response,
)

from omd_airflow_utils.lineage_core.adapters.config.config import (
    HttpxClientConfig,
)

logger = logging.getLogger(__name__)

class RetryTransportWrapper(BaseTransport):
    """Transport wrapper that adds retry functionality to HTTP requests."""

    def __init__(
        self,
        transport: HTTPTransport,
        retry_total: int,
        retry_backoff: float,
        retry_status_codes: set[int],
    ) -> None:
        self._transport = transport
        self._retry_total = retry_total
        self._retry_backoff = retry_backoff
        self._retry_status_codes = retry_status_codes

    def handle_request(self, request: Request) -> Response:
        """Handles HTTP request with automatic retry logic."""
        retry_exceptions = (RequestError,)

        @backoff.on_exception(
            backoff.expo,
            retry_exceptions,
            max_tries=self._retry_total,
            factor=self._retry_backoff,
            logger=logger,
        )
        @backoff.on_predicate(
            backoff.expo,
            predicate=lambda r: r.status_code in self._retry_status_codes,
            max_tries=self._retry_total,
            factor=self._retry_backoff,
            logger=logger,
        )
        def _handle_request_with_retry() -> Response:
            response = self._transport.handle_request(request)
            if response.status_code in self._retry_status_codes:
                logger.warning('HTTP request failed with status %s, retrying', response.status_code)
            return response

        return _handle_request_with_retry()


class HttpxClient:
    """HTTP client with retry and timeout capabilities."""

    def __init__(
        self,
        config: HttpxClientConfig = HttpxClientConfig(),
        base_url: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        transport = RetryTransportWrapper(
            transport=HTTPTransport(verify=config.verify_ssl),
            retry_total=config.retry.total,
            retry_backoff=config.retry.backoff_factor,
            retry_status_codes=set(config.retry.status_codes),
        )

        self.client = Client(
            transport=transport,
            timeout=config.timeout,
            headers=headers or {},
            base_url=base_url,
        )

    def get(self, url: str, headers: Optional[dict[str, str]] = None) -> Response:
        return self.client.get(url, headers=headers)

    def put(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
    ) -> Response:
        return self.client.put(url, headers=headers, json=json)

    def post(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
    ) -> Response:
        return self.client.post(url, headers=headers, json=json)

    def patch(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        content: Optional[str] = None,
    ) -> Response:
        return self.client.patch(url, headers=headers, json=json, content=content)

    def delete(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
    ) -> Response:
        return self.client.delete(url, headers=headers)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> 'HttpxClient':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()