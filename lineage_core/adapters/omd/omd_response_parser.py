import logging

from httpx import Response

logger = logging.getLogger(__name__)


class LineageResponseParser:
    """Parser for OpenMetadata API lineage responses."""

    def parse_lineage_raw_edges_response(
        self, response: Response, src_fqn: str
    ) -> list[tuple[str, str, str]]:
        """Parses downstream lineage response into raw edges."""
        raw_edges: list[tuple[str, str, str]] = []

        try:
            data = response.json()
            if not isinstance(data, dict):
                logger.warning('Unexpected lineage response for %s: not a dict', src_fqn)
                return []

            downstream = data.get('downstreamEdges', [])
            if not isinstance(downstream, list):
                logger.warning('Lineage response for %s: downstreamEdges not a list', src_fqn)
                return []

            for edge_data in downstream:
                if isinstance(edge_data, dict):
                    from_id = edge_data.get('fromEntity')
                    to_id = edge_data.get('toEntity')

                    if isinstance(from_id, str) and isinstance(to_id, str):
                        raw_edges.append((src_fqn, from_id, to_id))
                    else:
                        logger.warning('Lineage edge for %s has unexpected entity format', src_fqn)

        except Exception as e:
            logger.warning('Failed to parse lineage response for %s: %s', src_fqn, e)

        return raw_edges

    def parse_upstream_edges(
        self, response: Response, target_fqn: str
    ) -> list[tuple[str, str, str]]:
        """Parses upstream lineage response into raw edges."""
        raw_edges: list[tuple[str, str, str]] = []
        try:
            data = response.json()
            upstream = data.get('upstreamEdges', [])
            for edge in upstream:
                if isinstance(edge, dict):
                    from_id = edge.get('fromEntity')
                    to_id = edge.get('toEntity')
                    if isinstance(from_id, str) and isinstance(to_id, str):
                        raw_edges.append((target_fqn, from_id, to_id))
        except Exception as e:
            logger.warning('Failed to parse upstream lineage for %s: %s', target_fqn, e)
        return raw_edges