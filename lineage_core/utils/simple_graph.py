import logging
from collections import defaultdict
from typing import (
    Any,
    Generator,
)

logger = logging.getLogger(__name__)


class SimpleDiGraph:
    """Simple directed graph with node/edge management and traversal support."""

    def __init__(self):
        self.adj: dict[int, list[int]] = defaultdict(list)
        self._nodes: dict[int, dict[str, Any]] = {}

    def add_node(self, node_id: int, **attrs: Any) -> None:
        self._nodes[node_id] = attrs

    def add_edge(self, src: int, dst: int) -> None:
        if src not in self._nodes:
            raise ValueError(f'Source node {src} does not exist in the graph.')
        if dst not in self._nodes:
            raise ValueError(f'Destination node {dst} does not exist in the graph.')
        self.adj[src].append(dst)

    def nodes(self) -> list[int]:
        return list(self._nodes.keys())

    def get_node(self, node_id: int) -> dict[str, Any]:
        if node_id not in self._nodes:
            raise KeyError(f'Node with ID {node_id} not found.')
        return self._nodes[node_id]

    def in_degree(self) -> dict[int, int]:
        in_deg = defaultdict(int)
        for dsts in self.adj.values():
            for dst in dsts:
                in_deg[dst] += 1
        for node_id in self._nodes.keys():
            in_deg.setdefault(node_id, 0)
        return in_deg

    def out_degree(self) -> dict[int, int]:
        return {node_id: len(self.adj.get(node_id, [])) for node_id in self._nodes.keys()}

    def number_of_nodes(self) -> int:
        return len(self._nodes)

    def number_of_edges(self) -> int:
        return sum(len(destinations) for destinations in self.adj.values())

    def edges(self) -> list[tuple[int, int]]:
        return [(src, dst) for src, dsts in self.adj.items() for dst in dsts]

    def sources(self) -> set[int]:
        all_nodes = set(self.adj.keys())

        all_targets = {target for targets in self.adj.values() for target in targets}
        return all_nodes - all_targets

    def sinks(self) -> set[int]:
        return {node for node, targets in self.adj.items() if not targets}

    def all_simple_paths(
        self, start: int, end: int, cutoff: int = 20
    ) -> Generator[list[int], None, None]:
        """Yields all simple paths between start and end nodes up to a cutoff length."""
        if start not in self._nodes or end not in self._nodes:
            return

        def dfs(current: int, path: list[int]) -> Generator[list[int], None, None]:
            if current == end:
                yield path
                return
            if len(path) > cutoff:
                return
            for neighbor in self.adj.get(current, []):
                if neighbor in self._nodes and neighbor not in path:
                    yield from dfs(neighbor, path + [neighbor])

        yield from dfs(start, [start])

