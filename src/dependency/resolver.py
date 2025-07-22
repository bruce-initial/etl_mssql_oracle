from typing import List, Dict, Any, Set
import logging
from collections import defaultdict, deque

from ..utils.exceptions import DependencyError

logger = logging.getLogger(__name__)


class DependencyResolver:
    """Resolves table dependencies for proper transfer order"""
    
    def __init__(self, tables_config: List[Dict[str, Any]]):
        self.tables_config = tables_config
        self.table_map = {table['target_table']: table for table in tables_config}
        self.dependency_graph = self._build_dependency_graph()
    
    def _build_dependency_graph(self) -> Dict[str, Set[str]]:
        """Build a dependency graph from table configurations"""
        graph = defaultdict(set)
        
        for table_config in self.tables_config:
            table_name = table_config['target_table']
            foreign_keys = table_config.get('foreign_keys', [])
            
            for fk in foreign_keys:
                referenced_table = fk['referenced_table']
                graph[table_name].add(referenced_table)
        
        logger.info(f"Built dependency graph with {len(graph)} tables having dependencies")
        return graph
    
    def _detect_cycles(self) -> List[List[str]]:
        """Detect circular dependencies in the graph"""
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node: str, path: List[str]) -> None:
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return
            
            if node in visited:
                return
            
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for dependent in self.dependency_graph.get(node, []):
                dfs(dependent, path.copy())
            
            rec_stack.remove(node)
        
        for table in self.table_map.keys():
            if table not in visited:
                dfs(table, [])
        
        return cycles
    
    def _topological_sort(self) -> List[str]:
        """Perform topological sort on the dependency graph"""
        # Calculate in-degrees
        in_degree = defaultdict(int)
        all_tables = set(self.table_map.keys())
        
        # Initialize in-degrees to 0
        for table in all_tables:
            in_degree[table] = 0
        
        # Calculate actual in-degrees
        for table, dependencies in self.dependency_graph.items():
            for dep in dependencies:
                in_degree[table] += 1
        
        # Find tables with no dependencies
        queue = deque([table for table in all_tables if in_degree[table] == 0])
        sorted_tables = []
        
        while queue:
            current = queue.popleft()
            sorted_tables.append(current)
            
            # Reduce in-degree for dependent tables
            for table, dependencies in self.dependency_graph.items():
                if current in dependencies:
                    in_degree[table] -= 1
                    if in_degree[table] == 0:
                        queue.append(table)
        
        # Check if all tables were processed
        if len(sorted_tables) != len(all_tables):
            unprocessed = all_tables - set(sorted_tables)
            raise DependencyError(f"Circular dependency detected. Unprocessed tables: {unprocessed}")
        
        return sorted_tables
    
    def _handle_circular_dependencies(self, cycles: List[List[str]]) -> List[str]:
        """Handle circular dependencies by breaking cycles"""
        logger.warning(f"Found {len(cycles)} circular dependencies")
        
        for i, cycle in enumerate(cycles):
            logger.warning(f"Cycle {i+1}: {' -> '.join(cycle)}")
        
        # Create a modified graph without the problematic edges
        modified_graph = dict(self.dependency_graph)
        
        # Break cycles by removing one edge from each cycle
        for cycle in cycles:
            if len(cycle) > 1:
                # Remove the edge from the last table to the first in the cycle
                last_table = cycle[-2]  # Second to last because last is same as first
                first_table = cycle[0]
                if first_table in modified_graph[last_table]:
                    modified_graph[last_table].remove(first_table)
                    logger.warning(f"Broke cycle by removing dependency: {last_table} -> {first_table}")
        
        # Try topological sort with modified graph
        temp_graph = self.dependency_graph
        self.dependency_graph = modified_graph
        
        try:
            result = self._topological_sort()
            logger.info("Successfully resolved dependencies after breaking cycles")
            return result
        finally:
            self.dependency_graph = temp_graph
    
    def resolve_dependencies(self) -> List[Dict[str, Any]]:
        """Resolve dependencies and return tables in proper transfer order"""
        logger.info("Starting dependency resolution")
        
        # Detect cycles
        cycles = self._detect_cycles()
        
        if cycles:
            # Handle circular dependencies
            ordered_table_names = self._handle_circular_dependencies(cycles)
        else:
            # No cycles, proceed with normal topological sort
            ordered_table_names = self._topological_sort()
        
        # Convert table names back to configurations
        ordered_configs = []
        for table_name in ordered_table_names:
            if table_name in self.table_map:
                ordered_configs.append(self.table_map[table_name])
            else:
                logger.warning(f"Table {table_name} not found in configuration")
        
        logger.info(f"Dependency resolution complete. Transfer order: {[t['target_table'] for t in ordered_configs]}")
        return ordered_configs
    
    def validate_dependencies(self) -> None:
        """Validate that all referenced tables exist in configuration"""
        missing_tables = set()
        
        for table_config in self.tables_config:
            table_name = table_config['target_table']
            foreign_keys = table_config.get('foreign_keys', [])
            
            for fk in foreign_keys:
                referenced_table = fk['referenced_table']
                if referenced_table not in self.table_map:
                    missing_tables.add(referenced_table)
                    logger.error(f"Table {table_name} references unknown table {referenced_table}")
        
        if missing_tables:
            raise DependencyError(f"Missing referenced tables: {missing_tables}")
        
        logger.info("Dependency validation successful")
    
    def get_dependency_info(self) -> Dict[str, Any]:
        """Get detailed dependency information"""
        info = {
            'total_tables': len(self.table_map),
            'tables_with_dependencies': len(self.dependency_graph),
            'dependency_graph': dict(self.dependency_graph),
            'cycles': self._detect_cycles()
        }
        
        # Calculate levels (depth from tables with no dependencies)
        levels = {}
        queue = deque()
        
        # Find root tables (no dependencies)
        for table in self.table_map.keys():
            if not self.dependency_graph.get(table):
                levels[table] = 0
                queue.append(table)
        
        # Assign levels
        while queue:
            current = queue.popleft()
            current_level = levels[current]
            
            # Find tables that depend on current table
            for table, dependencies in self.dependency_graph.items():
                if current in dependencies:
                    if table not in levels:
                        levels[table] = current_level + 1
                        queue.append(table)
                    else:
                        levels[table] = max(levels[table], current_level + 1)
        
        info['levels'] = levels
        info['max_depth'] = max(levels.values()) if levels else 0
        
        return info