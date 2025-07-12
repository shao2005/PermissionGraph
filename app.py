from typing import List, Dict, Set, Optional, Tuple
from enum import Enum

class NodeType(Enum):
    # Enum of node types for type safety and consistency
    IDENTITY = "identity"
    RESOURCE = "resource"
    ORGANIZATION = "organization"
    FOLDER = "folder"
    PROJECT = "project"
    BILLING_ACCOUNT = "billing_account"
    BUCKET = "bucket"
    USER = "user"
    SERVICE_ACCOUNT = "service_account"
    GROUP = "group"
    DOMAIN = "domain"
    PROJECT_VIEWER = "project_viewer"
    PROJECT_OWNER = "project_owner"
    PROJECT_EDITOR = "project_editor"

class EdgeType(Enum):
    # Enum of edge types for type safety and consistency
    
    # Resource hierarchy relationship
    PARENT_CHILD = "parent_child"
    # Identity has permission on resource
    PERMISSION = "permission"
    # User is a member of a group
    MEMBER = "member"

class EdgeDirection(Enum):
    OUTGOING = 'outgoing'
    INCOMING = 'incoming'
    BOTH = 'both'

class Node:
    def __init__(self, type: NodeType, id: str, metadata: Dict = None):
        self.type = type
        self.id = id
        self.metadata = metadata if metadata is not None else {}
    
    def __hash__(self):
        return hash((self.type, self.id))
    
    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        return self.type == other.type and self.id == other.id
    
    def __repr__(self):
        return f"Node(type={self.type}, id={self.id})"

class Edge:
    def __init__(self, src_node: Node, dst_node: Node, type: EdgeType, metadata: Dict = None):
        self.src_node = src_node
        self.dst_node = dst_node
        self.type = type
        self.metadata = metadata if metadata is not None else {}
    
    def __hash__(self):
        return hash((self.src_node, self.dst_node, self.type))
    
    def __eq__(self, other):
        if not isinstance(other, Edge):
            return False
        return (self.src_node == other.src_node and 
                self.dst_node == other.dst_node and 
                self.type == other.type)
    
    def __repr__(self):
        return f"Edge({self.src_node.id} -> {self.dst_node.id}, type={self.type})"


class Graph:    
    def __init__(self):
        self.nodes: Set[Node] = set()
        self.edges: Set[Edge] = set()
        
        # Maintain adjacency lists for faster traversal
        self._outgoing_edges: Dict[Node, List[Edge]] = {}
        self._incoming_edges: Dict[Node, List[Edge]] = {}
        
        # Node lookup by type and id for faster retrievals
        self._node_lookup: Dict[Tuple[NodeType, str], Node] = {}
    
    def add_node(self, node: Node) -> None:
        if node not in self.nodes:
            self.nodes.add(node)
            self._node_lookup[(node.type, node.id)] = node
            self._outgoing_edges[node] = []
            self._incoming_edges[node] = []
    
    def add_edge(self, edge: Edge) -> None:
        self.add_node(edge.src_node)
        self.add_node(edge.dst_node)
        
        if edge not in self.edges:
            self.edges.add(edge)
            self._outgoing_edges[edge.src_node].append(edge)
            self._incoming_edges[edge.dst_node].append(edge)
    
    def get_node(self, node_type: NodeType, node_id: str) -> Optional[Node]:
        node = self._node_lookup.get((node_type, node_id))
        return node
    
    
    def get_outgoing_edges(self, node: Node, edge_type: EdgeType = None) -> List[Edge]:
        edges_from_node = self._outgoing_edges.get(node, [])
        if edge_type:
            edges_from_node_by_type = [edge for edge in edges_from_node if edge.type == edge_type]
            return edges_from_node_by_type
        return edges_from_node
    
    def get_incoming_edges(self, node: Node, edge_type: EdgeType = None) -> List[Edge]:
        edges_into_node = self._incoming_edges.get(node, [])
        if edge_type:
            edges_into_node_by_type = [edge for edge in edges_into_node if edge.type == edge_type]
            return edges_into_node_by_type
        return edges_into_node
    
    
    def get_descendants(self, node: Node, edge_type: EdgeType = EdgeType.PARENT_CHILD) -> List[Node]:
        # Get all descendants of a node by following parent-child relationships
        descendants = []
        visited = set()
        stack = [node]
        
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            
            # Find children (outgoing parent-child edges)
            children = self.get_neighbors(current, edge_type, EdgeDirection.OUTGOING)
            for child in children:
                if child not in visited:
                    descendants.append(child)
                    stack.append(child)
        
        return descendants
    
    def get_neighbors(self, node: Node, edge_type: EdgeType = None, direction: EdgeDirection = EdgeDirection.OUTGOING) -> list[Node]:
        neighbors_set = set()
        if direction in (EdgeDirection.OUTGOING, EdgeDirection.BOTH):
            outgoing_edges = self.get_outgoing_edges(node, edge_type)
            neighbors_set.update(edge.dst_node for edge in outgoing_edges)
        if direction in (EdgeDirection.INCOMING, EdgeDirection.BOTH):
            incoming_edges = self.get_incoming_edges(node, edge_type)
            neighbors_set.update(edge.src_node for edge in incoming_edges)
        neighbors = list(neighbors_set)
        return neighbors
    
    def get_resource_hierarchy(self, node: Node, edge_type: EdgeType = EdgeType.PARENT_CHILD) -> List[Node]:
        # Get all ancestors of a node by following parent-child relationships
        ancestors = []
        current = node
        visited = set()
        
        while current and current not in visited:
            visited.add(current)
            # Find parent (incoming parent-child edge)
            parent_edges = self.get_incoming_edges(current, edge_type)
            if len(parent_edges) > 1:
                # Multiple parents detected - this violates tree structure
                parent_ids = [edge.src_node.id for edge in parent_edges]
                raise ValueError(
                    f"Multiple parents found for {current.type.value} '{current.id}': {parent_ids}. "
                    f"Tree structure expected for edge type {edge_type.value}"
                )
            elif len(parent_edges) == 1:
                parent = parent_edges[0].src_node
                ancestors.append(parent)
                current = parent
            else:
                break
        
        return ancestors
    
    # TASK 2
    def get_resource_hierarchy_by_id(self, resource_type: NodeType, resource_id: str) -> List[Node]:
        resource_node = self.get_node(resource_type, resource_id)
        hierarchy = []
        if resource_node:
            hierarchy = self.get_resource_hierarchy(resource_node)
        
        return hierarchy
    
    # TASK 3
    def get_identity_permissions(self, identity_type: NodeType, identity_id: str) -> List[Tuple[str, str, str]]:
        # Get all permissions for an identity, including inherited permissions
        # Returns: List of (resource_name, resource_type, role) tuples
        
        identity_node = self.get_node(identity_type, identity_id)
        if not identity_node:
            return []
        
        permissions = []
        
        # Get all direct permission assignments for this user
        permission_edges = self.get_outgoing_edges(identity_node, EdgeType.PERMISSION)
        
        for edge in permission_edges:
            resource = edge.dst_node
            role = edge.metadata.get('role', '')
            
            # Add permission for the directly assigned resource
            permissions.append((resource.id, resource.type.value, role))
            
            # Add inherited permissions for all descendants
            descendants = self.get_descendants(resource, EdgeType.PARENT_CHILD)
            for descendant in descendants:
                permissions.append((descendant.id, descendant.type.value, role))
        
        return permissions