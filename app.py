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


# ===============================================================================
# TESTS:

def create_test_graph():
    """Create a test graph with dummy data based on the JSON file"""
    graph = Graph()
    
    # Create resource nodes - extract IDs from JSON names
    org_node = Node(NodeType.ORGANIZATION, "1066060271767")
    
    # Folders
    folder_767216091627 = Node(NodeType.FOLDER, "767216091627")
    folder_188906894377 = Node(NodeType.FOLDER, "188906894377") 
    folder_635215680011 = Node(NodeType.FOLDER, "635215680011")
    folder_518729943705 = Node(NodeType.FOLDER, "518729943705")
    folder_837642324986 = Node(NodeType.FOLDER, "837642324986")
    folder_96505015065 = Node(NodeType.FOLDER, "96505015065")
    folder_93198982071 = Node(NodeType.FOLDER, "93198982071")
    folder_361332156337 = Node(NodeType.FOLDER, "361332156337")
    folder_36290848176 = Node(NodeType.FOLDER, "36290848176")
    folder_495694787245 = Node(NodeType.FOLDER, "495694787245")
    
    # Projects
    project_185023072868 = Node(NodeType.PROJECT, "185023072868")
    project_20671306372 = Node(NodeType.PROJECT, "20671306372")
    project_377145543109 = Node(NodeType.PROJECT, "377145543109")
    
    # Bucket
    storage_bucket = Node(NodeType.BUCKET, "authomize-exercise-data")
    
    # Billing account
    billing_account = Node(NodeType.BILLING_ACCOUNT, "01B2E0-10D255-037E4D")
    
    # Identity nodes - parse from JSON bindings
    ron_user = Node(NodeType.USER, "ron@test.authomize.com")
    dev_manager_sa = Node(NodeType.SERVICE_ACCOUNT, "dev-manager@striking-arbor-264209.iam.gserviceaccount.com")
    devops_dude_sa = Node(NodeType.SERVICE_ACCOUNT, "devops-dude-1@striking-arbor-264209.iam.gserviceaccount.com")
    exercise_fetcher_sa = Node(NodeType.SERVICE_ACCOUNT, "exercise-fetcher@striking-arbor-264209.iam.gserviceaccount.com")
    cloudasset_sa = Node(NodeType.SERVICE_ACCOUNT, "service-377145543109@gcp-sa-cloudasset.iam.gserviceaccount.com")
    reviewers_group = Node(NodeType.GROUP, "reviewers@test.authomize.com")
    domain_node = Node(NodeType.DOMAIN, "test.authomize.com")
    
    # Add all nodes
    all_nodes = [
        org_node, folder_767216091627, folder_188906894377, folder_635215680011,
        folder_518729943705, folder_837642324986, folder_96505015065, 
        folder_93198982071, folder_361332156337, folder_36290848176,
        folder_495694787245, project_185023072868, project_20671306372,
        project_377145543109, storage_bucket, billing_account,
        ron_user, dev_manager_sa, devops_dude_sa, exercise_fetcher_sa,
        cloudasset_sa, reviewers_group, domain_node
    ]
    
    for node in all_nodes:
        graph.add_node(node)
    
    # Create CORRECT hierarchy edges based on ancestors in JSON
    hierarchy_edges = [
        # Direct children of organization
        Edge(org_node, folder_767216091627, EdgeType.PARENT_CHILD),
        Edge(org_node, folder_36290848176, EdgeType.PARENT_CHILD),
        Edge(org_node, project_185023072868, EdgeType.PARENT_CHILD),
        Edge(org_node, project_377145543109, EdgeType.PARENT_CHILD),
        
        # Children of folder_767216091627
        Edge(folder_767216091627, folder_188906894377, EdgeType.PARENT_CHILD),
        Edge(folder_767216091627, folder_635215680011, EdgeType.PARENT_CHILD),
        Edge(folder_767216091627, folder_96505015065, EdgeType.PARENT_CHILD),
        
        # Children of folder_635215680011
        Edge(folder_635215680011, folder_518729943705, EdgeType.PARENT_CHILD),
        Edge(folder_635215680011, folder_837642324986, EdgeType.PARENT_CHILD),
        
        # Children of folder_96505015065
        Edge(folder_96505015065, folder_93198982071, EdgeType.PARENT_CHILD),
        Edge(folder_96505015065, folder_361332156337, EdgeType.PARENT_CHILD),
        
        # Children of folder_36290848176
        Edge(folder_36290848176, folder_495694787245, EdgeType.PARENT_CHILD),
        Edge(folder_36290848176, project_20671306372, EdgeType.PARENT_CHILD),
        
        # Bucket under project
        Edge(project_185023072868, storage_bucket, EdgeType.PARENT_CHILD),
    ]
    
    for edge in hierarchy_edges:
        graph.add_edge(edge)
    
    # Create permission edges based on actual JSON bindings
    permission_edges = [
        # Organization permissions
        Edge(exercise_fetcher_sa, org_node, EdgeType.PERMISSION, {'role': 'roles/browser'}),
        Edge(exercise_fetcher_sa, org_node, EdgeType.PERMISSION, {'role': 'roles/cloudasset.owner'}),
        Edge(exercise_fetcher_sa, org_node, EdgeType.PERMISSION, {'role': 'roles/iam.securityReviewer'}),
        Edge(exercise_fetcher_sa, org_node, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(exercise_fetcher_sa, org_node, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderViewer'}),
        Edge(exercise_fetcher_sa, org_node, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.organizationViewer'}),
        Edge(ron_user, org_node, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(ron_user, org_node, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(domain_node, org_node, EdgeType.PERMISSION, {'role': 'roles/billing.creator'}),
        Edge(domain_node, org_node, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.projectCreator'}),
        
        # Folder permissions (sample - you'd add all from JSON)
        Edge(dev_manager_sa, folder_188906894377, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(devops_dude_sa, folder_188906894377, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(ron_user, folder_188906894377, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        
        # Add other permissions from JSON...
        Edge(reviewers_group, folder_96505015065, EdgeType.PERMISSION, {'role': 'roles/viewer'}),
        
        # Billing account
        Edge(ron_user, billing_account, EdgeType.PERMISSION, {'role': 'roles/billing.admin'}),
    ]
    
    for edge in permission_edges:
        graph.add_edge(edge)
    
    return graph

def test_graph_basic_operations():
    """Test basic graph operations"""
    print("=== Testing Task 1: Basic Graph Operations ===")
    
    graph = create_test_graph()
    
    # Test node retrieval
    org_node = graph.get_node(NodeType.ORGANIZATION, "1066060271767")
    print(f"Found organization: {org_node.id if org_node else 'Not found'}")
    
    
    # Test edge operations
    if org_node:
        outgoing_edges = graph.get_outgoing_edges(org_node, EdgeType.PARENT_CHILD)
        print(f"Organization has {len(outgoing_edges)} children: {[e.dst_node.id for e in outgoing_edges]}")
        
        incoming_edges = graph.get_incoming_edges(org_node, EdgeType.PERMISSION)
        print(f"Organization has {len(incoming_edges)} permission assignments: {[(e.dst_node.id, e.metadata.get('role')) for e in incoming_edges]}")
    
    print()

def test_hierarchy_operations():
    """Test hierarchy traversal operations"""
    print("=== Testing Task 2: Hierarchy Operations ===")
    
    graph = create_test_graph()
    
    # Test hierarchy
    hierarchy = graph.get_resource_hierarchy_by_id(NodeType.FOLDER, "518729943705")
    expected_hierarchy = ["635215680011", "767216091627", "1066060271767"]
    actual_hierarchy = [node.id for node in hierarchy]
    
    print(f"Folder 518729943705 hierarchy:")
    print(f"  Expected: {expected_hierarchy}")
    print(f"  Actual:   {actual_hierarchy}")
    print(f"  Match: {actual_hierarchy == expected_hierarchy}")
    
    # Test descendants
    org_node = graph.get_node(NodeType.ORGANIZATION, "1066060271767")
    if org_node:
        descendants = graph.get_descendants(org_node)
        print(f"Organization descendants: {len(descendants)} resources")
        
    
    # Test descendants
    folder_96505015065 = graph.get_node(NodeType.FOLDER, "96505015065")
    if folder_96505015065:
        descendants = graph.get_descendants(folder_96505015065)
        descendant_ids = [d.id for d in descendants]
        print(f"Folder 96505015065 descendants: {descendant_ids}")
        expected_descendants = {"361332156337", "93198982071"}
        actual_descendants = set(descendant_ids)
        print(f"  Expected descendants: {expected_descendants}")
        print(f"  Contains expected: {expected_descendants.issubset(actual_descendants)}")
        
    print()

def test_permission_inheritance():
    """Test permission inheritance - the core functionality"""
    print("=== Testing Task 3: Permission Inheritance ===")
    
    graph = create_test_graph()
    
    # Test user permissions (should include inherited permissions)
    ron_permissions = graph.get_identity_permissions(NodeType.USER, "ron@test.authomize.com")
    print(f"Ron's permissions ({len(ron_permissions)} total):")
    for resource, res_type, role in ron_permissions:
        print(f"  {resource} ({res_type}): {role}")
    
    print()

def test_edge_cases():
    """Test edge cases and error handling"""
    print("=== Testing Edge Cases ===")
    
    graph = create_test_graph()
    
    # Test non-existent user
    fake_permissions = graph.get_identity_permissions(NodeType.USER, "fake@user.com")
    print(f"Non-existent user permissions: {len(fake_permissions)}")
    
    # Test node equality and hashing
    node1 = Node(NodeType.USER, "test@user.com")
    node2 = Node(NodeType.USER, "test@user.com")
    print(f"Node equality test: {node1 == node2}")
    print(f"Node hash test: {hash(node1) == hash(node2)}")
    
    # Test adding duplicate nodes/edges
    initial_node_count = len(graph.nodes)
    graph.add_node(node1)
    graph.add_node(node2)  # Should not add duplicate
    print(f"Duplicate node test: {len(graph.nodes) - initial_node_count} nodes added")
    
    print()

def run_all_tests():
    """Run all tests"""
    print("Starting Permission Graph Tests")
    print("=" * 50)
    
    test_graph_basic_operations()
    test_hierarchy_operations()
    test_permission_inheritance()
    test_edge_cases()
    
    print("All tests completed!")

if __name__ == "__main__":
    run_all_tests()