import pytest
from app import Edge, EdgeType, Graph, Node, NodeType

@pytest.fixture
def graph():
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
    project_editor = Node(NodeType.PROJECT_EDITOR, "intrepid-fiber-264210")
    project_owner = Node(NodeType.PROJECT_OWNER, "intrepid-fiber-264210")
    project_viewer = Node(NodeType.PROJECT_VIEWER, "intrepid-fiber-264210")

    
    # Add all nodes
    all_nodes = [
        org_node, folder_767216091627, folder_188906894377, folder_635215680011,
        folder_518729943705, folder_837642324986, folder_96505015065, 
        folder_93198982071, folder_361332156337, folder_36290848176,
        folder_495694787245, project_185023072868, project_20671306372,
        project_377145543109, storage_bucket, billing_account,
        ron_user, dev_manager_sa, devops_dude_sa, exercise_fetcher_sa,
        cloudasset_sa, reviewers_group, domain_node, project_viewer , project_owner, project_editor
    ]
    
    for node in all_nodes:
        graph.add_node(node)
    
    # Create hierarchy edges based on ancestors in JSON
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
    
    # Create permission edges based on JSON bindings
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
        
        # Folder permissions
        Edge(dev_manager_sa, folder_188906894377, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(devops_dude_sa, folder_188906894377, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(dev_manager_sa, folder_767216091627, EdgeType.PERMISSION, {'role': 'roles/viewer'}),
        Edge(ron_user, folder_188906894377, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(reviewers_group, folder_96505015065, EdgeType.PERMISSION, {'role': 'roles/viewer'}),
        Edge(ron_user, folder_93198982071, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(ron_user, folder_93198982071, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderEditor'}),
        Edge(ron_user, folder_361332156337, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(ron_user, folder_361332156337, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderEditor'}),
        Edge(ron_user, folder_837642324986, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(ron_user, folder_837642324986, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderEditor'}),
        Edge(ron_user, folder_495694787245, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(ron_user, folder_495694787245, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderEditor'}),
        Edge(ron_user, folder_36290848176, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(ron_user, folder_36290848176, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderEditor'}),
        Edge(ron_user, folder_96505015065, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderAdmin'}),
        Edge(ron_user, folder_96505015065, EdgeType.PERMISSION, {'role': 'roles/resourcemanager.folderEditor'}),
        
        # Billing account
        Edge(ron_user, billing_account, EdgeType.PERMISSION, {'role': 'roles/billing.admin'}),

        # Storage Bucket 
        Edge(project_editor, storage_bucket, EdgeType.PERMISSION, {'role': 'roles/storage.legacyBucketOwner'}),
        Edge(project_owner, storage_bucket, EdgeType.PERMISSION, {'role': 'roles/storage.legacyBucketOwner'}),
        Edge(project_viewer, storage_bucket, EdgeType.PERMISSION, {'role': 'roles/storage.legacyBucketReader'}),

        # Projects
        Edge(ron_user, project_20671306372, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(ron_user, project_377145543109, EdgeType.PERMISSION, {'role': 'roles/owner'}),
        Edge(cloudasset_sa, project_377145543109, EdgeType.PERMISSION, {'role': 'roles/cloudasset.serviceAgent'}),
    ]
    
    for edge in permission_edges:
        graph.add_edge(edge)
    
    return graph


def test_node_retrieval(graph):
    org_node = graph.get_node(NodeType.ORGANIZATION, "1066060271767")
    assert org_node is not None
    assert org_node.id == "1066060271767"


def test_organization_child_count(graph):
    org_node = graph.get_node(NodeType.ORGANIZATION, "1066060271767")
    children = graph.get_outgoing_edges(org_node, EdgeType.PARENT_CHILD)
    assert len(children) == 4


def test_permission_bindings_on_organization(graph):
    org_node = graph.get_node(NodeType.ORGANIZATION, "1066060271767")
    permissions = graph.get_incoming_edges(org_node, EdgeType.PERMISSION)
    expected_identities = {
        "exercise-fetcher@striking-arbor-264209.iam.gserviceaccount.com",
        "ron@test.authomize.com",
        "test.authomize.com",
    }
    actual_identities = {edge.src_node.id.split(":")[-1] for edge in permissions}
    assert expected_identities.issubset(actual_identities)


def test_folder_hierarchy_traversal(graph):
    print('=== Testing Task 2: Hierarchy Operations ===')
    hierarchy = graph.get_resource_hierarchy_by_id(NodeType.FOLDER, "518729943705")
    hierarchy_ids = [n.id for n in hierarchy]
    print(hierarchy_ids)
    print()
    assert hierarchy_ids == ["635215680011", "767216091627", "1066060271767"]


def test_descendants_of_folder(graph):
    folder_node = graph.get_node(NodeType.FOLDER, "96505015065")
    descendants = graph.get_descendants(folder_node)
    descendant_ids = {d.id for d in descendants}
    assert {"361332156337", "93198982071"}.issubset(descendant_ids)


def test_inherited_permissions(graph):
    # === Testing Task 3: Permission Inheritance ===
    permissions = graph.get_identity_permissions(NodeType.USER, "ron@test.authomize.com")
    assert any(p[0] == "188906894377" and p[2] == "roles/resourcemanager.folderAdmin" for p in permissions)
    assert any(p[0] == "361332156337" for p in permissions)


def test_permission_inheritance(graph):
    print("=== Testing Task 3: Permission Inheritance ===")
    
    # Test user permissions (should include inherited permissions)
    ron_permissions = graph.get_identity_permissions(NodeType.USER, "ron@test.authomize.com")
    print(f"Ron's permissions ({len(ron_permissions)} total):")
    for resource, res_type, role in ron_permissions:
        print(f" {resource} ({res_type}): {role}")
    
    print()


def test_nonexistent_identity(graph):
    permissions = graph.get_identity_permissions(NodeType.USER, "fake@user.com")
    assert permissions == []


def test_node_equality_and_hashing():
    node1 = Node(NodeType.USER, "test@user.com")
    node2 = Node(NodeType.USER, "test@user.com")
    assert node1 == node2
    assert hash(node1) == hash(node2)


def test_duplicate_node_addition(graph):
    initial_count = len(graph.nodes)
    graph.add_node(Node(NodeType.USER, "duplicate@user.com"))
    graph.add_node(Node(NodeType.USER, "duplicate@user.com"))
    assert len(graph.nodes) == initial_count + 1

def test_direct_permissions_on_resource(graph):
    """Test identities with direct permissions on a folder"""
    folder_id = "188906894377"  # this has direct permissions
    results = graph.get_identities_with_permissions_on_resource(NodeType.FOLDER, folder_id)

    expected = {
        ("dev-manager@striking-arbor-264209.iam.gserviceaccount.com", "roles/owner"),
        ("devops-dude-1@striking-arbor-264209.iam.gserviceaccount.com", "roles/owner"),
        ("ron@test.authomize.com", "roles/resourcemanager.folderAdmin"),
    }

    for identity, role in expected:
        assert (identity, role) in results


def test_inherited_permissions_from_parent_folder(graph):
    """Test inherited permissions from parent folders"""
    folder_id = "518729943705"  # under 635... under 767...
    results = graph.get_identities_with_permissions_on_resource(NodeType.FOLDER, folder_id)

    # ron@test has permissions on ancestor folders
    assert ("ron@test.authomize.com", "roles/resourcemanager.folderAdmin") in results
    assert ("ron@test.authomize.com", "roles/resourcemanager.folderEditor") in results


def test_inherited_permissions_from_organization(graph):
    """Test inherited permissions from the organization level"""
    project_id = "185023072868"  # directly under organization
    results = graph.get_identities_with_permissions_on_resource(NodeType.PROJECT, project_id)

    assert ("exercise-fetcher@striking-arbor-264209.iam.gserviceaccount.com", "roles/owner") in results
    assert ("ron@test.authomize.com", "roles/owner") in results


def test_no_permissions_on_unassigned_resource(graph):
    """Test empty permissions on a resource with no bindings"""
    bucket_id = "authomize-exercise-data"
    results = graph.get_identities_with_permissions_on_resource(NodeType.BUCKET, bucket_id)

    # The bucket only has legacyBucket* permissions from project roles
    assert len(results) > 0  # inherited roles should be included
    assert any("roles/storage.legacyBucketOwner" in r for _, r in results)
