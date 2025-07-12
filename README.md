# PermissionGraph

## This project models a Google Cloud resource hierarchy as a graph, including organizations, folders, projects, and service identities. 

### It supports queries like:

- Retrieving all identities with access to a resource

- Inheriting permissions across parent-child relationships

- Creating resource structures and bindings

### Features
- Graph-based IAM permission modeling

- Permission inheritance resolution

- Resource hierarchy traversal

- Test suite using pytest

### Running test suite with a pre-built test graph (original data is from assets_iam.json file):
Make sure pytest is installed:

```bash
pip install pytest
```

Then run:

```bash
pytest -s
```

Use -s flag to see test output like printed permissions.


