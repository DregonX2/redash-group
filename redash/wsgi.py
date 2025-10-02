# redash/wsgi.py
from redash.app import create_app

# Create the Flask application
app = create_app()

# Activate object-level RBAC enforcement (adds ORM filters & ensures table).
import redash.object_rbac  # noqa: F401
