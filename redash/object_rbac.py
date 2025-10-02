# redash/object_rbac.py
"""
Runtime enforcement for object-level RBAC.

Rules:
  - If a Query/Dashboard has at least one 'view' grant (user or group),
    it is considered "restricted":
      * Only explicitly granted users or members of granted groups
        will see it in list/search and can fetch/open it.
  - If there are no 'view' grants, visibility falls back to Redash's
    data source / group permissions (existing behavior).

Implementation:
  - Uses SQLAlchemy 1.4's 'do_orm_execute' + with_loader_criteria to inject
    a WHERE condition on SELECTs for the Query and Dashboard entities.
  - No large core-file diffs; enabled by importing this module once.
"""

from flask_login import current_user
from sqlalchemy import and_, or_, exists, literal
from sqlalchemy import event
from sqlalchemy.orm import with_loader_criteria

from redash.models import db, AccessPermission, Query, Dashboard
from redash.models.group_permissions import GroupObjectPermission


def _visibility_clause(model_cls, object_type_literal):
    """
    Build a SQLAlchemy boolean expression implementing:
      (NOT restricted) OR (user_granted) OR (group_granted)

    Where 'restricted' means: there EXISTS at least one 'view' grant
    (user or group) for this object.
    """
    ap = AccessPermission.__table__.c
    gp = GroupObjectPermission.__table__.c

    # EXISTS any user or group 'view' grant for this object
    restricted = or_(
        exists().where(
            and_(
                ap.object_type == object_type_literal,
                ap.object_id == model_cls.id,
                ap.access_type == literal("view"),
            )
        ),
        exists().where(
            and_(
                gp.object_type == object_type_literal,
                gp.object_id == model_cls.id,
                gp.access_type == literal("view"),
            )
        ),
    )

    # current user context
    try:
        uid = getattr(current_user, "id", None)
        gids = list(getattr(current_user, "group_ids", []) or [])
        is_auth = bool(getattr(current_user, "is_authenticated", False))
    except Exception:
        uid, gids, is_auth = None, [], False

    # EXISTS a user grant
    user_granted = exists().where(
        and_(
            ap.object_type == object_type_literal,
            ap.object_id == model_cls.id,
            ap.access_type == literal("view"),
            ap.grantee_id == literal(uid),
        )
    ) if is_auth and uid is not None else literal(False)

    # EXISTS a group grant
    group_granted = exists().where(
        and_(
            gp.object_type == object_type_literal,
            gp.object_id == model_cls.id,
            gp.access_type == literal("view"),
            gp.group_id.in_(gids if gids else [literal(-1)]),
        )
    ) if is_auth and gids else literal(False)

    # Allow when not restricted OR user/group is granted
    return or_(~restricted, user_granted, group_granted)


@event.listens_for(db.session, "do_orm_execute")
def _inject_object_rbac(execute_state):
    """
    Injects visibility predicates for Query and Dashboard on SELECT statements.
    """
    if not execute_state.is_select:
        return

    stmt = execute_state.statement

    stmt = stmt.options(
        with_loader_criteria(
            Query,
            lambda cls: _visibility_clause(Query, literal("queries")),
            include_aliases=True,
        ),
        with_loader_criteria(
            Dashboard,
            lambda cls: _visibility_clause(Dashboard, literal("dashboards")),
            include_aliases=True,
        ),
    )

    execute_state.statement = stmt


def _create_table_if_missing():
    """
    Ensure the new table exists without requiring an Alembic migration.
    (Safe in Dockerized deployments where manage.py upgrades aren't run immediately.)
    """
    try:
        GroupObjectPermission.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        # Don't hard-fail app startup if DDL permissions are missing; admins can
        # create via migrations too.
        pass


# Initialize on import
_create_table_if_missing()
