# redash/models/group_permissions.py
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import relationship

from .base import db, GFKBase
from .users import Group, User  # existing Redash models


class GroupObjectPermission(db.Model, GFKBase):
    """
    Group-level object permission for Queries & Dashboards (generic target).

    Semantics:
      - If an object has at least one 'view' grant (user or group), it is
        considered "restricted": only grantees may see/open it.
      - 'modify' grants are for editorial actions and don't change visibility.

    This coexists with the built-in user-level AccessPermission model.
    """
    __tablename__ = "group_object_permissions"

    id = Column(Integer, primary_key=True)

    # Generic foreign key (object type/id) — same pattern as AccessPermission
    object_type = Column(String(100), nullable=False, index=True)
    object_id = Column(Integer, nullable=False, index=True)

    access_type = Column(String(100), nullable=False, index=True)  # 'view'|'modify'

    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False, index=True)
    grantor_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    group = relationship(Group)
    grantor = relationship(User)

    __table_args__ = (
        UniqueConstraint(
            "object_type",
            "object_id",
            "access_type",
            "group_id",
            name="uq_group_object_permissions_unique_grant",
        ),
        Index(
            "ix_group_obj_perm_lookup",
            "object_type",
            "object_id",
            "access_type",
            "group_id",
        ),
    )

    # ---------- Helper API to keep parity with AccessPermission ----------

    @classmethod
    def find(cls, obj, access_type=None):
        q = cls.query.filter(
            cls.object_type == obj.__class__.__tablename__,
            cls.object_id == obj.id,
        )
        if access_type:
            q = q.filter(cls.access_type == access_type)
        return q.all()

    @classmethod
    def grant(cls, obj, access_type, group: Group, grantor: User | None):
        instance = cls.query.filter(
            cls.object_type == obj.__class__.__tablename__,
            cls.object_id == obj.id,
            cls.access_type == access_type,
            cls.group_id == group.id,
        ).first()
        if instance is None:
            instance = cls(
                object_type=obj.__class__.__tablename__,
                object_id=obj.id,
                access_type=access_type,
                group_id=group.id,
                grantor_id=(grantor.id if grantor else None),
            )
            db.session.add(instance)
        return instance

    @classmethod
    def revoke(cls, obj, group: Group, access_type: str):
        cls.query.filter(
            cls.object_type == obj.__class__.__tablename__,
            cls.object_id == obj.id,
            cls.access_type == access_type,
            cls.group_id == group.id,
        ).delete()

    @classmethod
    def exists(cls, obj, access_type: str, user: User):
        """
        Does this user have access through any of their groups?
        """
        if not user or user.is_disabled:
            return False

        # read once; group_ids is a Redash user property
        gids = getattr(user, "group_ids", []) or []
        if not gids:
            return False

        return (
            db.session.query(cls.id)
            .filter(
                cls.object_type == obj.__class__.__tablename__,
                cls.object_id == obj.id,
                cls.access_type == access_type,
                cls.group_id.in_(gids),
            )
            .limit(1)
            .scalar()
            is not None
        )
