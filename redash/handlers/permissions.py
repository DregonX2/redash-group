# redash/handlers/permissions.py
from collections import defaultdict

from flask import request
from flask_restful import abort
from sqlalchemy.orm.exc import NoResultFound

from redash.handlers.base import BaseResource, get_object_or_404
from redash.models import AccessPermission, Dashboard, Query, User, db
from redash.models.group_permissions import GroupObjectPermission
from redash.permissions import ACCESS_TYPES, require_admin_or_owner


# Map API object types to model classes
_MODEL_BY_TYPE = {"queries": Query, "dashboards": Dashboard}


def _get_model_from_type(type_name: str):
    model = _MODEL_BY_TYPE.get(type_name)
    if model is None:
        abort(404)
    return model


class ObjectPermissionsResource(BaseResource):
    """
    Unified permissions endpoint:
      GET    -> returns current user+group grants grouped by access type
      POST   -> grant to user or group
      DELETE -> revoke from user or group

    Backward compatible with existing payloads:
      - user grant:   {"access_type": "view", "user_id": 123}
      - group grant:  {"access_type": "view", "group_id": 456}

    Response format (example):
    {
      "view": {
        "users": [<user dict>, ...],
        "groups": [<group dict>, ...],
        "all_grantees": [<user/group dicts with {"kind": "user"|"group"}>]
      },
      "modify": { ... }
    }
    """

    def _load_object(self, object_type, object_id):
        model = _get_model_from_type(object_type)
        return get_object_or_404(model.get_by_id_and_org, object_id, self.current_org)

    def get(self, object_type, object_id):
        obj = self._load_object(object_type, object_id)

        # Fetch user-level permissions (built-in)
        user_perms = AccessPermission.find(obj)
        # Fetch group-level permissions (new)
        group_perms = GroupObjectPermission.find(obj)

        by_type = defaultdict(lambda: {"users": [], "groups": [], "all_grantees": []})

        # Users
        for perm in user_perms:
            payload = perm.grantee.to_dict()
            payload["kind"] = "user"
            by_type[perm.access_type]["users"].append(payload)
            by_type[perm.access_type]["all_grantees"].append(payload)

        # Groups
        for gperm in group_perms:
            g = gperm.group
            payload = {"id": g.id, "name": g.name, "type": "group", "kind": "group"}
            by_type[gperm.access_type]["groups"].append(payload)
            by_type[gperm.access_type]["all_grantees"].append(payload)

        return by_type

    def post(self, object_type, object_id):
        obj = self._load_object(object_type, object_id)
        require_admin_or_owner(obj.user_id)

        req = request.get_json(True) or {}

        access_type = req.get("access_type")
        if access_type not in ACCESS_TYPES:
            abort(400, message="Unknown access type.")

        # Grant to user?
        if "user_id" in req:
            try:
                grantee = User.get_by_id_and_org(req["user_id"], self.current_org)
            except NoResultFound:
                abort(400, message="User not found.")

            permission = AccessPermission.grant(obj, access_type, grantee, self.current_user)
            db.session.commit()

            self.record_event(
                {
                    "action": "grant_permission",
                    "object_id": object_id,
                    "object_type": object_type,
                    "grantee": grantee.id,
                    "grantee_kind": "user",
                    "access_type": access_type,
                }
            )
            return permission.to_dict()

        # Grant to group?
        if "group_id" in req:
            group_id = req["group_id"]
            # Verify group exists within org; Group model lives in models.users
            from redash.models.users import Group

            group = Group.query.filter(Group.id == group_id, Group.org == self.current_org).first()
            if group is None:
                abort(400, message="Group not found.")

            permission = GroupObjectPermission.grant(obj, access_type, group, self.current_user)
            db.session.commit()

            self.record_event(
                {
                    "action": "grant_permission",
                    "object_id": object_id,
                    "object_type": object_type,
                    "grantee": group.id,
                    "grantee_kind": "group",
                    "access_type": access_type,
                }
            )
            # Match AccessPermission's dict shape minimally
            return {
                "id": permission.id,
                "object_type": permission.object_type,
                "object_id": permission.object_id,
                "access_type": permission.access_type,
                "grantee_kind": "group",
                "group": {"id": group.id, "name": group.name},
            }

        abort(400, message="Missing 'user_id' or 'group_id' in payload.")

    def delete(self, object_type, object_id):
        obj = self._load_object(object_type, object_id)
        require_admin_or_owner(obj.user_id)

        req = request.get_json(True) or {}
        access_type = req.get("access_type")

        if access_type not in ACCESS_TYPES:
            abort(400, message="Unknown access type.")

        # Revoke from user?
        if "user_id" in req:
            grantee = User.query.get(req["user_id"])
            if grantee is None:
                abort(400, message="User not found.")
            AccessPermission.revoke(obj, grantee, access_type)
            db.session.commit()

            self.record_event(
                {
                    "action": "revoke_permission",
                    "object_id": object_id,
                    "object_type": object_type,
                    "access_type": access_type,
                    "grantee_kind": "user",
                    "grantee_id": grantee.id,
                }
            )
            return {"ok": True}

        # Revoke from group?
        if "group_id" in req:
            from redash.models.users import Group

            group = Group.query.get(req["group_id"])
            if group is None:
                abort(400, message="Group not found.")
            GroupObjectPermission.revoke(obj, group, access_type)
            db.session.commit()

            self.record_event(
                {
                    "action": "revoke_permission",
                    "object_id": object_id,
                    "object_type": object_type,
                    "access_type": access_type,
                    "grantee_kind": "group",
                    "grantee_id": group.id,
                }
            )
            return {"ok": True}

        abort(400, message="Missing 'user_id' or 'group_id' in payload.")


class CheckPermissionResource(BaseResource):
    """
    Backward-compatible 'check' endpoint; now consults both user & group grants.
    """
    def get(self, object_type, object_id, access_type):
        model = _get_model_from_type(object_type)
        obj = get_object_or_404(model.get_by_id_and_org, object_id, self.current_org)

        # user-level
        user_has = AccessPermission.exists(obj, access_type, self.current_user)

        # group-level
        group_has = GroupObjectPermission.exists(obj, access_type, self.current_user)

        return {"response": bool(user_has or group_has)}
