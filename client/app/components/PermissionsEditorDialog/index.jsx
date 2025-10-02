/* eslint-disable react/prop-types */
import React, { useCallback, useEffect, useMemo, useState } from "react";
import PropTypes from "prop-types";
import { debounce, get, find } from "lodash";

import { axios } from "@/services/axios";
import Button from "antd/lib/button";
import List from "antd/lib/list";
import Modal from "antd/lib/modal";
import Select from "antd/lib/select";
import Space from "antd/lib/space";
import Tag from "antd/lib/tag";

import Tooltip from "@/components/Tooltip";
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import { toHuman } from "@/lib/utils";
import HelpTrigger from "@/components/HelpTrigger";
import { UserPreviewCard } from "@/components/PreviewCard";
import PlainButton from "@/components/PlainButton";
import notification from "@/services/notification";
import User from "@/services/user";

import "./index.less";

const { Option } = Select;
const DEBOUNCE_SEARCH_DURATION = 200;

const ACCESS_TYPES = ["view", "modify"]; // visibility vs editing

// ---------- Utilities ----------
function normalizePermissionsShape(data) {
  // Accept both shapes:
  // 1) Legacy: { view: [users...], modify: [users...] }
  // 2) New:    { view: { users: [...], groups: [...] }, modify: { users: [...], groups: [...] } }
  const out = {
    view: { users: [], groups: [] },
    modify: { users: [], groups: [] },
  };

  if (!data || typeof data !== "object") return out;

  Object.keys(out).forEach(key => {
    const val = data[key];
    if (!val) return;

    if (Array.isArray(val)) {
      // Legacy: arrays contain users only
      out[key].users = val;
    } else if (typeof val === "object") {
      out[key].users = Array.isArray(val.users) ? val.users : [];
      out[key].groups = Array.isArray(val.groups) ? val.groups : [];
    }
  });

  return out;
}

const searchUsers = searchTerm =>
  User.query({ q: searchTerm })
    .then(({ results }) => results)
    .catch(() => []);

// Groups list from API (works for array or {results})
const listAllGroups = () =>
  axios
    .get("/api/groups")
    .then(res => (Array.isArray(res) ? res : res.results || []))
    .catch(() => []);

// ---------- Small presentational bits ----------
function PermissionsEditorDialogHeader({ context }) {
  return (
    <>
      <div style={{ fontSize: 16, fontWeight: 600, marginBottom: 4 }}>Manage Permissions</div>
      <div className="text-muted" style={{ marginBottom: 12 }}>
        {`Editing this ${context} is enabled for grantees and admins. `}
        <Tooltip title="If there is any View grant (user or group), this item is restricted and hidden from non‑grantees.">
          <Tag color="gold" style={{ marginLeft: 6 }}>
            Visibility rule
          </Tag>
        </Tooltip>
      </div>
    </>
  );
}
PermissionsEditorDialogHeader.propTypes = { context: PropTypes.oneOf(["query", "dashboard"]) };
PermissionsEditorDialogHeader.defaultProps = { context: "query" };

function UserSelect({ onSelect, shouldShowUser }) {
  const [loadingUsers, setLoadingUsers] = useState(true);
  const [users, setUsers] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");

  const debouncedSearchUsers = useCallback(
    debounce(search => searchUsers(search).then(setUsers).finally(() => setLoadingUsers(false)), DEBOUNCE_SEARCH_DURATION),
    []
  );

  useEffect(() => {
    setLoadingUsers(true);
    debouncedSearchUsers(searchTerm);
  }, [debouncedSearchUsers, searchTerm]);

  return (
    <Select
      showSearch
      style={{ width: "100%" }}
      placeholder="Search users…"
      filterOption={false}
      notFoundContent={null}
      value={undefined}
      onSearch={setSearchTerm}
      getPopupContainer={trigger => trigger.parentNode}
      onSelect={onSelect}>
      {loadingUsers ? (
        <Option key="__loading" value={null} disabled>
          Loading...
        </Option>
      ) : (
        users.filter(shouldShowUser).map(user => (
          <Option key={`${user.id}`} value={user.id}>
            {user.name || user.email} <span className="text-muted">({user.email})</span>
          </Option>
        ))
      )}
    </Select>
  );
}
UserSelect.propTypes = {
  onSelect: PropTypes.func,
  shouldShowUser: PropTypes.func,
};
UserSelect.defaultProps = { onSelect: () => {}, shouldShowUser: () => true };

function GroupSelect({ groups, onSelect }) {
  return (
    <Select
      showSearch
      style={{ width: "100%" }}
      placeholder="Pick a group…"
      optionFilterProp="children"
      filterOption={(input, option) => String(option.children).toLowerCase().includes((input || "").toLowerCase())}
      value={undefined}
      onSelect={onSelect}>
      {groups.map(g => (
        <Option key={`${g.id}`} value={g.id}>
          {g.name} <span className="text-muted">#{g.id}</span>
        </Option>
      ))}
    </Select>
  );
}
GroupSelect.propTypes = {
  groups: PropTypes.arrayOf(PropTypes.object).isRequired,
  onSelect: PropTypes.func,
};
GroupSelect.defaultProps = { onSelect: () => {} };

// ---------- Main dialog ----------
function PermissionsEditorDialog({ dialog, author, context, aclUrl }) {
  const [loading, setLoading] = useState(true);
  const [grants, setGrants] = useState(() => ({
    view: { users: [], groups: [] },
    modify: { users: [], groups: [] },
  }));
  const [allGroups, setAllGroups] = useState([]);

  const loadAll = useCallback(() => {
    setLoading(true);
    Promise.all([
      axios.get(aclUrl).catch(() => ({})), // permissions map
      listAllGroups(),
    ])
      .then(([perm, groups]) => {
        setGrants(normalizePermissionsShape(perm));
        setAllGroups(groups);
      })
      .catch(() => notification.error("Failed to load permissions"))
      .finally(() => setLoading(false));
  }, [aclUrl]);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  // ----- grant/revoke helpers -----
  const grantUser = useCallback(
    (accessType, userId) =>
      axios
        .post(aclUrl, { access_type: accessType, user_id: userId })
        .then(loadAll)
        .catch(() => notification.error("Could not grant permission to the user")),
    [aclUrl, loadAll]
  );

  const revokeUser = useCallback(
    (accessType, userId) =>
      axios
        .delete(aclUrl, { data: { access_type: accessType, user_id: userId } })
        .then(loadAll)
        .catch(() => notification.error("Could not remove permission from the user")),
    [aclUrl, loadAll]
  );

  const grantGroup = useCallback(
    (accessType, groupId) =>
      axios
        .post(aclUrl, { access_type: accessType, group_id: groupId })
        .then(loadAll)
        .catch(() => notification.error("Could not grant permission to the group")),
    [aclUrl, loadAll]
  );

  const revokeGroup = useCallback(
    (accessType, groupId) =>
      axios
        .delete(aclUrl, { data: { access_type: accessType, group_id: groupId } })
        .then(loadAll)
        .catch(() => notification.error("Could not remove permission from the group")),
    [aclUrl, loadAll]
  );

  // ----- selection filters -----
  const userHasPermission = useCallback(
    (u, type) =>
      u.id === author.id ||
      !!get(find(grants[type].users, { id: u.id }), "id"),
    [author.id, grants]
  );

  const availableGroups = useMemo(() => {
    // Don’t show groups that already have the grant
    const grantedIds = new Set([
      ...grants.view.groups.map(g => g.id),
      ...grants.modify.groups.map(g => g.id),
    ]);
    return allGroups.filter(g => !grantedIds.has(g.id));
  }, [allGroups, grants]);

  // ----- render helpers -----
  function UserGrants({ type }) {
    return (
      <>
        <div style={{ fontWeight: 600, marginTop: 16, marginBottom: 8 }}>
          {toHuman(type)} — grant to users
        </div>
        <Space.Compact style={{ width: "100%", marginBottom: 8 }}>
          <UserSelect onSelect={userId => grantUser(type, userId)} shouldShowUser={u => !userHasPermission(u, type)} />
          <Button type="primary" disabled>
            Grant
          </Button>
        </Space.Compact>

        <List
          bordered
          size="small"
          dataSource={grants[type].users}
          locale={{ emptyText: "No users granted yet" }}
          renderItem={user => (
            <List.Item
              actions={[
                user.id !== author.id ? (
                  <PlainButton type="danger" onClick={() => revokeUser(type, user.id)} data-test="RevokeUser">
                    Revoke
                  </PlainButton>
                ) : (
                  <Tag color="green">Author</Tag>
                ),
              ]}>
              <UserPreviewCard user={user} />
            </List.Item>
          )}
        />
      </>
    );
  }

  function GroupGrants({ type }) {
    return (
      <>
        <div style={{ fontWeight: 600, marginTop: 16, marginBottom: 8 }}>
          {toHuman(type)} — grant to groups
        </div>
        <Space.Compact style={{ width: "100%", marginBottom: 8 }}>
          <GroupSelect groups={availableGroups} onSelect={groupId => grantGroup(type, groupId)} />
          <Button disabled>Grant</Button>
        </Space.Compact>

        <List
          bordered
          size="small"
          dataSource={grants[type].groups}
          locale={{ emptyText: "No groups granted yet" }}
          renderItem={group => (
            <List.Item
              actions={[
                <PlainButton type="danger" onClick={() => revokeGroup(type, group.id)} key="revoke-group" data-test="RevokeGroup">
                  Revoke
                </PlainButton>,
              ]}>
              <Space size="small" wrap>
                <Tag color="purple">Group</Tag>
                <span style={{ fontWeight: 500 }}>{group.name}</span>
                <span className="text-muted">#{group.id}</span>
              </Space>
            </List.Item>
          )}
        />
      </>
    );
  }

  return (
    <Modal
      className="permissions-editor-dialog"
      title={<PermissionsEditorDialogHeader context={context} />}
      {...dialog.props}
      footer={
        <div style={{ display: "flex", justifyContent: "space-between", width: "100%" }}>
          <div>
            <HelpTrigger
              type="PERMISSIONS"
              title="How visibility works"
              className="m-r-10">
              <div style={{ maxWidth: 420 }}>
                <p className="text-muted" style={{ marginBottom: 6 }}>
                  <strong>View</strong> grants (to users or groups) make the object <em>restricted</em>:
                  only grantees (and admins) can see or open it.
                </p>
                <p className="text-muted" style={{ margin: 0 }}>
                  <strong>Modify</strong> grants let users edit the object; they do not affect visibility.
                </p>
              </div>
            </HelpTrigger>
          </div>
          <div>
            <Button onClick={() => dialog.close()}>Close</Button>
          </div>
        </div>
      }>
      {loading ? (
        <div>Loading...</div>
      ) : (
        <>
          {/* VIEW (visibility) */}
          <div style={{ marginBottom: 12 }}>
            <Tag color="gold">Visibility</Tag> Anyone granted <strong>View</strong> (user or group) can see this {context}.
          </div>
          <UserGrants type="view" />
          <GroupGrants type="view" />

          {/* MODIFY (edit rights) */}
          <div style={{ marginTop: 24, marginBottom: 12 }}>
            <Tag>Edit Rights</Tag> Users/groups with <strong>Modify</strong> can edit this {context}.
          </div>
          <UserGrants type="modify" />
          <GroupGrants type="modify" />
        </>
      )}
    </Modal>
  );
}

PermissionsEditorDialog.propTypes = {
  dialog: DialogPropType.isRequired,
  author: PropTypes.object.isRequired, // shape from your codebase; we keep object to avoid tight coupling
  context: PropTypes.oneOf(["query", "dashboard"]),
  aclUrl: PropTypes.string.isRequired, // /api/permissions/{queries|dashboards}/{id}
};

PermissionsEditorDialog.defaultProps = { context: "query" };

export default wrapDialog(PermissionsEditorDialog);
