import { actions, afterMount, kea, key, listeners, path, props, reducers, selectors } from 'kea'
import { loaders } from 'kea-loaders'

import api from 'lib/api'
import { membersLogic } from 'scenes/organization/membersLogic'
import { rolesLogic } from 'scenes/settings/organization/Permissions/Roles/rolesLogic'

import type { propertyAccessControlLogicType } from './propertyAccessControlLogicType'

export interface PropertyAccessControlLogicProps {
    propertyDefinitionId: string
    teamId: number
}

export interface AccessControlRule {
    id: string
    access_level: string
    organization_member: string | null
    role: string | null
}

export interface AccessControlResponse {
    access_controls: AccessControlRule[]
    available_access_levels: string[]
    default_access_level: string
}

// Local state shape: tracks the user's pending edits
export interface LocalAccessControlState {
    defaultLevel: string
    // Maps member/role ID to their override level, or null meaning "remove override"
    memberOverrides: Record<string, string | null>
    roleOverrides: Record<string, string | null>
}

export const propertyAccessControlLogic = kea<propertyAccessControlLogicType>([
    path(['scenes', 'data-management', 'definition', 'propertyAccessControlLogic']),
    props({} as PropertyAccessControlLogicProps),
    key((props) => props.propertyDefinitionId),

    actions({
        setLocalDefaultLevel: (level: string) => ({ level }),
        setLocalMemberOverride: (memberId: string, level: string | null) => ({ memberId, level }),
        setLocalRoleOverride: (roleId: string, level: string | null) => ({ roleId, level }),
        resetLocalState: true,
        saveAccessControls: true,
        setActiveTab: (tab: string) => ({ tab }),
    }),

    loaders(({ props }) => ({
        remoteState: [
            null as AccessControlResponse | null,
            {
                loadRemoteState: async () => {
                    return await api.get<AccessControlResponse>(
                        `api/projects/${props.teamId}/property_access_controls/?property_definition_id=${encodeURIComponent(
                            props.propertyDefinitionId
                        )}`
                    )
                },
            },
        ],
    })),

    reducers({
        localState: [
            null as LocalAccessControlState | null,
            {
                loadRemoteStateSuccess: (_, { remoteState }) => {
                    if (!remoteState) {
                        return null
                    }
                    const memberOverrides: Record<string, string | null> = {}
                    const roleOverrides: Record<string, string | null> = {}
                    for (const rule of remoteState.access_controls) {
                        if (rule.organization_member) {
                            memberOverrides[rule.organization_member] = rule.access_level
                        } else if (rule.role) {
                            roleOverrides[rule.role] = rule.access_level
                        }
                    }
                    return {
                        defaultLevel: remoteState.default_access_level,
                        memberOverrides,
                        roleOverrides,
                    }
                },
                setLocalDefaultLevel: (state, { level }) => (state ? { ...state, defaultLevel: level } : state),
                setLocalMemberOverride: (state, { memberId, level }) =>
                    state ? { ...state, memberOverrides: { ...state.memberOverrides, [memberId]: level } } : state,
                setLocalRoleOverride: (state, { roleId, level }) =>
                    state ? { ...state, roleOverrides: { ...state.roleOverrides, [roleId]: level } } : state,
                resetLocalState: () => null,
            },
        ],
        activeTab: [
            'members' as string,
            {
                setActiveTab: (_, { tab }) => tab,
            },
        ],
    }),

    selectors({
        defaultLevel: [(s) => [s.localState], (localState): string => localState?.defaultLevel ?? 'read_write'],
        memberOverrides: [
            (s) => [s.localState],
            (localState): Record<string, string | null> => localState?.memberOverrides ?? {},
        ],
        roleOverrides: [
            (s) => [s.localState],
            (localState): Record<string, string | null> => localState?.roleOverrides ?? {},
        ],
        allMembers: [
            () => [membersLogic.selectors.members],
            (members): { id: string; first_name: string; last_name: string; email: string }[] =>
                (members ?? []).map((member: any) => ({
                    id: member.id,
                    first_name: member.user?.first_name ?? '',
                    last_name: member.user?.last_name ?? '',
                    email: member.user?.email ?? '',
                })),
        ],
        allRoles: [
            () => [rolesLogic.selectors.roles],
            (roles): { id: string; name: string; members: any[] }[] =>
                (roles ?? []).map((role: any) => ({
                    id: role.id,
                    name: role.name,
                    members: (role.members ?? []).map((m: any) => ({
                        id: m.id,
                        first_name: m.user?.first_name ?? '',
                        last_name: m.user?.last_name ?? '',
                        email: m.user?.email ?? '',
                    })),
                })),
        ],
        hasChanges: [
            (s) => [s.localState, s.remoteState],
            (localState, remoteState): boolean => {
                if (!localState || !remoteState) {
                    return false
                }
                // Check default level
                if (localState.defaultLevel !== remoteState.default_access_level) {
                    return true
                }
                // Build remote overrides for comparison
                const remoteMemberOverrides: Record<string, string> = {}
                const remoteRoleOverrides: Record<string, string> = {}
                for (const rule of remoteState.access_controls) {
                    if (rule.organization_member) {
                        remoteMemberOverrides[rule.organization_member] = rule.access_level
                    } else if (rule.role) {
                        remoteRoleOverrides[rule.role] = rule.access_level
                    }
                }
                // Check member overrides
                const allMemberIds = new Set([
                    ...Object.keys(localState.memberOverrides),
                    ...Object.keys(remoteMemberOverrides),
                ])
                for (const id of allMemberIds) {
                    const local = localState.memberOverrides[id] ?? undefined
                    const remote = remoteMemberOverrides[id] ?? undefined
                    if (local !== remote) {
                        return true
                    }
                }
                // Check role overrides
                const allRoleIds = new Set([
                    ...Object.keys(localState.roleOverrides),
                    ...Object.keys(remoteRoleOverrides),
                ])
                for (const id of allRoleIds) {
                    const local = localState.roleOverrides[id] ?? undefined
                    const remote = remoteRoleOverrides[id] ?? undefined
                    if (local !== remote) {
                        return true
                    }
                }
                return false
            },
        ],
    }),

    listeners(({ values, actions, props }) => ({
        saveAccessControls: async () => {
            if (!values.localState || !values.remoteState) {
                return
            }
            const endpoint = `api/projects/${props.teamId}/property_access_controls/`

            // Save default level if changed
            if (values.localState.defaultLevel !== values.remoteState.default_access_level) {
                await api.create(endpoint, {
                    property_definition_id: props.propertyDefinitionId,
                    access_level: values.localState.defaultLevel,
                })
            }

            // Build remote override maps for diffing
            const remoteMemberOverrides: Record<string, string> = {}
            const remoteRoleOverrides: Record<string, string> = {}
            for (const rule of values.remoteState.access_controls) {
                if (rule.organization_member) {
                    remoteMemberOverrides[rule.organization_member] = rule.access_level
                } else if (rule.role) {
                    remoteRoleOverrides[rule.role] = rule.access_level
                }
            }

            // Save member override changes
            for (const [memberId, level] of Object.entries(values.localState.memberOverrides)) {
                const remoteLevel = remoteMemberOverrides[memberId] ?? undefined
                if (level !== remoteLevel) {
                    await api.create(endpoint, {
                        property_definition_id: props.propertyDefinitionId,
                        access_level: level,
                        organization_member: memberId,
                    })
                }
            }
            // Handle removed member overrides (was in remote but set to null locally)
            for (const memberId of Object.keys(remoteMemberOverrides)) {
                if (values.localState.memberOverrides[memberId] === null) {
                    await api.create(endpoint, {
                        property_definition_id: props.propertyDefinitionId,
                        access_level: null,
                        organization_member: memberId,
                    })
                }
            }

            // Save role override changes
            for (const [roleId, level] of Object.entries(values.localState.roleOverrides)) {
                const remoteLevel = remoteRoleOverrides[roleId] ?? undefined
                if (level !== remoteLevel) {
                    await api.create(endpoint, {
                        property_definition_id: props.propertyDefinitionId,
                        access_level: level,
                        role: roleId,
                    })
                }
            }
            // Handle removed role overrides
            for (const roleId of Object.keys(remoteRoleOverrides)) {
                if (values.localState.roleOverrides[roleId] === null) {
                    await api.create(endpoint, {
                        property_definition_id: props.propertyDefinitionId,
                        access_level: null,
                        role: roleId,
                    })
                }
            }

            // Reload remote state to sync
            actions.loadRemoteState()
        },
    })),

    afterMount(({ actions }) => {
        actions.loadRemoteState()
        membersLogic.actions.ensureAllMembersLoaded()
        rolesLogic.actions.loadRoles()
    }),
])
