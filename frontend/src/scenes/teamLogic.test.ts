import { MOCK_DEFAULT_TEAM, MOCK_TEAM_ID } from 'lib/api.mock'

import { expectLogic } from 'kea-test-utils'

import { initKeaTests } from '~/test/init'
import { AppContext } from '~/types'

import { sanitizeTestAccountFilters, teamLogic } from './teamLogic'

describe('teamLogic', () => {
    describe('sanitizeTestAccountFilters', () => {
        it('returns arrays unchanged', () => {
            const filters = [{ key: 'email', value: '@x.com', operator: 'not_icontains', type: 'person' }]
            expect(sanitizeTestAccountFilters(filters)).toBe(filters)
        })

        it('parses a JSON-encoded array string', () => {
            const filters = [{ key: 'email', value: '@x.com', operator: 'not_icontains', type: 'person' }]
            expect(sanitizeTestAccountFilters(JSON.stringify(filters))).toEqual(filters)
        })

        it.each([
            ['undefined', undefined],
            ['null', null],
            ['a plain string', 'not-a-list'],
            ['a JSON object string', '{"key":"email"}'],
            ['an object', { key: 'email' }],
            ['a number', 1],
        ])('returns [] for %s', (_label, value) => {
            expect(sanitizeTestAccountFilters(value)).toEqual([])
        })
    })

    let logic: ReturnType<typeof teamLogic.build>

    describe('when team is loaded', () => {
        beforeEach(() => {
            initKeaTests()
            logic = teamLogic()
            logic.mount()
        })

        it('currentTeamIdStrict returns the team id', async () => {
            await expectLogic(logic).toDispatchActions(['loadCurrentTeamSuccess'])
            expect(logic.values.currentTeamIdStrict).toBe(MOCK_TEAM_ID)
        })

        it('currentProjectId returns the project id', async () => {
            await expectLogic(logic).toDispatchActions(['loadCurrentTeamSuccess'])
            expect(logic.values.currentProjectId).toBe(MOCK_DEFAULT_TEAM.project_id)
        })
    })

    describe('before team is loaded', () => {
        beforeEach(() => {
            initKeaTests(false)
            // Clear team context after initKeaTests so currentTeam starts as null
            window.POSTHOG_APP_CONTEXT = {
                ...window.POSTHOG_APP_CONTEXT,
                current_team: undefined,
            } as unknown as AppContext
            logic = teamLogic()
            logic.mount()
        })

        it('currentTeamIdStrict returns @current fallback', () => {
            expect(logic.values.currentTeamIdStrict).toBe('@current')
        })

        it('currentProjectId returns @current fallback', () => {
            expect(logic.values.currentProjectId).toBe('@current')
        })

        it('currentTeamId returns null (non-breaking)', () => {
            expect(logic.values.currentTeamId).toBeNull()
        })
    })
})
