import { kea, key, path, props, events } from 'kea'
import { loaders } from 'kea-loaders'

import { query } from '~/queries/query'
import { NodeKind, SessionsTimelineQuery, SessionsTimelineQueryResponse } from '~/queries/schema'

export type NotebookNodePersonFeedLogicProps = {
    personId: string
}

export const notebookNodePersonFeedLogic = kea([
    props({} as NotebookNodePersonFeedLogicProps),
    path((key) => ['scenes', 'notebooks', 'Notebook', 'Nodes', 'notebookNodePersonFeedLogic', key]),
    key(({ personId }) => personId),

    loaders(() => ({
        sessions: [
            null as SessionsTimelineQueryResponse['results'] | null,
            {
                loadSessionsTimeline: async () => {
                    const result = await query<SessionsTimelineQuery>({
                        kind: NodeKind.SessionsTimelineQuery,
                        after: '2021-01-01T18:00:00Z',
                        before: '2024-01-01T06:00:00Z',
                    })
                    return result.results
                },
            },
        ],
    })),
    events(({ actions, props }) => ({
        afterMount: [
            actions.loadSessionsTimeline,
            () => {
                // actions.loadSessionsTimeline(props.personId)
            },
        ],
    })),
])
