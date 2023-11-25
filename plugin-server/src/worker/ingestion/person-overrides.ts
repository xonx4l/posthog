import { ProducerRecord } from 'kafkajs'
import { DateTime } from 'luxon'

import { KAFKA_PERSON_OVERRIDE } from '../../config/kafka-topics'
import { Person, TimestampFormat } from '../../types'
import { DB } from '../../utils/db/db'
import { TransactionClient } from '../../utils/db/postgres'
import { status } from '../../utils/status'
import { castTimestampOrNow } from '../../utils/utils'
import { SQL } from './person-state'

type PersonOverride = {
    teamId: number
    oldPersonUuid: string
    overridePersonUuid: string
    oldestEvent: DateTime // TODO: is this something that should be in the log table, or resolved at merge time?e
}

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/require-await -- work in progress */

export class DeferredPersonOverrideWriter {
    constructor(private db: DB) {}

    public async addPersonOverride(
        tx: TransactionClient,
        teamId: number,
        oldPerson: Person,
        overridePerson: Person
    ): Promise<void> {
        // TODO: validate both persons have the expected teamId
        const record: PersonOverride = {
            teamId,
            oldPersonUuid: oldPerson.uuid,
            overridePersonUuid: overridePerson.uuid,
            oldestEvent: overridePerson.created_at,
        }
    }
}

/* eslint-enable */

class _PersonOverrideWriter {
    constructor(private db: DB) {}

    public async addPersonOverride(tx: TransactionClient, record: PersonOverride): Promise<ProducerRecord> {
        const mergedAt = DateTime.now()
        /**
            We'll need to do 4 updates:

         1. Add the persons involved to the helper table (2 of them)
         2. Add an override from oldPerson to override person
         3. Update any entries that have oldPerson as the override person to now also point to the new override person. Note that we don't update `oldest_event`, because it's a heuristic (used to optimise squashing) tied to the old_person and nothing changed about the old_person who's events need to get squashed.
         */
        const oldPersonMappingId = await this.addPersonOverrideMapping(tx, record.teamId, record.oldPersonUuid)
        const overridePersonMappingId = await this.addPersonOverrideMapping(
            tx,
            record.teamId,
            record.overridePersonUuid
        )

        await this.db.postgres.query(
            tx,
            SQL`
                INSERT INTO posthog_personoverride (
                    team_id,
                    old_person_id,
                    override_person_id,
                    oldest_event,
                    version
                ) VALUES (
                    ${record.teamId},
                    ${oldPersonMappingId},
                    ${overridePersonMappingId},
                    ${record.oldestEvent},
                    0
                )
            `,
            undefined,
            'personOverride'
        )

        // The follow-up JOIN is required as ClickHouse requires UUIDs, so we need to fetch the UUIDs
        // of the IDs we updated from the mapping table.
        const { rows: transitiveUpdates } = await this.db.postgres.query(
            tx,
            SQL`
                WITH updated_ids AS (
                    UPDATE
                        posthog_personoverride
                    SET
                        override_person_id = ${overridePersonMappingId}, version = COALESCE(version, 0)::numeric + 1
                    WHERE
                        team_id = ${record.teamId} AND override_person_id = ${oldPersonMappingId}
                    RETURNING
                        old_person_id,
                        version,
                        oldest_event
                )
                SELECT
                    helper.uuid as old_person_id,
                    updated_ids.version,
                    updated_ids.oldest_event
                FROM
                    updated_ids
                JOIN
                    posthog_personoverridemapping helper
                ON
                    helper.id = updated_ids.old_person_id;
            `,
            undefined,
            'transitivePersonOverrides'
        )

        status.debug('🔁', 'person_overrides_updated', { transitiveUpdates })

        const personOverrideMessages: ProducerRecord = {
            topic: KAFKA_PERSON_OVERRIDE,
            messages: [
                {
                    value: JSON.stringify({
                        team_id: record.teamId,
                        merged_at: castTimestampOrNow(mergedAt, TimestampFormat.ClickHouse),
                        override_person_id: record.overridePersonUuid,
                        old_person_id: record.oldPersonUuid,
                        oldest_event: castTimestampOrNow(record.oldestEvent, TimestampFormat.ClickHouse),
                        version: 0,
                    }),
                },
                ...transitiveUpdates.map(({ old_person_id, version, oldest_event }) => ({
                    value: JSON.stringify({
                        team_id: record.teamId,
                        merged_at: castTimestampOrNow(mergedAt, TimestampFormat.ClickHouse),
                        override_person_id: record.overridePersonUuid,
                        old_person_id: old_person_id,
                        oldest_event: castTimestampOrNow(oldest_event, TimestampFormat.ClickHouse),
                        version: version,
                    }),
                })),
            ],
        }

        return personOverrideMessages
    }

    private async addPersonOverrideMapping(tx: TransactionClient, teamId: number, personUuid: string): Promise<number> {
        /**
            Update the helper table that serves as a mapping between a serial ID and a Person UUID.

            This mapping is used to enable an exclusion constraint in the personoverrides table, which
            requires int[], while avoiding any constraints on "hotter" tables, like person.
         **/
        // ON CONFLICT nothing is returned, so we get the id in the second SELECT statement below.
        // Fear not, the constraints on personoverride will handle any inconsistencies.
        // This mapping table is really nothing more than a mapping to support exclusion constraints
        // as we map int ids to UUIDs (the latter not supported in exclusion contraints).
        const {
            rows: [{ id }],
        } = await this.db.postgres.query(
            tx,
            `WITH insert_id AS (
                    INSERT INTO posthog_personoverridemapping(
                        team_id,
                        uuid
                    )
                    VALUES (
                        ${teamId},
                        '${personUuid}'
                    )
                    ON CONFLICT("team_id", "uuid") DO NOTHING
                    RETURNING id
                )
                SELECT * FROM insert_id
                UNION ALL
                SELECT id
                FROM posthog_personoverridemapping
                WHERE uuid = '${personUuid}'
            `,
            undefined,
            'personOverrideMapping'
        )

        return id
    }
}
