import { expect, test, beforeAll, afterAll } from 'vitest'
import pgMeta from '../src/index'
import { createTestDatabase, cleanupRoot } from './db/utils'

beforeAll(async () => {
  // Any global setup if needed
})

afterAll(async () => {
  await cleanupRoot()
})

const withTestDatabase = (
  name: string,
  fn: (db: Awaited<ReturnType<typeof createTestDatabase>>) => Promise<void>
) => {
  test(name, async () => {
    const db = await createTestDatabase()
    try {
      await fn(db)
    } finally {
      await db.cleanup()
    }
  })
}

withTestDatabase('list roles', async ({ executeQuery }) => {
  const { sql } = await pgMeta.roles.list()
  const rawRes = await executeQuery(sql)
  const res = pgMeta.roles.zod.array().parse(rawRes)

  let role = res.find(({ name }) => name === 'postgres')

  expect(role).toMatchInlineSnapshot(
    { activeConnections: expect.any(Number), id: expect.any(Number) },
    `
    {
      "activeConnections": Any<Number>,
      "canBypassRls": true,
      "canCreateDb": true,
      "canCreateRole": true,
      "canLogin": true,
      "config": null,
      "connectionLimit": 100,
      "id": Any<Number>,
      "inheritRole": true,
      "isReplicationRole": true,
      "isSuperuser": true,
      "name": "postgres",
      "password": "********",
      "validUntil": null,
    }
  `
  )

  // pg_monitor is a predefined role. `includeDefaultRoles` defaults to false,
  // so it shouldn't be included in the result.
  role = res.find(({ name }) => name === 'pg_monitor')

  expect(role).toMatchInlineSnapshot(`undefined`)
})

withTestDatabase('list roles w/ default roles', async ({ executeQuery }) => {
  const { sql } = await pgMeta.roles.list({ includeDefaultRoles: true })
  const rawRes = await executeQuery(sql)
  const res = pgMeta.roles.zod.array().parse(rawRes)

  const role = res.find(({ name }) => name === 'pg_monitor')

  expect(role).toMatchInlineSnapshot(
    {
      activeConnections: expect.any(Number),
      id: expect.any(Number),
    },
    `
    {
      "activeConnections": Any<Number>,
      "canBypassRls": false,
      "canCreateDb": false,
      "canCreateRole": false,
      "canLogin": false,
      "config": null,
      "connectionLimit": 100,
      "id": Any<Number>,
      "inheritRole": true,
      "isReplicationRole": false,
      "isSuperuser": false,
      "name": "pg_monitor",
      "password": "********",
      "validUntil": null,
    }
  `
  )
})

withTestDatabase('retrieve, create, update, delete roles', async ({ executeQuery }) => {
  // Create role
  const { sql: createSql } = pgMeta.roles.create({
    name: 'r',
    isSuperuser: true,
    canCreateDb: true,
    canCreateRole: true,
    inheritRole: false,
    canLogin: true,
    isReplicationRole: true,
    canBypassRls: true,
    connectionLimit: 100,
    validUntil: '2020-01-01T00:00:00.000Z',
    config: { search_path: 'extension, public' },
  })
  const rawRes = await executeQuery(createSql)
  let res = pgMeta.roles.zod.parse(rawRes[0])
  expect({ data: res, error: null }).toMatchInlineSnapshot(
    { data: { id: expect.any(Number) } },
    `
    {
      "data": {
        "activeConnections": 0,
        "canBypassRls": true,
        "canCreateDb": true,
        "canCreateRole": true,
        "canLogin": true,
        "config": {
          "search_path": "extension, public",
        },
        "connectionLimit": 100,
        "id": Any<Number>,
        "inheritRole": false,
        "isReplicationRole": true,
        "isSuperuser": true,
        "name": "r",
        "password": "********",
        "validUntil": "2020-01-01 00:00:00+00",
      },
      "error": null,
    }
  `
  )

  // Retrieve role
  const { sql: retrieveSql } = pgMeta.roles.retrieve({ id: res.id })
  const rawRetrieveRes = await executeQuery(retrieveSql)
  res = pgMeta.roles.zod.parse(rawRetrieveRes[0])
  expect({ data: res, error: null }).toMatchInlineSnapshot(
    { data: { id: expect.any(Number) } },
    `
    {
      "data": {
        "activeConnections": 0,
        "canBypassRls": true,
        "canCreateDb": true,
        "canCreateRole": true,
        "canLogin": true,
        "config": {
          "search_path": "extension, public",
        },
        "connectionLimit": 100,
        "id": Any<Number>,
        "inheritRole": false,
        "isReplicationRole": true,
        "isSuperuser": true,
        "name": "r",
        "password": "********",
        "validUntil": "2020-01-01 00:00:00+00",
      },
      "error": null,
    }
  `
  )

  // Remove role
  const { sql: removeSql } = pgMeta.roles.remove({ id: res.id })
  await executeQuery(removeSql)

  // Create a new role for update test
  const { sql: createNewSql } = pgMeta.roles.create({
    name: 'r',
  })
  const rawCreateRes = await executeQuery(createNewSql)
  res = pgMeta.roles.zod.parse(rawCreateRes[0])

  // Update role
  const { sql: updateSql } = pgMeta.roles.update(
    { id: res.id },
    {
      name: 'rr',
      isSuperuser: true,
      canCreateDb: true,
      canCreateRole: true,
      inheritRole: false,
      canLogin: true,
      isReplicationRole: true,
      canBypassRls: true,
      connectionLimit: 100,
      validUntil: '2020-01-01T00:00:00.000Z',
    }
  )
  await executeQuery(updateSql)

  // Create role with config
  const { sql: createConfigSql } = pgMeta.roles.create({
    name: 'rr',
    config: { search_path: 'public', log_statement: 'all' },
  })
  const rawConfigRes = await executeQuery(createConfigSql)
  res = pgMeta.roles.zod.parse(rawConfigRes[0])
  expect({ data: res, error: null }).toMatchInlineSnapshot(
    { data: { id: expect.any(Number) } },
    `
    {
      "data": {
        "activeConnections": 0,
        "canBypassRls": false,
        "canCreateDb": false,
        "canCreateRole": false,
        "canLogin": false,
        "config": {
          "log_statement": "all",
          "search_path": "public",
        },
        "connectionLimit": -1,
        "id": Any<Number>,
        "inheritRole": true,
        "isReplicationRole": false,
        "isSuperuser": false,
        "name": "rr",
        "password": "********",
        "validUntil": null,
      },
      "error": null,
    }
  `
  )

  // Remove role and verify it's gone
  const { sql: finalRemoveSql } = pgMeta.roles.remove({ id: res.id })
  await executeQuery(finalRemoveSql)

  const { sql: finalRetrieveSql } = pgMeta.roles.retrieve({ id: res.id })
  const finalRes = await executeQuery(finalRetrieveSql)
  expect(finalRes).toHaveLength(0)
})
