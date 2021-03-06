import cloneDeep from 'lodash/cloneDeep'
import expect = require('expect')
import {
    StorageMiddleware,
    StorageMiddlewareContext,
} from '@worldbrain/storex/lib/types/middleware'
import StorageManager, {
    CollectionFields,
    IndexDefinition,
    OperationBatch,
} from '@worldbrain/storex'
import { DexieStorageBackend } from '@worldbrain/storex-backend-dexie'
import inMemory from '@worldbrain/storex-backend-dexie/lib/in-memory'
import { ChangeWatchMiddlewareSettings, ChangeWatchMiddleware } from '.'
import { StorageOperationChangeInfo, StorageOperationEvent } from './types'

interface ProcessedTestOperations {
    preprocessed: Array<StorageOperationEvent<'pre'>>
    postprocessed: Array<StorageOperationEvent<'post'>>
}
interface TestSetup {
    storageManager: StorageManager
    changeWatchMiddleware: ChangeWatchMiddleware
    popProcessedOperations: <Key extends keyof ProcessedTestOperations>(
        key: Key,
    ) => ProcessedTestOperations[Key]
    popLoggedOperations: () => any[][]
}

async function setupTest(
    options?: {
        preprocesses?: boolean
        postprocesses?: boolean
        userFields?: CollectionFields
        userIndices?: IndexDefinition[]
        operationWatchers?: ChangeWatchMiddlewareSettings['operationWatchers']
        extraMiddleware?: StorageMiddleware[]
    } & Partial<ChangeWatchMiddlewareSettings>,
): Promise<TestSetup> {
    const backend = new DexieStorageBackend({
        idbImplementation: inMemory(),
        dbName: 'unittest',
    })
    const storageManager = new StorageManager({ backend: backend as any })
    storageManager.registry.registerCollections({
        user: {
            version: new Date('2019-02-19'),
            fields: options?.userFields ?? {
                displayName: { type: 'string' },
            },
            indices: options?.userIndices,
        },
        email: {
            version: new Date('2019-02-19'),
            fields: {
                address: { type: 'string' },
            },
        },
    })
    await storageManager.finishInitialization()

    const operations: ProcessedTestOperations = {
        preprocessed: [],
        postprocessed: [],
    }
    const changeWatchMiddleware = new ChangeWatchMiddleware({
        storageManager,
        shouldWatchCollection: options?.shouldWatchCollection ?? (() => true),
        operationWatchers: options?.operationWatchers,
        getCollectionDefinition: collection =>
            storageManager.registry.collections[collection],
        preprocessOperation:
            options?.preprocesses ?? true
                ? event => {
                      operations.preprocessed.push(event)
                  }
                : undefined,
        postprocessOperation:
            options?.postprocesses ?? true
                ? event => {
                      operations.postprocessed.push(event)
                  }
                : undefined,
    })

    const loggedOperations: any[][] = []
    const operationLoggingMiddleware: StorageMiddleware = {
        process: async context => {
            loggedOperations.push(cloneDeep(context.operation))
            return context.next.process({ operation: context.operation })
        },
    }

    storageManager.setMiddleware([
        changeWatchMiddleware,
        operationLoggingMiddleware,
        ...(options?.extraMiddleware ?? []),
    ])
    return {
        storageManager,
        changeWatchMiddleware,
        popProcessedOperations: type => {
            const preprocessed = operations[type]
            operations[type] = []
            return preprocessed
        },
        popLoggedOperations: () => {
            const copy = [...loggedOperations]
            loggedOperations.splice(0)
            return copy
        },
    }
}

async function executeTestCreate(
    storageManager: StorageManager,
    options?: { id?: number | string; objectValues?: any },
) {
    const objectValues = options?.objectValues ?? { displayName: 'John Doe' }
    const { object } = await storageManager
        .collection('user')
        .createObject({ ...objectValues, id: options?.id })

    return { object, objectValues }
}

async function verifiyTestCreate(
    storageManager: StorageManager,
    options: { object: any; objectValues: any },
) {
    const objects = await storageManager.collection('user').findObjects({})
    expect(objects).toEqual([
        { id: options.object.id, ...options.objectValues },
    ])
}

async function testCreateWithoutLogging(
    setup: Pick<TestSetup, 'storageManager' | 'popProcessedOperations'>,
) {
    const creation = await executeTestCreate(setup.storageManager)
    expect(setup.popProcessedOperations('preprocessed')).toEqual([])
    expect(setup.popProcessedOperations('postprocessed')).toEqual([])
    await verifiyTestCreate(setup.storageManager, creation)
}

async function insertTestObjects(
    setup: Pick<TestSetup, 'storageManager' | 'popProcessedOperations'>,
    options?: { compoundPk?: boolean },
) {
    const { object: object1 } = await setup.storageManager
        .collection('user')
        .createObject(
            options?.compoundPk
                ? { first: 'Joe', last: 'Doe', foo: 'Bla' }
                : { displayName: 'Joe' },
        )
    const { object: object2 } = await setup.storageManager
        .collection('user')
        .createObject(
            options?.compoundPk
                ? { first: 'Bob', last: 'Doe', foo: 'Bla' }
                : { displayName: 'Bob' },
        )

    setup.popProcessedOperations('preprocessed')
    setup.popProcessedOperations('postprocessed')

    return { object1, object2 }
}

function expectPreProcessedOperations(
    setup: Pick<TestSetup, 'popProcessedOperations'>,
    expected: ProcessedTestOperations['preprocessed'],
) {
    expect(setup.popProcessedOperations('preprocessed')).toEqual(expected)
}

function expectPostProcessedOperations(
    setup: Pick<TestSetup, 'popProcessedOperations'>,
    expected: ProcessedTestOperations['postprocessed'],
) {
    expect(setup.popProcessedOperations('postprocessed')).toEqual(expected)
}

function expectProcessedOperations(
    setup: Pick<TestSetup, 'popProcessedOperations'>,
    expected: ProcessedTestOperations,
) {
    expect({
        preprocessed: setup.popProcessedOperations('preprocessed'),
        postprocessed: setup.popProcessedOperations('postprocessed'),
    }).toEqual(expected)
}

describe('ChangeWatchMiddleware', () => {
    it('should correctly report creations with auto-generated IDs', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const creation = await executeTestCreate(storageManager)

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'create',
                    collection: 'user',
                    values: creation.objectValues,
                },
            ],
        }
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'createObject',
                    'user',
                    creation.objectValues,
                ],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'create',
                    collection: 'user',
                    pk: creation.object.id,
                    values: creation.objectValues,
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'createObject',
                    'user',
                    creation.objectValues,
                ],
                info: expectedPostInfo,
            },
        ])

        await verifiyTestCreate(storageManager, creation)
    })

    it('should correctly report creations with manual IDs', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const creation = await executeTestCreate(storageManager, { id: 5 })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'create',
                    collection: 'user',
                    pk: 5,
                    values: { ...creation.objectValues },
                },
            ],
        }
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'createObject',
                    'user',
                    { ...creation.objectValues, id: 5 },
                ],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'create',
                    collection: 'user',
                    pk: creation.object.id,
                    values: { ...creation.objectValues },
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'createObject',
                    'user',
                    { ...creation.objectValues, id: 5 },
                ],
                info: expectedPostInfo,
            },
        ])

        await verifiyTestCreate(storageManager, creation)
    })

    it('should correctly report creations with a compound primary key', async () => {
        const { storageManager, popProcessedOperations } = await setupTest({
            userFields: {
                first: { type: 'string' },
                last: { type: 'string' },
                foo: { type: 'string' },
            },
            userIndices: [
                { field: ['first', 'last'], pk: true },
                { field: 'last' },
            ],
        })
        const creation = await executeTestCreate(storageManager, {
            objectValues: { first: 'Bob', last: 'Doe', foo: 'Bla' },
        })
        expectProcessedOperations(
            { popProcessedOperations },
            {
                preprocessed: [
                    {
                        originalOperation: [
                            'createObject',
                            'user',
                            { ...creation.objectValues },
                        ],
                        info: {
                            changes: [
                                {
                                    type: 'create',
                                    collection: 'user',
                                    pk: ['Bob', 'Doe'],
                                    values: { foo: 'Bla' },
                                },
                            ],
                        },
                    },
                ],
                postprocessed: [
                    {
                        originalOperation: [
                            'createObject',
                            'user',
                            { ...creation.objectValues },
                        ],
                        info: {
                            changes: [
                                {
                                    type: 'create',
                                    collection: 'user',
                                    pk: ['Bob', 'Doe'],
                                    values: { foo: 'Bla' },
                                },
                            ],
                        },
                    },
                ],
            },
        )
        await verifiyTestCreate(storageManager, creation)
    })

    it('should correctly report modifications by updateObject filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        await storageManager.operation(
            'updateObject',
            'user',
            { id: object1.id },
            { displayName: 'Jon' },
        )

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'modify',
                    collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id],
                },
            ],
        }
        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'updateObjects',
                placeholder: 'change-0',
                updates: {
                    displayName: 'Jon',
                },
                where: { id: { $in: [object1.id] } },
            },
        ]
        const expectedPreprocessedOperations: ProcessedTestOperations['preprocessed'] = [
            {
                originalOperation: [
                    'updateObject',
                    'user',
                    { id: object1.id },
                    { displayName: 'Jon' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ]
        expectPreProcessedOperations(
            { popProcessedOperations },
            expectedPreprocessedOperations,
        )
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'modify',
                    collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'updateObject',
                    'user',
                    { id: object1.id },
                    { displayName: 'Jon' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual(
            [
                { id: object1.id, displayName: 'Jon' },
                { id: object2.id, displayName: 'Bob' },
            ],
        )
    })

    it('should correctly report updateObject by compound primary key', async () => {
        const { storageManager, popProcessedOperations } = await setupTest({
            userFields: {
                first: { type: 'string' },
                last: { type: 'string' },
                foo: { type: 'string' },
            },
            userIndices: [
                { field: ['first', 'last'], pk: true },
                { field: 'last' },
            ],
        })
        const { object1, object2 } = await insertTestObjects(
            { storageManager, popProcessedOperations },
            { compoundPk: true },
        )

        const where = {
            first: object1.first,
            last: object1.last,
        }
        const updates = { foo: 'Green' }
        await storageManager.operation(
            'updateObject',
            'user',
            { ...where },
            { ...updates },
        )

        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'updateObjects',
                placeholder: 'change-0',
                where,
                updates,
            },
        ]
        expectProcessedOperations(
            { popProcessedOperations },
            {
                preprocessed: [
                    {
                        originalOperation: [
                            'updateObject',
                            'user',
                            where,
                            updates,
                        ],
                        modifiedOperation: ['executeBatch', batch],
                        info: {
                            changes: [
                                {
                                    type: 'modify',
                                    collection: 'user',
                                    pks: [['Joe', 'Doe']],
                                    where,
                                    updates,
                                },
                            ],
                        },
                    },
                ],
                postprocessed: [
                    {
                        originalOperation: [
                            'updateObject',
                            'user',
                            where,
                            updates,
                        ],
                        modifiedOperation: ['executeBatch', batch],
                        info: {
                            changes: [
                                {
                                    type: 'modify',
                                    collection: 'user',
                                    pks: [['Joe', 'Doe']],
                                    where,
                                    updates,
                                },
                            ],
                        },
                    },
                ],
            },
        )
    })

    it('should correctly report modifications by updateObjects filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        await storageManager.operation(
            'updateObjects',
            'user',
            { id: object1.id },
            { displayName: 'Jon' },
        )

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'modify',
                    collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id],
                },
            ],
        }
        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'updateObjects',
                placeholder: 'change-0',
                updates: {
                    displayName: 'Jon',
                },
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'updateObjects',
                    'user',
                    { id: object1.id },
                    { displayName: 'Jon' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'modify',
                    collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'updateObjects',
                    'user',
                    { id: object1.id },
                    { displayName: 'Jon' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual(
            [
                { id: object1.id, displayName: 'Jon' },
                { id: object2.id, displayName: 'Bob' },
            ],
        )
    })

    it('should correctly report modifications by updateObjects filtered by other fields', async () => {
        const { storageManager, popProcessedOperations } = await setupTest({
            userIndices: [],
        })
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        await storageManager.operation(
            'updateObjects',
            'user',
            { displayName: 'Joe' },
            { displayName: 'Jon' },
        )

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'modify',
                    collection: 'user',
                    where: { displayName: 'Joe' },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id],
                },
            ],
        }
        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'updateObjects',
                placeholder: 'change-0',
                updates: {
                    displayName: 'Jon',
                },
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'updateObjects',
                    'user',
                    { displayName: 'Joe' },
                    { displayName: 'Jon' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'modify',
                    collection: 'user',
                    where: { displayName: 'Joe' },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'updateObjects',
                    'user',
                    { displayName: 'Joe' },
                    { displayName: 'Jon' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual(
            [
                { id: object1.id, displayName: 'Jon' },
                { id: object2.id, displayName: 'Bob' },
            ],
        )
    })

    it('should correctly report updateObjects for collections with compound primary key', async () => {
        const { storageManager, popProcessedOperations } = await setupTest({
            userFields: {
                first: { type: 'string' },
                last: { type: 'string' },
                foo: { type: 'string' },
            },
            userIndices: [
                { field: ['first', 'last'], pk: true },
                { field: 'last' },
            ],
        })
        const { object1, object2 } = await insertTestObjects(
            { storageManager, popProcessedOperations },
            { compoundPk: true },
        )

        const where = {
            last: object1.last,
        }
        const updates = { foo: 'Green' }
        await storageManager.operation(
            'updateObjects',
            'user',
            { ...where },
            { ...updates },
        )

        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'updateObjects',
                placeholder: 'change-0',
                where: { ...where, first: 'Bob' },
                updates,
            },
            {
                collection: 'user',
                operation: 'updateObjects',
                placeholder: 'change-1',
                where: { ...where, first: 'Joe' },
                updates,
            },
        ]
        expectProcessedOperations(
            { popProcessedOperations },
            {
                preprocessed: [
                    {
                        originalOperation: [
                            'updateObjects',
                            'user',
                            where,
                            updates,
                        ],
                        modifiedOperation: ['executeBatch', batch],
                        info: {
                            changes: [
                                {
                                    type: 'modify',
                                    collection: 'user',
                                    pks: [
                                        ['Bob', 'Doe'],
                                        ['Joe', 'Doe'],
                                    ],
                                    where,
                                    updates,
                                },
                            ],
                        },
                    },
                ],
                postprocessed: [
                    {
                        originalOperation: [
                            'updateObjects',
                            'user',
                            where,
                            updates,
                        ],
                        modifiedOperation: ['executeBatch', batch],
                        info: {
                            changes: [
                                {
                                    type: 'modify',
                                    collection: 'user',
                                    pks: [
                                        ['Bob', 'Doe'],
                                        ['Joe', 'Doe'],
                                    ],
                                    where,
                                    updates,
                                },
                            ],
                        },
                    },
                ],
            },
        )
    })

    it('should correctly report deletions by deleteObject filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        await storageManager.operation('deleteObject', 'user', {
            id: object1.id,
        })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'delete',
                    collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id],
                },
            ],
        }
        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'deleteObjects',
                placeholder: 'change-0',
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObject', 'user', { id: object1.id }],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'delete',
                    collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObject', 'user', { id: object1.id }],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])

        expect(
            await storageManager.collection('user').findObjects({}),
        ).toEqual([{ id: object2.id, displayName: 'Bob' }])
    })

    it('should correctly report deletions by deleteObjects filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        await storageManager.operation('deleteObjects', 'user', {
            id: object1.id,
        })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'delete',
                    collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id],
                },
            ],
        }
        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'deleteObjects',
                placeholder: 'change-0',
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'deleteObjects',
                    'user',
                    { id: object1.id },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'delete',
                    collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'deleteObjects',
                    'user',
                    { id: object1.id },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])

        expect(
            await storageManager.collection('user').findObjects({}),
        ).toEqual([{ id: object2.id, displayName: 'Bob' }])
    })

    it('should correctly report deletions by deleteObjects filtered by other fields', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        await storageManager.operation('deleteObjects', 'user', {
            displayName: 'Joe',
        })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'delete',
                    collection: 'user',
                    where: { displayName: 'Joe' },
                    pks: [object1.id],
                },
            ],
        }
        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'deleteObjects',
                placeholder: 'change-0',
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'deleteObjects',
                    'user',
                    { displayName: 'Joe' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'delete',
                    collection: 'user',
                    where: { displayName: 'Joe' },
                    pks: [object1.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: [
                    'deleteObjects',
                    'user',
                    { displayName: 'Joe' },
                ],
                modifiedOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])

        expect(
            await storageManager.collection('user').findObjects({}),
        ).toEqual([{ id: object2.id, displayName: 'Bob' }])
    })

    it('should correctly report changes through batch operations', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })

        const batch = [
            {
                placeholder: 'jane',
                operation: 'createObject',
                collection: 'user',
                args: {
                    displayName: 'Jane',
                },
            },
            {
                operation: 'updateObjects',
                collection: 'user',
                where: { id: object1.id },
                updates: { displayName: 'Jack' },
            },
            {
                operation: 'deleteObjects',
                collection: 'user',
                where: { id: object2.id },
            },
        ]
        const batchResult = await storageManager.operation(
            'executeBatch',
            cloneDeep(batch),
        )

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'create',
                    collection: 'user',
                    values: { displayName: 'Jane' },
                },
                {
                    type: 'modify',
                    collection: 'user',
                    where: batch[1].where!,
                    updates: batch[1].updates!,
                    pks: [object1.id],
                },
                {
                    type: 'delete',
                    collection: 'user',
                    where: batch[2].where!,
                    pks: [object2.id],
                },
            ],
        }
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['executeBatch', batch],
                info: expectedPreInfo,
            },
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'create',
                    collection: 'user',
                    pk: batchResult.info.jane.object.id,
                    values: { displayName: 'Jane' },
                },
                {
                    type: 'modify',
                    collection: 'user',
                    where: batch[1].where!,
                    updates: batch[1].updates!,
                    pks: [object1.id],
                },
                {
                    type: 'delete',
                    collection: 'user',
                    where: batch[2].where!,
                    pks: [object2.id],
                },
            ],
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['executeBatch', batch],
                info: expectedPostInfo,
            },
        ])
        expect(await storageManager.collection('user').findObjects({})).toEqual(
            [
                { id: object1.id, displayName: 'Jack' },
                { id: batchResult.info.jane.object.id, displayName: 'Jane' },
            ],
        )
    })

    it('should correctly report deleteObjects for collections with compound primary key', async () => {
        const { storageManager, popProcessedOperations } = await setupTest({
            userFields: {
                first: { type: 'string' },
                last: { type: 'string' },
                foo: { type: 'string' },
            },
            userIndices: [
                { field: ['first', 'last'], pk: true },
                { field: 'last' },
            ],
        })
        const { object1, object2 } = await insertTestObjects(
            { storageManager, popProcessedOperations },
            { compoundPk: true },
        )

        const where = {
            last: object1.last,
        }
        await storageManager.operation('deleteObjects', 'user', { ...where })

        const batch: OperationBatch = [
            {
                collection: 'user',
                operation: 'deleteObjects',
                placeholder: 'change-0',
                where: { ...where, first: 'Bob' },
            },
            {
                collection: 'user',
                operation: 'deleteObjects',
                placeholder: 'change-1',
                where: { ...where, first: 'Joe' },
            },
        ]
        expectProcessedOperations(
            { popProcessedOperations },
            {
                preprocessed: [
                    {
                        originalOperation: ['deleteObjects', 'user', where],
                        modifiedOperation: ['executeBatch', batch],
                        info: {
                            changes: [
                                {
                                    type: 'delete',
                                    collection: 'user',
                                    pks: [
                                        ['Bob', 'Doe'],
                                        ['Joe', 'Doe'],
                                    ],
                                    where,
                                },
                            ],
                        },
                    },
                ],
                postprocessed: [
                    {
                        originalOperation: ['deleteObjects', 'user', where],
                        modifiedOperation: ['executeBatch', batch],
                        info: {
                            changes: [
                                {
                                    type: 'delete',
                                    collection: 'user',
                                    pks: [
                                        ['Bob', 'Doe'],
                                        ['Joe', 'Doe'],
                                    ],
                                    where,
                                },
                            ],
                        },
                    },
                ],
            },
        )
    })

    it('should let operations through if not enabled', async () => {
        const setup = await setupTest()
        setup.changeWatchMiddleware.enabled = false

        await testCreateWithoutLogging(setup)
    })

    it('should let operations through for which there are no watchers', async () => {
        const setup = await setupTest({
            operationWatchers: {},
        })

        await testCreateWithoutLogging(setup)
    })

    it('should let operations through if not passed a preprocessor', async () => {
        const setup = await setupTest({
            preprocesses: false,
            postprocesses: false,
        })

        await testCreateWithoutLogging(setup)
    })

    it('should not touch operations in batches for collections it is not enabled for', async () => {
        const setup = await setupTest({
            shouldWatchCollection: collectionName => collectionName === 'user',
        })
        await insertTestObjects(setup)
        await setup.storageManager.operation('createObject', 'email', {
            address: 'bla@spam.com',
        })

        const batch: OperationBatch = [
            {
                placeholder: 'test-1',
                operation: 'createObject',
                collection: 'user',
                args: {
                    displayName: 'Spam eggs',
                },
            },
            {
                placeholder: 'test-2',
                operation: 'updateObjects',
                collection: 'email',
                where: {
                    address: 'bla@spam.com',
                },
                updates: {
                    address: 'bla@foo.com',
                },
            },
        ]
        await setup.storageManager.operation('executeBatch', cloneDeep(batch))
        expectProcessedOperations(setup, {
            preprocessed: [
                {
                    originalOperation: ['executeBatch', batch],
                    info: {
                        changes: [
                            expect.objectContaining({
                                type: 'create',
                                collection: 'user',
                            }) as any,
                        ],
                    },
                },
            ],
            postprocessed: [
                {
                    originalOperation: ['executeBatch', batch],
                    info: {
                        changes: [
                            expect.objectContaining({
                                type: 'create',
                                collection: 'user',
                            }) as any,
                        ],
                    },
                },
            ],
        })
        expect(
            await setup.storageManager.operation('findObjects', 'email', {}),
        ).toEqual([{ id: expect.anything(), address: 'bla@foo.com' }])
    })

    it('should correctly pass down change information to next middleware', async () => {
        const calls: Array<Pick<StorageMiddlewareContext, 'extraData'>> = []
        const { storageManager, popProcessedOperations } = await setupTest({
            extraMiddleware: [
                {
                    process: context => {
                        calls.push({ extraData: context.extraData })
                        return context.next.process({
                            operation: context.operation,
                        })
                    },
                },
            ],
        })

        const { object1 } = await insertTestObjects({
            storageManager,
            popProcessedOperations,
        })
        await storageManager.operation('deleteObjects', 'user', {
            displayName: 'Joe',
        })
        const expected: Array<{
            extraData: { changeInfo?: StorageOperationChangeInfo<'pre'> }
        }> = [
            {
                extraData: {
                    changeInfo: {
                        changes: [
                            {
                                type: 'create',
                                collection: 'user',
                                values: {
                                    displayName: 'Joe',
                                },
                            },
                        ],
                    },
                },
            },
            {
                extraData: {
                    changeInfo: {
                        changes: [
                            {
                                type: 'create',
                                collection: 'user',
                                values: {
                                    displayName: 'Bob',
                                },
                            },
                        ],
                    },
                },
            },
            {
                extraData: {
                    changeInfo: {
                        changes: [],
                    },
                },
            },
            {
                extraData: {
                    changeInfo: {
                        changes: [
                            {
                                type: 'delete',
                                collection: 'user',
                                where: { displayName: 'Joe' },
                                pks: [object1.id],
                            },
                        ],
                    },
                },
            },
        ]
        expect(calls).toEqual(expected)
    })
})
