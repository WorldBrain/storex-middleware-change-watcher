import cloneDeep from 'lodash/cloneDeep'
import expect = require("expect")
import { StorageMiddleware, StorageMiddlewareContext } from '@worldbrain/storex/lib/types/middleware'
import StorageManager, { CollectionFields, IndexDefinition, OperationBatch } from "@worldbrain/storex"
import { DexieStorageBackend } from "@worldbrain/storex-backend-dexie"
import inMemory from "@worldbrain/storex-backend-dexie/lib/in-memory"
import { ChangeWatchMiddlewareSettings, ChangeWatchMiddleware } from "."
import { StorageOperationChangeInfo, StorageOperationEvent } from "./types"

interface ProcessedTestOperations {
    preproccessed: Array<StorageOperationEvent<'pre'>>
    postproccessed: Array<StorageOperationEvent<'post'>>
}
interface TestSetup {
    storageManager: StorageManager
    changeWatchMiddleware: ChangeWatchMiddleware
    popProcessedOperations: <Key extends keyof ProcessedTestOperations>(key: Key) => ProcessedTestOperations[Key]
}

async function setupTest(options?: {
    preprocesses?: boolean
    postprocesses?: boolean
    userFields?: CollectionFields
    userIndices?: IndexDefinition[]
    operationWatchers?: ChangeWatchMiddlewareSettings['operationWatchers']
    extraMiddleware?: StorageMiddleware[]
} & Partial<ChangeWatchMiddlewareSettings>): Promise<TestSetup> {
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
    })
    await storageManager.finishInitialization()

    const operations: ProcessedTestOperations = { preproccessed: [], postproccessed: [] }
    const changeWatchMiddleware = new ChangeWatchMiddleware({
        storageManager,
        shouldWatchCollection: options?.shouldWatchCollection ?? (() => true),
        operationWatchers: options?.operationWatchers,
        getCollectionDefinition: (collection) => storageManager.registry.collections[collection],
        preprocessOperation: (options?.preprocesses ?? true) ? (event => {
            operations.preproccessed.push(event)
        }) : undefined,
        postprocessOperation: (options?.postprocesses ?? true) ? (event => {
            operations.postproccessed.push(event)
        }) : undefined
    })
    storageManager.setMiddleware([changeWatchMiddleware, ...(options?.extraMiddleware ?? [])])
    return {
        storageManager,
        changeWatchMiddleware,
        popProcessedOperations: (type) => {
            const preprocessed = operations[type]
            operations[type] = []
            return preprocessed
        }
    }
}

async function executeTestCreate(storageManager: StorageManager, options?: { id?: number | string }) {
    const objectValues = { displayName: 'John Doe' }
    const { object } = await storageManager
        .collection('user')
        .createObject({ ...objectValues, id: options?.id })

    return { object, objectValues }
}

async function verifiyTestCreate(storageManager: StorageManager, options: { object: any, objectValues: any }) {
    const objects = await storageManager.collection('user').findObjects({})
    expect(objects).toEqual([
        { id: options.object.id, ...options.objectValues }
    ])
}

async function testCreateWithoutLogging(setup: Pick<TestSetup, 'storageManager' | 'popProcessedOperations'>) {
    const creation = await executeTestCreate(setup.storageManager)
    expect(setup.popProcessedOperations('preproccessed')).toEqual([])
    expect(setup.popProcessedOperations('postproccessed')).toEqual([])
    await verifiyTestCreate(setup.storageManager, creation)
}

async function insertTestObjects(setup: Pick<TestSetup, 'storageManager' | 'popProcessedOperations'>) {
    const { object: object1 } = await setup.storageManager
        .collection('user')
        .createObject({ displayName: 'Joe' })
    const { object: object2 } = await setup.storageManager
        .collection('user')
        .createObject({ displayName: 'Bob' })

    setup.popProcessedOperations('preproccessed')
    setup.popProcessedOperations('postproccessed')

    return { object1, object2 }
}

function expectPreProcessedOperations(setup: Pick<TestSetup, 'popProcessedOperations'>, expected: ProcessedTestOperations['preproccessed']) {
    expect(setup.popProcessedOperations('preproccessed')).toEqual(expected)
}

function expectPostProcessedOperations(setup: Pick<TestSetup, 'popProcessedOperations'>, expected: ProcessedTestOperations['postproccessed']) {
    expect(setup.popProcessedOperations('postproccessed')).toEqual(expected)
}

describe('ChangeWatchMiddleware', () => {
    it('should correctly report creations with auto-generated IDs', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const creation = await executeTestCreate(storageManager)

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                { type: 'create', collection: 'user', values: creation.objectValues }
            ]
        }
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['createObject', 'user', creation.objectValues],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                { type: 'create', collection: 'user', pk: creation.object.id, values: creation.objectValues }
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['createObject', 'user', creation.objectValues],
                info: expectedPostInfo
            }
        ])

        await verifiyTestCreate(storageManager, creation)
    })

    it('should correctly report creations with manual IDs', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const creation = await executeTestCreate(storageManager, { id: 5 })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                { type: 'create', collection: 'user', pk: 5, values: { ...creation.objectValues, id: 5 } }
            ]
        }
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['createObject', 'user', { ...creation.objectValues, id: 5 }],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                { type: 'create', collection: 'user', pk: creation.object.id, values: { ...creation.objectValues, id: 5 } }
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['createObject', 'user', { ...creation.objectValues, id: 5 }],
                info: expectedPostInfo
            }
        ])

        await verifiyTestCreate(storageManager, creation)
    })

    it('should correctly report modifications by updateObject filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        await storageManager.operation('updateObject', 'user', { id: object1.id }, { displayName: 'Jon' })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'modify', collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id]
                },
            ]
        }
        const batch: OperationBatch = [
            {
                collection: "user",
                operation: "updateObjects",
                placeholder: "change-0",
                updates: {
                    displayName: "Jon",
                },
                where: { id: { $in: [object1.id] } },
            },
        ]
        const expectedPreprocessedOperations: ProcessedTestOperations['preproccessed'] = [
            {
                originalOperation: ['updateObject', 'user', { id: object1.id }, { displayName: 'Jon' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPreInfo
            }
        ]
        expectPreProcessedOperations({ popProcessedOperations }, expectedPreprocessedOperations)
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'modify', collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id]
                },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['updateObject', 'user', { id: object1.id }, { displayName: 'Jon' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPostInfo
            }
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object1.id, displayName: 'Jon' },
            { id: object2.id, displayName: 'Bob' },
        ])
    })

    it('should correctly report modifications by updateObjects filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        await storageManager.operation('updateObjects', 'user', { id: object1.id }, { displayName: 'Jon' })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'modify', collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id]
                },
            ]
        }
        const batch: OperationBatch = [
            {
                collection: "user",
                operation: "updateObjects",
                placeholder: "change-0",
                updates: {
                    displayName: "Jon",
                },
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['updateObjects', 'user', { id: object1.id }, { displayName: 'Jon' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'modify', collection: 'user',
                    where: { id: object1.id },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id]
                },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['updateObjects', 'user', { id: object1.id }, { displayName: 'Jon' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPostInfo
            }
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object1.id, displayName: 'Jon' },
            { id: object2.id, displayName: 'Bob' },
        ])
    })

    it('should correctly report modifications by updateObjects filtered by other fields', async () => {
        const { storageManager, popProcessedOperations } = await setupTest({ userIndices: [] })
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        await storageManager.operation('updateObjects', 'user', { displayName: 'Joe' }, { displayName: 'Jon' })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'modify', collection: 'user',
                    where: { displayName: 'Joe' },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id]
                },
            ]
        }
        const batch: OperationBatch = [
            {
                collection: "user",
                operation: "updateObjects",
                placeholder: "change-0",
                updates: {
                    displayName: "Jon",
                },
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['updateObjects', 'user', { displayName: 'Joe' }, { displayName: 'Jon' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'modify', collection: 'user',
                    where: { displayName: 'Joe' },
                    updates: { displayName: 'Jon' },
                    pks: [object1.id]
                },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['updateObjects', 'user', { displayName: 'Joe' }, { displayName: 'Jon' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPostInfo
            }
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object1.id, displayName: 'Jon' },
            { id: object2.id, displayName: 'Bob' },
        ])
    })

    it('should correctly report deletions by deleteObject filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        await storageManager.operation('deleteObject', 'user', { id: object1.id })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'delete', collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id]
                },
            ]
        }
        const batch: OperationBatch = [
            {
                collection: "user",
                operation: "deleteObjects",
                placeholder: "change-0",
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObject', 'user', { id: object1.id }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'delete', collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id]
                },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObject', 'user', { id: object1.id }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPostInfo
            }
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object2.id, displayName: 'Bob' },
        ])
    })

    it('should correctly report deletions by deleteObjects filtered by PK', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        await storageManager.operation('deleteObjects', 'user', { id: object1.id })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'delete', collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id]
                },
            ]
        }
        const batch: OperationBatch = [
            {
                collection: "user",
                operation: "deleteObjects",
                placeholder: "change-0",
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObjects', 'user', { id: object1.id }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'delete', collection: 'user',
                    where: { id: object1.id },
                    pks: [object1.id]
                },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObjects', 'user', { id: object1.id }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPostInfo
            }
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object2.id, displayName: 'Bob' },
        ])
    })

    it('should correctly report deletions by deleteObjects filtered by other fields', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        await storageManager.operation('deleteObjects', 'user', { displayName: 'Joe' })

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                {
                    type: 'delete', collection: 'user',
                    where: { displayName: 'Joe' },
                    pks: [object1.id]
                },
            ]
        }
        const batch: OperationBatch = [
            {
                collection: "user",
                operation: "deleteObjects",
                placeholder: "change-0",
                where: { id: { $in: [object1.id] } },
            },
        ]
        expectPreProcessedOperations({ popProcessedOperations }, [
            {

                originalOperation: ['deleteObjects', 'user', { displayName: 'Joe' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                {
                    type: 'delete', collection: 'user',
                    where: { displayName: 'Joe' },
                    pks: [object1.id]
                },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['deleteObjects', 'user', { displayName: 'Joe' }],
                modifiedOperation: ["executeBatch", batch],
                info: expectedPostInfo
            }
        ])

        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object2.id, displayName: 'Bob' },
        ])
    })

    it('should correctly report changes through batch operations', async () => {
        const { storageManager, popProcessedOperations } = await setupTest()
        const { object1, object2 } = await insertTestObjects({ storageManager, popProcessedOperations })

        const batch = [
            {
                placeholder: 'jane',
                operation: 'createObject',
                collection: 'user',
                args: {
                    displayName: 'Jane'
                }
            },
            { operation: 'updateObjects', collection: 'user', where: { id: object1.id }, updates: { displayName: 'Jack' } },
            { operation: 'deleteObjects', collection: 'user', where: { id: object2.id } },
        ]
        const batchResult = await storageManager.operation('executeBatch', cloneDeep(batch))

        const expectedPreInfo: StorageOperationChangeInfo<'pre'> = {
            changes: [
                { type: 'create', collection: 'user', values: { displayName: 'Jane' } },
                { type: 'modify', collection: 'user', where: batch[1].where!, updates: batch[1].updates!, pks: [object1.id] },
                { type: 'delete', collection: 'user', where: batch[2].where!, pks: [object2.id] },
            ]
        }
        expectPreProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['executeBatch', batch],
                info: expectedPreInfo
            }
        ])
        const expectedPostInfo: StorageOperationChangeInfo<'post'> = {
            changes: [
                { type: 'create', collection: 'user', pk: batchResult.info.jane.object.id, values: { displayName: 'Jane' } },
                { type: 'modify', collection: 'user', where: batch[1].where!, updates: batch[1].updates!, pks: [object1.id] },
                { type: 'delete', collection: 'user', where: batch[2].where!, pks: [object2.id] },
            ]
        }
        expectPostProcessedOperations({ popProcessedOperations }, [
            {
                originalOperation: ['executeBatch', batch],
                info: expectedPostInfo
            }
        ])
        expect(await storageManager.collection('user').findObjects({})).toEqual([
            { id: object1.id, displayName: 'Jack' },
            { id: batchResult.info.jane.object.id, displayName: 'Jane' }
        ])
    })

    it('should let operations through if not enabled', async () => {
        const setup = await setupTest()
        setup.changeWatchMiddleware.enabled = false

        await testCreateWithoutLogging(setup)
    })

    it('should let operations through for which there are no watchers', async () => {
        const setup = await setupTest({
            operationWatchers: {}
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

    it('should correctly pass down change information to next middleware', async () => {
        const calls: Array<Pick<StorageMiddlewareContext, 'extraData'>> = []
        const { storageManager, popProcessedOperations } = await setupTest({
            extraMiddleware: [{
                process: context => {
                    calls.push({ extraData: context.extraData })
                    return context.next.process({ operation: context.operation })
                }
            }]
        })

        const { object1 } = await insertTestObjects({ storageManager, popProcessedOperations })
        await storageManager.operation('deleteObjects', 'user', { displayName: 'Joe' })
        const expected: Array<{ extraData: { changeInfo?: StorageOperationChangeInfo<'pre'> } }> = [
            {
                extraData: {
                    changeInfo: {
                        changes: [
                            {
                                type: 'create',
                                collection: 'user',
                                values: {
                                    displayName: 'Joe',
                                }
                            }
                        ]
                    }
                }
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
                                }
                            }
                        ]
                    }
                }
            },
            {
                extraData: {
                    changeInfo: {
                        changes: []
                    }
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
                                pks: [object1.id]
                            }
                        ]
                    }
                }
            },
        ]
        expect(calls).toEqual(expected)
    })
})
