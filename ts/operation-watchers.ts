import every from 'lodash/every'
import StorageManager, { OperationBatch, CollectionDefinition } from "@worldbrain/storex";
import { StorageOperationWatcher, ModificationStorageChange, DeletionStorageChange, CreationStorageChange, StorageChange, StorageOperationChangeInfo, StorageChangePk } from "./types";
import { getObjectPk, getObjectWithoutPk } from "@worldbrain/storex/lib/utils";

const createObject: StorageOperationWatcher = {
    getInfoBeforeExecution(context) {
        const { operation } = context
        const [_, collection, values] = operation
        const pk = getObjectPk(values, collection, context.storageManager.registry)
        const hasPk = !!pk && (pk instanceof Array ? every(pk) : true)
        const change: CreationStorageChange<'pre'> = {
            type: 'create',
            collection,
            values: getObjectWithoutPk(values, collection, context.storageManager.registry),
            ...(hasPk ? { pk } : {})
        }
        return {
            changes: [change]
        }
    },
    getInfoAfterExecution(context) {
        const { operation } = context
        const [_, collection] = operation
        const change: CreationStorageChange<'post'> = {
            type: 'create',
            collection: operation[1],
            pk: getObjectPk(context.result.object, collection, context.storageManager.registry),
            values: getObjectWithoutPk(operation[2], collection, context.storageManager.registry),
        }
        return {
            changes: [change]
        }
    },
}

const updateObject: StorageOperationWatcher = {
    async getInfoBeforeExecution(context) {
        const { operation } = context
        const collection = operation[1]
        const affectedObjects: any[] = await _findObjectsInvolvedInFilteredOperation(operation, context.storageManager)
        const change: ModificationStorageChange<'pre'> = {
            type: 'modify',
            collection: operation[1],
            where: operation[2],
            updates: operation[3],
            pks: affectedObjects.map(object => getObjectPk(object, collection, context.storageManager.registry)),
        }
        return {
            changes: [
                change
            ]
        }
    },
    async transformOperation(context) {
        const batch: OperationBatch = []
        let index = -1
        for (const change of context.info.changes) {
            if (change.type !== 'modify') {
                throw new Error('Something weird happened in updateObject change watcher during operation transform')
            }

            const collectionDefinition = context.storageManager.registry.collections[change.collection]

            if (typeof collectionDefinition.pkIndex === 'string') {
                index += 1

                batch.push({
                    placeholder: `change-${index}`,
                    operation: 'updateObjects',
                    collection: change.collection,
                    where: { [collectionDefinition.pkIndex]: { $in: change.pks } },
                    updates: change.updates
                })
            } else {
                for (const pk of change.pks) {
                    index += 1

                    batch.push({
                        placeholder: `change-${index}`,
                        operation: 'updateObjects',
                        collection: change.collection,
                        where: _getChangeWhere(pk, collectionDefinition),
                        updates: change.updates
                    })
                }
            }
        }

        return ['executeBatch', batch]
    },
    async getInfoAfterExecution(context) {
        const { operation } = context
        const collection = operation[1]

        const preInfoChange = context.preInfo.changes[0]
        if (preInfoChange.type !== 'modify') {
            throw new Error('Something weird happened in updateObject change watcher')
        }
        const change: ModificationStorageChange<'post'> = {
            type: 'modify',
            collection,
            where: operation[2],
            updates: operation[3],
            pks: preInfoChange.pks,
        };
        return {
            changes: [
                change
            ]
        }
    },
}

const deleteObject: StorageOperationWatcher = {
    async getInfoBeforeExecution(context) {
        const { operation } = context
        const collection = operation[1]
        const affectedObjects: any[] = await _findObjectsInvolvedInFilteredOperation(operation, context.storageManager)
        const change: DeletionStorageChange<'pre'> = {
            type: 'delete',
            collection: operation[1],
            where: operation[2],
            pks: affectedObjects.map(object => getObjectPk(object, collection, context.storageManager.registry)),
        }
        return {
            changes: [change]
        }
    },
    async transformOperation(context) {
        const batch: OperationBatch = []
        let index = -1
        for (const change of context.info.changes) {
            if (change.type !== 'delete') {
                throw new Error('Something weird happened in updateObject change watcher during operation transform')
            }

            const collectionDefinition = context.storageManager.registry.collections[change.collection]

            if (typeof collectionDefinition.pkIndex === 'string') {
                index += 1

                batch.push({
                    placeholder: `change-${index}`,
                    operation: 'deleteObjects',
                    collection: change.collection,
                    where: { [collectionDefinition.pkIndex]: { $in: change.pks } },
                })
            } else {
                for (const pk of change.pks) {
                    index += 1

                    batch.push({
                        placeholder: `change-${index}`,
                        operation: 'deleteObjects',
                        collection: change.collection,
                        where: _getChangeWhere(pk, collectionDefinition),
                    })
                }
            }
        }

        return ['executeBatch', batch]
    },
    async getInfoAfterExecution(context) {
        const { operation } = context
        const collection = operation[1]

        const preInfoChange = context.preInfo.changes[0]
        if (preInfoChange.type !== 'delete') {
            throw new Error('Something weird happened in updateObject change watcher')
        }
        const change: DeletionStorageChange<'post'> = {
            type: 'delete',
            collection,
            where: operation[2],
            pks: preInfoChange.pks,
        };
        return {
            changes: [
                change
            ]
        }
    },
}

const executeBatch: StorageOperationWatcher = {
    async getInfoBeforeExecution(context) {
        const batch: OperationBatch = context.operation[1]
        const changes: StorageChange<'pre'>[] = []
        const appendInfo = (info: StorageOperationChangeInfo<'pre'>) => {
            changes.push(...info.changes)
        }

        for (const batchOperation of batch) {
            if (!batchOperation.placeholder && batchOperation.operation === 'createObject') {
                throw new Error(`ChangeWatchMiddleware cannot handle executeBatch createObject operations without placeholders`)
            }

            if (batchOperation.operation === 'createObject') {
                appendInfo(await createObject.getInfoBeforeExecution({
                    operation: ['createObject', batchOperation.collection, batchOperation.args],
                    storageManager: context.storageManager,
                }))
            } else if (batchOperation.operation === 'updateObjects') {
                appendInfo(await updateObject.getInfoBeforeExecution({
                    operation: ['updateObjects', batchOperation.collection, batchOperation.where, batchOperation.updates],
                    storageManager: context.storageManager
                }))
            } else if (batchOperation.operation === 'deleteObjects') {
                appendInfo(await deleteObject.getInfoBeforeExecution({
                    operation: ['deleteObjects', batchOperation.collection, batchOperation.where],
                    storageManager: context.storageManager,
                }))
            } else {
                throw new Error(`Change watcher middleware encountered unknown batch operation: ${(batchOperation as any).operation}`)
            }
        }

        return {
            changes
        }
    },
    async getInfoAfterExecution(context) {
        const batch: OperationBatch = context.operation[1]
        const changes: StorageChange<'post'>[] = []
        const appendInfo = (info: StorageOperationChangeInfo<'post'>) => {
            changes.push(...info.changes)
        }

        let index = -1
        for (const batchOperation of batch) {
            index += 1

            if (batchOperation.operation === 'createObject') {
                appendInfo(await createObject.getInfoAfterExecution({
                    operation: ['createObject', batchOperation.collection, batchOperation.args],
                    preInfo: { changes: [context.preInfo.changes[index]] },
                    storageManager: context.storageManager,
                    result: context.result.info[batchOperation.placeholder!],
                }))
            } else if (batchOperation.operation === 'updateObjects') {
                appendInfo(await updateObject.getInfoAfterExecution({
                    operation: ['updateObjects', batchOperation.collection, batchOperation.where, batchOperation.updates],
                    preInfo: { changes: [context.preInfo.changes[index]] },
                    storageManager: context.storageManager,
                    result: context.result,
                }))
            } else if (batchOperation.operation === 'deleteObjects') {
                appendInfo(await deleteObject.getInfoAfterExecution({
                    operation: ['deleteObjects', batchOperation.collection, batchOperation.where],
                    preInfo: { changes: [context.preInfo.changes[index]] },
                    storageManager: context.storageManager,
                    result: context.result,
                }))
            } else {
                throw new Error(`Change watcher middleware encountered unknown batch operation: ${(batchOperation as any).operation}`)
            }
        }

        return {
            changes
        }
    },
}

async function _findObjectsInvolvedInFilteredOperation(operation: any[], storageManager: StorageManager) {
    const collection = operation[1]
    return storageManager.operation(
        'findObjects', collection, operation[2],
    )
}

export function _getChangeWhere(pk: StorageChangePk, collectionDefinition: CollectionDefinition): { [key: string]: { $in: any } } {
    const where = {}
    let index = -1
    for (const pkFieldName of collectionDefinition.pkIndex!) {
        index += 1
        if (typeof pkFieldName !== 'string') {
            throw new Error(`Collection ${collectionDefinition.name!} has primary type unsupported by change watch middleware`)
        }
        where[pkFieldName] = pk[index]
    }

    return where
}

export const DEFAULT_OPERATION_WATCHERS = {
    createObject,
    updateObject,
    updateObjects: updateObject,
    deleteObject,
    deleteObjects: deleteObject,
    executeBatch,
}
