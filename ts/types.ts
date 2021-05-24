import StorageManager from '@worldbrain/storex'

export type StorageChange<Phase extends 'pre' | 'post'> =
    | CreationStorageChange<Phase>
    | ModificationStorageChange<Phase>
    | DeletionStorageChange<Phase>

export type StorageChangePk = string | number | Array<string | number>

export type ShouldWatchCollection = (collection: string) => boolean

export interface StorageChangeBase {
    collection: string
}

export type CreationStorageChange<
    Phase extends 'pre' | 'post'
    > = StorageChangeBase & {
        type: 'create'
        values: { [key: string]: any }
    } & (Phase extends 'post' ? { pk: StorageChangePk } : { pk?: StorageChangePk })

export type ModificationStorageChange<
    Phase extends 'pre' | 'post'
    > = StorageChangeBase & {
        type: 'modify'
        where: { [key: string]: any }
        updates: { [key: string]: any }
        pks: StorageChangePk[]
    }

export type DeletionStorageChange<
    Phase extends 'pre' | 'post'
    > = StorageChangeBase & {
        type: 'delete'
        where: { [key: string]: any }
        pks: StorageChangePk[]
    }

export interface StorageOperationChangeInfo<Phase extends 'pre' | 'post'> {
    changes: StorageChange<Phase>[]
}

export interface StorageOperationEvent<Phase extends 'pre' | 'post'> {
    originalOperation: any[]
    modifiedOperation?: any[]
    info: StorageOperationChangeInfo<Phase>
}

export interface StorageOperationWatcher {
    shouldWatchOperation(context: {
        operation: any[]
        shouldWatchCollection: ShouldWatchCollection
    }): boolean
    getInfoBeforeExecution(context: {
        operation: any[]
        storageManager: StorageManager
        shouldWatchCollection: ShouldWatchCollection
    }):
        | StorageOperationChangeInfo<'pre'>
        | Promise<StorageOperationChangeInfo<'pre'>>
    transformOperation?(context: {
        originalOperation: any[]
        storageManager: StorageManager
        shouldWatchCollection: ShouldWatchCollection
        info: StorageOperationChangeInfo<'pre'>
    }): Promise<any[] | void>
    getInfoAfterExecution(context: {
        preInfo: StorageOperationChangeInfo<'pre'>
        shouldWatchCollection: ShouldWatchCollection
        storageManager: StorageManager
        operation: any[]
        result: any
    }):
        | StorageOperationChangeInfo<'post'>
        | Promise<StorageOperationChangeInfo<'post'>>
}
