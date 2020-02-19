import cloneDeep from 'lodash/cloneDeep'
import StorageManager, { CollectionDefinition } from "@worldbrain/storex";
import { StorageMiddlewareContext, StorageMiddleware } from "@worldbrain/storex/lib/types/middleware";
import { StorageChange, StorageOperationChangeInfo, StorageOperationWatcher } from "./types";
import { DEFAULT_OPERATION_WATCHERS } from "./operation-watchers";

export interface ChangeWatchMiddlewareSettings {
    shouldWatchCollection(collection: string): boolean
    operationWatchers?: { [name: string]: StorageOperationWatcher }
    getCollectionDefinition?(collection: string): CollectionDefinition
    preprocessOperation?(operation: any[], info: StorageOperationChangeInfo<'pre'>): void | Promise<void>
    postprocessOperation?(operation: any[], info: StorageOperationChangeInfo<'post'>): void | Promise<void>
}
export class ChangeWatchMiddleware implements StorageMiddleware {
    enabled = true

    getCollectionDefinition: (collection: string) => CollectionDefinition
    operationWatchers: { [name: string]: StorageOperationWatcher }

    constructor(private options: ChangeWatchMiddlewareSettings & {
        storageManager: StorageManager
    }) {
        this.getCollectionDefinition = options.getCollectionDefinition ??
            ((collection) => options.storageManager.registry.collections[collection])
        this.operationWatchers = options.operationWatchers ?? DEFAULT_OPERATION_WATCHERS
    }

    async process(context: StorageMiddlewareContext) {
        const executeNext = () => context.next.process({ operation: cloneDeep(context.operation) })
        if (!this.enabled) {
            return executeNext()
        }

        const watcher = this.operationWatchers[context.operation[0]]
        if (!watcher) {
            return executeNext()
        }

        const originalOperation = cloneDeep(context.operation)
        const preInfo = await watcher.getInfoBeforeExecution({
            operation: originalOperation,
            storageManager: this.options.storageManager
        })
        if (this.options.preprocessOperation) {
            await this.options.preprocessOperation(originalOperation, preInfo)
        }
        const result = await executeNext()

        const postInfo = await watcher.getInfoAfterExecution({
            operation: originalOperation,
            preInfo,
            result,
            storageManager: this.options.storageManager,
        })
        if (this.options.postprocessOperation) {
            await this.options.postprocessOperation(originalOperation, postInfo)
        }
        return result
    }
}