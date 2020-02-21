import cloneDeep from 'lodash/cloneDeep'
import StorageManager, { CollectionDefinition } from "@worldbrain/storex";
import { StorageMiddlewareContext, StorageMiddleware } from "@worldbrain/storex/lib/types/middleware";
import { StorageOperationChangeInfo, StorageOperationWatcher, StorageOperationEvent } from "./types";
import { DEFAULT_OPERATION_WATCHERS } from "./operation-watchers";

export interface ChangeWatchMiddlewareSettings {
    shouldWatchCollection(collection: string): boolean
    operationWatchers?: { [name: string]: StorageOperationWatcher }
    getCollectionDefinition?(collection: string): CollectionDefinition
    preprocessOperation?(context: StorageOperationEvent<'pre'>): void | Promise<void>
    postprocessOperation?(context: StorageOperationEvent<'post'>): void | Promise<void>
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
        const originalOperation = cloneDeep(context.operation)
        let modifiedOperation: any[] | undefined
        const executeNext = (preInfo?: StorageOperationChangeInfo<'pre'>) => {
            if (!preInfo) {
                preInfo = { changes: [] }
            }
            return context.next.process({
                operation: modifiedOperation || cloneDeep(originalOperation),
                extraData: {
                    changeInfo: preInfo,
                }
            })
        }
        if (!this.enabled) {
            return executeNext()
        }

        const watcher = this.operationWatchers[context.operation[0]]
        if (!watcher) {
            return executeNext()
        }

        const rawPreInfo = await watcher.getInfoBeforeExecution({
            operation: originalOperation,
            storageManager: this.options.storageManager
        })
        const preInfo: StorageOperationChangeInfo<'pre'> = {
            changes: rawPreInfo.changes.filter(change => this.options.shouldWatchCollection(change.collection))
        }
        if (!preInfo.changes.length) {
            return executeNext()
        }
        if (watcher.transformOperation) {
            modifiedOperation = (await watcher.transformOperation({
                originalOperation,
                storageManager: this.options.storageManager,
                info: preInfo,
            })) || undefined
        }
        if (this.options.preprocessOperation) {
            await this.options.preprocessOperation({ originalOperation, modifiedOperation, info: preInfo })
        }
        const result = await executeNext(preInfo)

        const postInfo = await watcher.getInfoAfterExecution({
            operation: originalOperation,
            preInfo: rawPreInfo,
            result,
            storageManager: this.options.storageManager,
        })
        if (this.options.postprocessOperation) {
            await this.options.postprocessOperation({ originalOperation, modifiedOperation, info: postInfo })
        }
        return result
    }
}
