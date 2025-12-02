import cloneDeep from 'lodash/cloneDeep'
import type StorageManager from '@worldbrain/storex/ts'
import type { CollectionDefinition } from '@worldbrain/storex/ts'
import type {
    StorageMiddlewareContext,
    StorageMiddleware,
} from '@worldbrain/storex/ts/types/middleware'
import type {
    StorageOperationChangeInfo,
    StorageOperationWatcher,
    StorageOperationEvent,
    ShouldWatchCollection,
    CustomStorageOperationWatcher,
} from './types'
import { DEFAULT_OPERATION_WATCHERS } from './operation-watchers'

export interface ChangeWatchMiddlewareSettings {
    shouldWatchCollection: ShouldWatchCollection
    operationWatchers?: { [opName: string]: StorageOperationWatcher }
    /**
     * Define custom actions here that this middleware should take for specific non-standard operations.
     */
    customOperationWatchers?: {
        [opName: string]: CustomStorageOperationWatcher
    }
    getCollectionDefinition?(collection: string): CollectionDefinition
    preprocessOperation?(
        context: StorageOperationEvent<'pre'>,
    ): void | Promise<void>
    postprocessOperation?(
        context: StorageOperationEvent<'post'>,
    ): void | Promise<void>
}

export class ChangeWatchMiddleware implements StorageMiddleware {
    enabled = true

    getCollectionDefinition: (collection: string) => CollectionDefinition
    operationWatchers: { [name: string]: StorageOperationWatcher }
    customOperationWatchers: { [name: string]: CustomStorageOperationWatcher }

    constructor(
        private options: ChangeWatchMiddlewareSettings & {
            storageManager: StorageManager
        },
    ) {
        this.getCollectionDefinition =
            options.getCollectionDefinition ??
            ((collection) =>
                options.storageManager.registry.collections[collection])
        this.operationWatchers =
            options.operationWatchers ?? DEFAULT_OPERATION_WATCHERS
        this.customOperationWatchers = options.customOperationWatchers ?? {}
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
                },
            })
        }
        if (!this.enabled) {
            return executeNext()
        }

        const customOpWatcher =
            this.customOperationWatchers[context.operation[0]]
        if (customOpWatcher != null) {
            const { skipNextMiddlewares } = await customOpWatcher(
                context.operation,
            )
            if (skipNextMiddlewares) {
                return
            }
            return executeNext()
        }

        const opWatcher = this.operationWatchers[context.operation[0]]
        if (!opWatcher) {
            return executeNext()
        }

        const shouldWatchOperation = opWatcher.shouldWatchOperation({
            operation: originalOperation,
            shouldWatchCollection: this.options.shouldWatchCollection,
        })
        if (!shouldWatchOperation) {
            return executeNext()
        }

        const rawPreInfo = await opWatcher.getInfoBeforeExecution({
            operation: originalOperation,
            storageManager: this.options.storageManager,
            shouldWatchCollection: this.options.shouldWatchCollection,
        })
        const preInfo: StorageOperationChangeInfo<'pre'> = {
            changes: rawPreInfo.changes.filter((change) =>
                this.options.shouldWatchCollection(change.collection),
            ),
        }
        if (!preInfo.changes.length) {
            return executeNext()
        }
        if (opWatcher.transformOperation) {
            modifiedOperation =
                (await opWatcher.transformOperation({
                    originalOperation,
                    storageManager: this.options.storageManager,
                    info: preInfo,
                    shouldWatchCollection: this.options.shouldWatchCollection,
                })) || undefined
        }
        if (this.options.preprocessOperation) {
            await this.options.preprocessOperation({
                originalOperation,
                modifiedOperation,
                info: preInfo,
            })
        }
        const result = await executeNext(preInfo)

        const postInfo = await opWatcher.getInfoAfterExecution({
            operation: originalOperation,
            preInfo: rawPreInfo,
            result,
            storageManager: this.options.storageManager,
            shouldWatchCollection: this.options.shouldWatchCollection,
        })
        if (this.options.postprocessOperation) {
            await this.options.postprocessOperation({
                originalOperation,
                modifiedOperation,
                info: postInfo,
            })
        }
        return result
    }
}

export function mergeChangeWatchSettings(
    allSettings: Array<ChangeWatchMiddlewareSettings | undefined | null>,
): ChangeWatchMiddlewareSettings {
    const operationWatchers: NonNullable<
        ChangeWatchMiddlewareSettings['operationWatchers']
    > = {}
    for (const settings of allSettings) {
        Object.assign(operationWatchers, settings?.operationWatchers ?? {})
    }

    return {
        shouldWatchCollection: (collection) => {
            for (const settings of allSettings) {
                if (settings?.shouldWatchCollection?.(collection)) {
                    return true
                }
            }
            return false
        },
        operationWatchers: Object.keys(operationWatchers).length
            ? operationWatchers
            : undefined,
        getCollectionDefinition: (collection) => {
            for (const settings of allSettings) {
                const definition =
                    settings?.getCollectionDefinition?.(collection)
                if (definition) {
                    return definition
                }
            }
            throw new Error(
                `Could not find definition for collection '${collection}'`,
            )
        },
        preprocessOperation: async (context) => {
            for (const settings of allSettings) {
                await settings?.preprocessOperation?.(context)
            }
        },
        postprocessOperation: async (context) => {
            for (const settings of allSettings) {
                await settings?.postprocessOperation?.(context)
            }
        },
    }
}
