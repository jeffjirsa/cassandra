package org.apache.cassandra.db.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Operations that run on the {@link CompactionManager.CompactionExecutor}
 * will utilize the priorities here to sort compaction tasks using
 * a priority queue (via {@link Priorities}).
 */
public enum TaskPriority
{
    MIN(0),                           // used by default, for any unprioritized task
    COMPACTION(128),
    KEY_CACHE_SAVE(256),
    ROW_CACHE_SAVE(256),
    COUNTER_CACHE_SAVE(256),
    CLEANUP(64),
    SCRUB(64),
    UPGRADE_SSTABLES(64),
    INDEX_BUILD(512),
    TOMBSTONE_COMPACTION(96),
    ANTICOMPACTION(1024),
    VERIFY(1),
    VIEW_BUILD(512),
    USER_DEFINED_COMPACTION(192),
    RELOCATE(192),
    GARBAGE_COLLECT(96),
    VALIDATION(Integer.MAX_VALUE);     // Validation compactions run on a separate executor,
                                       // so are not really subject to prioritization

    private static final Logger logger = LoggerFactory.getLogger(TaskPriority.class);
    private static final String priorityPropertyRoot = "cassandra.compaction.priority.";
    private final int priority;

    TaskPriority(int priority)
    {
        this.priority = priority;
    }

    public int priority()
    {
        return Math.max(Integer.getInteger(priorityPropertyRoot + name().toLowerCase(), priority), 1);
    }

    static TaskPriority forCacheWrite(OperationType cacheWriterType)
    {
        switch(cacheWriterType)
        {
            case KEY_CACHE_SAVE:
                return KEY_CACHE_SAVE;
            case ROW_CACHE_SAVE:
                return ROW_CACHE_SAVE;
            case COUNTER_CACHE_SAVE:
                return COUNTER_CACHE_SAVE;
            default:
                logger.warn("Unknown operation type {} for cache writer, will be prioritized with default value",
                            cacheWriterType);
                return MIN;
        }
    }
}
