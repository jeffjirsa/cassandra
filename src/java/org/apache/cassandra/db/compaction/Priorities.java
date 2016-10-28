package org.apache.cassandra.db.compaction;

public class Priorities implements Comparable<Priorities>
{
    public static final Priorities DEFAULT = new Priorities(0, 0, 0);
    public final int type;
    public final long subtype;
    public final long timestamp;

    public Priorities(int type)
    {
        this(type, 0, System.currentTimeMillis());
    }

    public Priorities(int type, long subtype)
    {
        this(type, subtype, System.currentTimeMillis());
    }

    public Priorities(int type, long subtype, long timestamp)
    {
        this.type = type;
        this.subtype = subtype;
        this.timestamp = timestamp;
    }

    public int compareTo(Priorities o)
    {
        if (type > o.type)
            return -1;
        if (type < o.type)
            return 1;
        if (subtype > o.subtype)
            return -1;
        if (subtype < o.subtype)
            return 1;

        // If same op type, and same sub priority
        // Favor the task with the lowest timestamp
        return timestamp < o.timestamp
               ? -1
               : timestamp > o.timestamp ? 1 : 0;
    }
}
