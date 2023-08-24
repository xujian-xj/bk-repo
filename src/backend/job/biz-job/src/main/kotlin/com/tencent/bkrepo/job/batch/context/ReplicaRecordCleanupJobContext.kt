package com.tencent.bkrepo.job.batch.context

import com.tencent.bkrepo.job.batch.base.JobContext
import java.util.concurrent.atomic.AtomicLong

class ReplicaRecordCleanupJobContext(
    val recordCount: AtomicLong = AtomicLong(),
) : JobContext() {
    override fun toString(): String {
        return "Cleanup Replica Record: ${super.toString()}, Deleted Record Count[$recordCount]."
    }
}
