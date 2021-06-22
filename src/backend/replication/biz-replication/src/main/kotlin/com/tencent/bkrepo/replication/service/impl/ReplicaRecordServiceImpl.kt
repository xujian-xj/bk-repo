package com.tencent.bkrepo.replication.service.impl

import com.tencent.bkrepo.common.api.exception.ErrorCodeException
import com.tencent.bkrepo.common.api.pojo.Page
import com.tencent.bkrepo.common.mongo.dao.util.Pages
import com.tencent.bkrepo.replication.dao.ReplicaRecordDao
import com.tencent.bkrepo.replication.dao.ReplicaRecordDetailDao
import com.tencent.bkrepo.replication.dao.ReplicaTaskDao
import com.tencent.bkrepo.replication.message.ReplicationMessageCode
import com.tencent.bkrepo.replication.model.TReplicaRecord
import com.tencent.bkrepo.replication.model.TReplicaRecordDetail
import com.tencent.bkrepo.replication.model.TReplicaTask
import com.tencent.bkrepo.replication.pojo.record.ExecutionResult
import com.tencent.bkrepo.replication.pojo.record.ExecutionStatus
import com.tencent.bkrepo.replication.pojo.record.ReplicaProgress
import com.tencent.bkrepo.replication.pojo.record.ReplicaRecordDetail
import com.tencent.bkrepo.replication.pojo.record.ReplicaRecordDetailListOption
import com.tencent.bkrepo.replication.pojo.record.ReplicaRecordInfo
import com.tencent.bkrepo.replication.pojo.record.ReplicaTaskRecordInfo
import com.tencent.bkrepo.replication.pojo.record.request.RecordDetailInitialRequest
import com.tencent.bkrepo.replication.pojo.task.ReplicationStatus
import com.tencent.bkrepo.replication.service.ReplicaRecordService
import com.tencent.bkrepo.replication.util.CronUtils
import com.tencent.bkrepo.replication.util.TaskRecordQueryHelper
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ReplicaRecordServiceImpl(
    private val replicaRecordDao: ReplicaRecordDao,
    private val replicaRecordDetailDao: ReplicaRecordDetailDao,
    private val replicaTaskDao: ReplicaTaskDao
) : ReplicaRecordService {

    override fun startNewRecord(key: String): ReplicaRecordInfo {
        val initialRecord = initialRecord(key)
        val tReplicaTask = replicaTaskDao.findByKey(key)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, key)
        tReplicaTask.status = ReplicationStatus.REPLICATING
        tReplicaTask.lastExecutionTime = LocalDateTime.now()
        if (isCronJob(tReplicaTask)) {
            tReplicaTask.nextExecutionTime =
                CronUtils.getNextTriggerTime(key, tReplicaTask.setting.executionPlan.cronExpression!!)
        }
        tReplicaTask.lastExecutionStatus = ExecutionStatus.RUNNING
        replicaTaskDao.save(tReplicaTask)
        return initialRecord
    }

    override fun initialRecord(taskKey: String): ReplicaRecordInfo {
        val record = TReplicaRecord(
            taskKey = taskKey,
            status = ExecutionStatus.RUNNING,
            startTime = LocalDateTime.now()
        )
        return try {
            replicaRecordDao.insert(record).let { convert(it)!! }
        } catch (exception: DuplicateKeyException) {
            logger.warn("init record [$taskKey] error: [${exception.message}]")
            throw exception
        }
    }

    override fun completeRecord(recordId: String, status: ExecutionStatus, errorReason: String?) {
        val replicaRecordInfo = getRecordById(recordId)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, recordId)
        val record = with(replicaRecordInfo) {
            TReplicaRecord(
                id = id,
                taskKey = taskKey,
                status = status,
                startTime = startTime,
                endTime = LocalDateTime.now(),
                errorReason = errorReason
            )
        }
        val tReplicaTask = replicaTaskDao.findByKey(record.taskKey)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, record.taskKey)
        tReplicaTask.lastExecutionStatus = status
        tReplicaTask.status = if (isCronJob(tReplicaTask)) ReplicationStatus.WAITING else ReplicationStatus.COMPLETED
        replicaRecordDao.save(record)
        replicaTaskDao.save(tReplicaTask)
        logger.info("complete record [$recordId], status from [${replicaRecordInfo.status}] to [$status].")
    }

    override fun initialRecordDetail(request: RecordDetailInitialRequest): ReplicaRecordDetail {
        with(request) {
            val recordDetail = TReplicaRecordDetail(
                recordId = recordId,
                localCluster = localCluster,
                remoteCluster = remoteCluster,
                localRepoName = localRepoName,
                repoType = repoType,
                status = ExecutionStatus.RUNNING,
                progress = ReplicaProgress(),
                startTime = LocalDateTime.now()
            )
            return try {
                replicaRecordDetailDao.insert(recordDetail).let { convert(it)!! }
            } catch (exception: DuplicateKeyException) {
                logger.warn("init record detail [$recordId] error: [${exception.message}]")
                throw exception
            }
        }
    }

    override fun updateRecordDetailProgress(detailId: String, progress: ReplicaProgress) {
        val replicaRecordDetail = findRecordDetailById(detailId)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, detailId)
        replicaRecordDetail.progress.success = progress.success
        replicaRecordDetail.progress.skip = progress.skip
        replicaRecordDetail.progress.failed = progress.failed
        replicaRecordDetail.progress.totalSize = progress.totalSize
        replicaRecordDetailDao.save(replicaRecordDetail)
        logger.info("Update record detail [$detailId] success.")
    }

    override fun completeRecordDetail(detailId: String, result: ExecutionResult) {
        val replicaRecordDetail = getRecordDetailById(detailId)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, detailId)
        val recordDetail = with(replicaRecordDetail) {
            TReplicaRecordDetail(
                id = detailId,
                recordId = recordId,
                localCluster = localCluster,
                remoteCluster = remoteCluster,
                localRepoName = localRepoName,
                repoType = repoType,
                status = result.status,
                progress = result.progress!!,
                startTime = startTime,
                endTime = LocalDateTime.now(),
                errorReason = result.errorReason
            )
        }
        replicaRecordDetailDao.save(recordDetail)
    }

    override fun listRecordsByTaskKey(key: String): List<ReplicaRecordInfo> {
        return replicaRecordDao.listByTaskKey(key).map { convert(it)!! }
    }

    override fun listRecordsPage(key: String, pageNumber: Int, pageSize: Int): Page<ReplicaRecordInfo> {
        val pageRequest = Pages.ofRequest(pageNumber, pageSize)
        val query = TaskRecordQueryHelper.recordListQuery(key)
        val totalRecords = replicaRecordDao.count(query)
        val records = replicaRecordDao.find(query.with(pageRequest)).map { convert(it)!! }
        return Pages.ofResponse(pageRequest, totalRecords, records)
    }

    override fun listDetailsByRecordId(recordId: String): List<ReplicaRecordDetail> {
        return replicaRecordDetailDao.listByRecordId(recordId).map { convert(it)!! }
    }

    override fun getRecordAndTaskInfoByRecordId(recordId: String): ReplicaTaskRecordInfo {
        val replicaRecordInfo = getRecordById(recordId)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, recordId)
        val taskKey = replicaRecordInfo.taskKey
        val replicaTask = replicaTaskDao.findByKey(taskKey)
            ?: throw ErrorCodeException(ReplicationMessageCode.REPLICA_TASK_NOT_FOUND, taskKey)
        return ReplicaTaskRecordInfo(replicaTask.replicaObjectType, replicaRecordInfo)
    }

    override fun getRecordById(id: String): ReplicaRecordInfo? {
        return convert(replicaRecordDao.findById(id))
    }

    override fun getRecordDetailById(id: String): ReplicaRecordDetail? {
        return convert(findRecordDetailById(id))
    }

    private fun findRecordDetailById(id: String): TReplicaRecordDetail? {
        return replicaRecordDetailDao.findById(id)
    }

    override fun deleteByTaskKey(key: String) {
        replicaRecordDao.listByTaskKey(key).forEach {
            replicaRecordDetailDao.deleteByRecordId(it.id!!)
        }
        replicaRecordDao.deleteByTaskKey(key)
    }

    override fun listRecordDetailPage(
        recordId: String,
        option: ReplicaRecordDetailListOption
    ): Page<ReplicaRecordDetail> {
        val pageNumber = option.pageNumber
        val pageSize = option.pageSize
        val pageRequest = Pages.ofRequest(pageNumber, pageSize)
        val query = TaskRecordQueryHelper.recordDetailListQuery(recordId, option)
        val totalRecords = replicaRecordDetailDao.count(query)
        val records = replicaRecordDetailDao.find(query.with(pageRequest)).map { convert(it)!! }
        return Pages.ofResponse(pageRequest, totalRecords, records)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ReplicaRecordServiceImpl::class.java)

        private fun isCronJob(tReplicaTask: TReplicaTask): Boolean {
            return !tReplicaTask.setting.executionPlan.cronExpression.isNullOrBlank()
        }

        private fun convert(tReplicaRecord: TReplicaRecord?): ReplicaRecordInfo? {
            return tReplicaRecord?.let {
                ReplicaRecordInfo(
                    id = it.id!!,
                    taskKey = it.taskKey,
                    status = it.status,
                    startTime = it.startTime,
                    endTime = it.endTime,
                    errorReason = it.errorReason
                )
            }
        }

        private fun convert(tReplicaRecordDetail: TReplicaRecordDetail?): ReplicaRecordDetail? {
            return tReplicaRecordDetail?.let {
                ReplicaRecordDetail(
                    id = it.id.orEmpty(),
                    recordId = it.recordId,
                    localCluster = it.localCluster,
                    remoteCluster = it.remoteCluster,
                    localRepoName = it.localRepoName,
                    repoType = it.repoType,
                    packageConstraint = it.packageConstraint,
                    pathConstraint = it.pathConstraint,
                    status = it.status,
                    progress = it.progress,
                    startTime = it.startTime,
                    endTime = it.endTime,
                    errorReason = it.errorReason
                )
            }
        }
    }
}
