/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2024 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bkrepo.job.batch.task.cache

import com.tencent.bkrepo.common.service.cluster.ClusterProperties
import com.tencent.bkrepo.common.service.log.LoggerHolder
import com.tencent.bkrepo.common.storage.core.StorageProperties
import com.tencent.bkrepo.common.storage.core.cache.indexer.StorageCacheIndexProperties
import com.tencent.bkrepo.common.storage.core.cache.indexer.StorageCacheIndexerManager
import com.tencent.bkrepo.common.storage.credentials.StorageCredentials
import com.tencent.bkrepo.job.config.properties.StorageCacheIndexSyncJobProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.stereotype.Component

/**
 * 缓存索引器中维护缓存条目信息可能与实际磁盘中缓存的文件条目不一致，需要定时同步
 */
@Component
@EnableConfigurationProperties(StorageCacheIndexSyncJobProperties::class)
class StorageCacheIndexSyncJob(
    properties: StorageCacheIndexSyncJobProperties,
    storageProperties: StorageProperties,
    clusterProperties: ClusterProperties,
    mongoTemplate: MongoTemplate,
    storageCacheIndexProperties: StorageCacheIndexProperties?,
    indexerManager: StorageCacheIndexerManager?
) : StorageCacheIndexJob(
    properties,
    storageProperties,
    clusterProperties,
    mongoTemplate,
    storageCacheIndexProperties,
    indexerManager
) {

    override fun doWithCredentials(credentials: StorageCredentials) {
        val synced = indexerManager?.sync(credentials)
        logger.info("credential[default] sync[$synced]")
    }

    companion object {
        private val logger = LoggerHolder.jobLogger
    }
}
