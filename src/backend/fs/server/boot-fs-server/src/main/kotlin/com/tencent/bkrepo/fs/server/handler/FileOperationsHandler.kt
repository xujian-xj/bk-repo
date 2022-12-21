/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
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

package com.tencent.bkrepo.fs.server.handler

import com.tencent.bkrepo.common.artifact.exception.ArtifactNotFoundException
import com.tencent.bkrepo.common.artifact.exception.NodeNotFoundException
import com.tencent.bkrepo.common.artifact.stream.FileArtifactInputStream
import com.tencent.bkrepo.common.artifact.stream.Range
import com.tencent.bkrepo.fs.server.api.RRepositoryClient
import com.tencent.bkrepo.fs.server.bodyToArtifactFile
import com.tencent.bkrepo.fs.server.io.RegionInputStreamResource
import com.tencent.bkrepo.fs.server.request.BlockRequest
import com.tencent.bkrepo.fs.server.request.FlushRequest
import com.tencent.bkrepo.fs.server.request.NodeRequest
import com.tencent.bkrepo.fs.server.service.FileNodeService
import com.tencent.bkrepo.fs.server.service.FileOperationService
import com.tencent.bkrepo.fs.server.utils.ReactiveResponseBuilder
import com.tencent.bkrepo.fs.server.utils.ReactiveSecurityUtils
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.core.io.FileSystemResource
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import org.springframework.web.reactive.function.server.bodyValueAndAwait

/**
 * 文件操作相关处理器
 *
 * 处理文件操作请求
 * */
class FileOperationsHandler(
    private val rRepositoryClient: RRepositoryClient,
    private val fileNodeService: FileNodeService,
    private val fileOperationService: FileOperationService
) {

    /**
     * 读取文件
     * 支持按范围读取
     * */
    suspend fun read(request: ServerRequest): ServerResponse {
        with(NodeRequest(request)) {
            val node = rRepositoryClient.getNodeDetail(projectId, repoName, fullPath).awaitSingle().data
            if (node?.folder == true) {
                throw NodeNotFoundException(this.toString())
            }
            // 读取的文件，可能是个正在写入的文件，有块数据，但是还未冲刷，所以这里的size和sha256可能为null。
            val nodeSize = node?.size ?: 0
            // 新写入的块，可能还未冲刷成文件，所以需要获取最新的文件长度
            val fileLength = fileNodeService.getFileLength(projectId, repoName, fullPath, nodeSize)
            val range = resolveRange(request, fileLength)
            val artifactInputStream = fileOperationService.read(
                request = this,
                digest = node?.sha256,
                size = node?.size,
                range = range
            ) ?: throw ArtifactNotFoundException(this.toString())
            val source = if (artifactInputStream is FileArtifactInputStream) {
                FileSystemResource(artifactInputStream.file)
            } else {
                RegionInputStreamResource(artifactInputStream, range.length)
            }
            return ok().bodyValueAndAwait(source)
        }
    }

    /**
     * 写入文件块
     * 写入的文件块，立马可以被读取
     * */
    suspend fun write(request: ServerRequest): ServerResponse {
        val user = ReactiveSecurityUtils.getUser()
        val artifactFile = request.bodyToArtifactFile()
        val blockRequest = BlockRequest(request)
        val blockNode = fileOperationService.write(artifactFile, blockRequest, user)
        return ReactiveResponseBuilder.success(blockNode)
    }

    /**
     * 把文件块，冲刷到新文件
     * */
    suspend fun flush(request: ServerRequest): ServerResponse {
        val user = ReactiveSecurityUtils.getUser()
        val flushRequest = FlushRequest(request)
        fileOperationService.flush(flushRequest, user)
        return ReactiveResponseBuilder.success()
    }

    /**
     * 写入块的同时冲刷文件，避免小文件的写入需要两个请求，write和flush。
     * */
    suspend fun writeAndFlush(request: ServerRequest): ServerResponse {
        val user = ReactiveSecurityUtils.getUser()
        val blockRequest = BlockRequest(request)
        val artifactFile = request.bodyToArtifactFile()
        val blockNode = fileOperationService.write(artifactFile, blockRequest, user)
        val flushRequest = FlushRequest(request)
        fileOperationService.flush(flushRequest, user)
        return ReactiveResponseBuilder.success(blockNode)
    }

    /**
     * 处理文件范围请求
     * @param request http server request
     * @param total 文件总长度
     * @return range 文件请求范围
     * */
    private fun resolveRange(request: ServerRequest, total: Long): Range {
        val httpRange = request.headers().range().firstOrNull()
        return if (httpRange != null) {
            val startPosition = httpRange.getRangeStart(total)
            val endPosition = httpRange.getRangeEnd(total)
            Range(startPosition, endPosition, total)
        } else {
            Range.full(total)
        }
    }
}
