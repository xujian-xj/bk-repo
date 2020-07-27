package com.tencent.bkrepo.generic.controller

import com.tencent.bkrepo.common.api.pojo.Response
import com.tencent.bkrepo.common.artifact.api.ArtifactInfo
import com.tencent.bkrepo.common.service.util.ResponseBuilder
import com.tencent.bkrepo.generic.api.OperateResource
import com.tencent.bkrepo.generic.pojo.FileInfo
import com.tencent.bkrepo.generic.service.OperateService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RestController

/**
 * 文件操作接口实现类
 *
 * @author: carrypan
 * @date: 2019-10-13
 */
@RestController
class OperateResourceImpl @Autowired constructor(
    private val operateService: OperateService
) : OperateResource {

    override fun listFile(userId: String, artifactInfo: ArtifactInfo, includeFolder: Boolean, deep: Boolean): Response<List<FileInfo>> {
        return artifactInfo.run {
            ResponseBuilder.success(operateService.listFile(userId, projectId, repoName, artifactUri, includeFolder, deep))
        }
    }
}