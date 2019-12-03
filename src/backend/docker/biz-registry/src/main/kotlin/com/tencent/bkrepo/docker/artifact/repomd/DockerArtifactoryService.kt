package com.tencent.bkrepo.docker.artifact.repomd

import com.tencent.bkrepo.common.api.constant.CommonMessageCode
import com.tencent.bkrepo.common.api.exception.ErrorCodeException
import com.tencent.bkrepo.common.api.exception.ExternalErrorCodeException
import com.tencent.bkrepo.common.storage.core.FileStorage
import com.tencent.bkrepo.common.storage.util.CredentialsUtils
import com.tencent.bkrepo.common.storage.util.DataDigestUtils
import com.tencent.bkrepo.docker.DockerWorkContext
import com.tencent.bkrepo.docker.constant.REPO_TYPE
import com.tencent.bkrepo.docker.repomd.Artifact
import com.tencent.bkrepo.docker.repomd.DownloadContext
import com.tencent.bkrepo.docker.repomd.UploadContext
import com.tencent.bkrepo.docker.repomd.WriteContext
import com.tencent.bkrepo.repository.api.MetadataResource
import com.tencent.bkrepo.repository.api.NodeResource
import com.tencent.bkrepo.repository.api.RepositoryResource
import com.tencent.bkrepo.repository.pojo.metadata.MetadataSaveRequest
import com.tencent.bkrepo.repository.pojo.node.NodeDetail
import com.tencent.bkrepo.repository.pojo.node.service.NodeCreateRequest
import com.tencent.bkrepo.repository.pojo.node.service.NodeRenameRequest
import com.tencent.bkrepo.repository.pojo.node.service.NodeCopyRequest
import com.tencent.bkrepo.common.query.model.Rule
import com.tencent.bkrepo.common.query.model.QueryModel
import com.tencent.bkrepo.common.query.model.PageLimit
import com.tencent.bkrepo.common.query.model.Sort
import java.io.File
import java.io.InputStream
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.FileOutputStream

@Service
class DockerArtifactoryService @Autowired constructor(
        private val repositoryResource: RepositoryResource,
        private val nodeResource: NodeResource,
        private val fileStorage: FileStorage,
        private val metadataService: MetadataResource

) {

    // protected var propertiesService: PropertiesService ？
    protected lateinit var context: DockerWorkContext

    private var localPath: String = ""

    @Value("\${storage.localTempPath}")
    fun setAuthUsername(path: String) {
        localPath = path
    }

    init {
        this.context = DockerPackageWorkContext()
    }

    fun writeLocal(projectId:String, repoName: String,dockerRepo: String, name: String, inputStream: InputStream): ResponseEntity<Any> {

        val filePath = "$localPath/$projectId/$repoName/$dockerRepo/"
        var fullPath = "/$localPath/$projectId/$repoName/$dockerRepo/$name"
        File(filePath).mkdirs()
        val file = File(fullPath)
        if (!file.exists()) {
            file.createNewFile()
        }
        val outputStream = FileOutputStream(file, true)
        inputStream.use { input ->
            outputStream.use { output ->
                input.copyTo(output)
            }
        }
        return ResponseEntity.ok().body("ok")
    }

    fun readLocal(projectId: String,repoName: String,path: String):InputStream{
        var fullPath = "$localPath/$projectId/$repoName/$path"
        return  File(fullPath).inputStream()
    }


    fun readGlobal(context: DownloadContext): InputStream {
        // check repository
        val repository = repositoryResource.detail(context.projectId, context.repoName, REPO_TYPE).data ?: run {
            logger.warn("user ${context.userId} read global file  ${context.fullPath} failed: ${context.repoName} not found")
            throw ErrorCodeException(CommonMessageCode.ELEMENT_NOT_FOUND, context.repoName)
        }
        // get content from storage
        val storageCredentials = CredentialsUtils.readString(repository.storageCredentials?.type, repository.storageCredentials?.credentials)
        var file = fileStorage.load(context.sha256, storageCredentials)
        return file!!.inputStream()
    }

    fun getWorkContextC(): DockerWorkContext {
        return this.context
    }


    fun write(context: WriteContext) {
        try {
            // check the repository
            val repository = repositoryResource.detail(context.projectId, context.repoName, REPO_TYPE).data
                    ?: run {
                        logger.warn("user[$context.userId]  upload file  [$context.path] failed: ${context.repoName} not found")
                        throw ErrorCodeException(CommonMessageCode.ELEMENT_NOT_FOUND, context.repoName)
                    }
            // save the node
            val result = nodeResource.create(NodeCreateRequest(
                    projectId = context.projectId,
                    repoName = context.repoName,
                    folder = false,
                    fullPath = context.path,
                    size = context.contentLength,
                    sha256 = context.sha256,
                    operator = context.userId
            ))

            if (result.isOk()) {
                val storageCredentials = CredentialsUtils.readString(repository.storageCredentials?.type, repository.storageCredentials?.credentials)
                fileStorage.store(context.sha256, context.content!!, storageCredentials)
                logger.info("user[$context.userId] simply upload file [$context.path] success")
            } else {
                logger.warn("user[$context.userId] simply upload file [$context.path] failed: [${result.code}, ${result.message}]")
                throw ExternalErrorCodeException(result.code, result.message)
            }
        } catch (exception: Exception) {
            throw RuntimeException("Failed to save stream to ${context.path}", exception)
        }
    }

    fun delete(path: String): Boolean {
        return true
    }

    fun deleteLocal(projectId: String,repoName: String,path: String): Boolean {
        val fullPath = "$localPath/$projectId/$repoName/$path"
        return File(fullPath).delete()
    }

    fun download(context: DownloadContext): File {
        // check repository
        val repository = repositoryResource.detail(context.projectId, context.repoName, REPO_TYPE).data ?: run {
            logger.warn("user[$context.userId] simply download file  [$context.path] failed: $context.repoName not found")
            throw ErrorCodeException(CommonMessageCode.ELEMENT_NOT_FOUND, context.repoName)
        }

        // fileStorage
        val storageCredentials = CredentialsUtils.readString(repository.storageCredentials?.type, repository.storageCredentials?.credentials)
        var file = fileStorage.load(context.sha256, storageCredentials)
        return file!!
    }


    @Transactional(rollbackFor = [Throwable::class])
    fun upload(context: UploadContext): ResponseEntity<Any> {
        // check repository
        val repository = repositoryResource.detail(context.projectId, context.repoName, REPO_TYPE).data ?: run {
            logger.warn("user[$context.userId]  upload file  [$context.path] failed: ${context.repoName} not found")
            throw ErrorCodeException(CommonMessageCode.ELEMENT_NOT_FOUND, context.repoName)
        }

        // save node
        val result = nodeResource.create(NodeCreateRequest(
                projectId = context.projectId,
                repoName = context.repoName,
                folder = false,
                fullPath = context.path,
                size = context.contentLength,
                sha256 = context.sha256,
                operator = context.userId,
                metadata = emptyMap(),
                overwrite = true
        ))

        if (result.isOk()) {
            val storageCredentials = CredentialsUtils.readString(repository.storageCredentials?.type, repository.storageCredentials?.credentials)
            fileStorage.store(context.sha256, context.content!!, storageCredentials)
            logger.info("user[$context.userId] simply upload file [$context.path] success")
        } else {
            logger.warn("user[$context.userId] simply upload file [$context.path] failed: [${result.code}, ${result.message}]")
            throw ExternalErrorCodeException(result.code, result.message)
        }
        return ResponseEntity.ok().body("ok")
    }

    @Transactional(rollbackFor = [Throwable::class])
    fun uploadFromLocal(path: String, context: UploadContext): ResponseEntity<Any> {
        // check repository
        val repository = repositoryResource.detail(context.projectId, context.repoName, REPO_TYPE).data ?: run {
            logger.warn("user[$context.userId]  upload file  [$context.path] failed: ${context.repoName} not found")
            throw ErrorCodeException(CommonMessageCode.ELEMENT_NOT_FOUND, context.repoName)
        }
        var fullPath = "$localPath/${context.projectId}/${context.repoName}/$path"
        var content = File(fullPath).readBytes()
        context.content(content.inputStream()).contentLength(content.size.toLong()).sha256(DataDigestUtils.sha256FromByteArray(content))

        val node = NodeCreateRequest(
                projectId = context.projectId,
                repoName = context.repoName,
                folder = false,
                fullPath = context.path,
                size = context.contentLength,
                sha256 = context.sha256,
                operator = context.userId,
                metadata = emptyMap(),
                overwrite = true
        )

        // save node
        val result = nodeResource.create(node)
        if (result.isOk()) {
            val storageCredentials = CredentialsUtils.readString(repository.storageCredentials?.type, repository.storageCredentials?.credentials)
            fileStorage.store(context.sha256, context.content!!, storageCredentials)
            logger.info("user[$context.userId] simply upload file [$context.path] success")
        } else {
            logger.warn("user[$context.userId] simply upload file [$context.path] failed: [${result.code}, ${result.message}]")
            throw ExternalErrorCodeException(result.code, result.message)
        }
        return ResponseEntity.ok().body("ok")
    }


//    private fun contentLength(context: UploadContext): Long {
//        if (context.getContentLength() > 0L) {
//            return context.getContentLength()
//        } else {
//            val headers = context.getRequestHeaders()
//            if (headers != null) {
//                val headerValue = (headers!!.entries.stream().filter({ entry -> "Content-Length".equals(entry.key as String, ignoreCase = true).toLong() }).findFirst().map({ entry -> Optional.ofNullable<String>(entry.value).toLong() }).orElse(Optional.empty<String>()) as Optional<*>).orElse("-1") as String
//
//                try {
//                    return java.lang.Long.valueOf(headerValue)
//                } catch (var5: NumberFormatException) {
//                    log.warn("Content-Length header value is not a number: '{}' (path: )", headerValue, context.getPath())
//                }
//
//            }
//
//            return -1L
//        }
//    }

    fun copy(projectId: String,repoName: String, srcPath:String, destPath: String): Boolean {
        val copyRequest = NodeCopyRequest(
                srcProjectId = projectId,
                srcRepoName = repoName,
                srcFullPath = srcPath,
                destProjectId = projectId,
                destRepoName = repoName,
                destPath = destPath,
                overwrite = true,
                operator = "bk_admin"
        )
        nodeResource.copy(copyRequest)
        return true
    }

    fun move(projectId: String,repoName: String, from: String, to: String): Boolean {
        logger.info("rename, path: $from, to: $to")
        val renameRequest  = NodeRenameRequest(projectId, repoName, from, to ,"bk_admin")
        val result = nodeResource.rename(renameRequest)
        if (result.isNotOk()) {
            logger.warn("reanme  [$from] to [$to] failed: [${result.code}, ${result.message}]")
            throw ExternalErrorCodeException(result.code, result.message)
        }
        logger.info(" rename [$from] to [$to}] success")
        return true
    }

//    override fun getAttribute(path: String, key: String): Any? {
//        val properties = this.propertiesService.getProperties(this.repoPath(path))
//        return if (properties.containsKey(key)) properties.getFirst(key) else null
//    }

//    open fun getAttributes(path: String, key: String): Set<*>? {
//        val properties = this.propertiesService.getProperties(this.repoPath(path))
//        return if (properties.containsKey(key)) properties.get(key) else null
//    }



//    fun addAttribute(path: String, key: String, vararg values: Any) {
//        this.addProperty(path, key, values)
//    }

//    fun removeAttribute(path: String, key: String, vararg values: Any) {
//        this.removePropertyValues(path, key, values)
//    }

//    fun setAttributes(path: String, keyValueMap: Map<String, String>) {
//
//        throw UnsupportedOperationException("NOT IMPLEMENTED")
//        // this.setProperties(path, keyValueMap)
//    }

    fun setAttributes(projectId: String, repoName: String, path: String, keyValueMap: Map<String, String>) {
        metadataService.save(MetadataSaveRequest(projectId, repoName, path, keyValueMap))
    }

    fun getAttribute(projectId: String, repoName: String, fullPath: String,key:String) :String?{
        return metadataService.query(projectId,repoName,fullPath).data!!.get(key)
    }

    fun exists(projectId: String, repoName: String, dockerRepo: String): Boolean {
        return nodeResource.exist(projectId, repoName, dockerRepo).data!!
//        return this.repoService.exists(this.repoPath(path))
    }

    fun existsLocal(projectId:String, repoName:String, path: String): Boolean {
        val fullPath = "$localPath/$projectId/$repoName/$path"
        val file = File(fullPath)
        return file.exists()
    }

    fun canRead(path: String): Boolean {
        return true
//        return this.authorizationService.canRead(this.repoPath(path))
    }

    fun canWrite(path: String): Boolean {
        return true
    }

    fun canDelete(path: String): Boolean {
        return true
    }


    fun artifactLocal(projectId: String, repoName: String, dockerRepo: String): Artifact? {
        val fullPath = "$localPath/$projectId/$repoName/$dockerRepo"
        val file = File(fullPath)
        val content = file.readBytes()
        val sha256 = DataDigestUtils.sha256FromByteArray(content)
        var length = content.size.toLong()
        return Artifact(projectId, repoName, dockerRepo).sha256(sha256).contentLength(length)
    }

    fun artifact(projectId: String, repoName: String, fullPath: String): Artifact? {
        val nodes = nodeResource.queryDetail(projectId, repoName, fullPath).data ?: run {
            logger.warn("find artifact failed: $projectId, $repoName, $fullPath found no artifacts")
            return  null
        }
        return Artifact(projectId, repoName, fullPath).sha256(nodes.nodeInfo.sha256!!).contentLength(nodes.nodeInfo.size)
    }

    fun findArtifact(projectId: String, repoName: String, dockerRepo: String, fileName: String): NodeDetail? {
        // query node info
        var fullPath = "/$dockerRepo/$fileName"
        val nodes = nodeResource.queryDetail(projectId, repoName, fullPath).data ?: run {
            logger.warn("find artifacts failed: $projectId, $repoName, $fullPath found no node")
            return  null
        }
        return nodes
    }

    fun findArtifacts(projectId: String, repoName: String,fileName:String): List<Map<String,Any>> {
        val projectId = Rule.QueryRule("projectId", projectId)
        val repoName = Rule.QueryRule("repoName", repoName)
        val name = Rule.QueryRule("name", fileName)
        val rule = Rule.NestedRule(mutableListOf(projectId, repoName, name))
        val queryModel = QueryModel(
                page = PageLimit(0, 10),
                sort = Sort(listOf("fullPath"), Sort.Direction.ASC),
                select = mutableListOf("fullPath","path","size"),
                rule = rule
        )

        val result =  nodeResource.query(queryModel).data?: run {
            logger.warn("find artifacts failed: $projectId, $repoName, $name, $fileName found no node")
            return  emptyList()
        }
        return result.records
    }

    fun findArtifacts(fileName:String): List<Map<String,Any>> {
        //find artifacts by name
        val projectId = Rule.QueryRule("projectId", "ops")
        val repoName = Rule.QueryRule("repoName", "dockerlocal")
        val name = Rule.QueryRule("name", fileName)
        val rule = Rule.NestedRule(mutableListOf( projectId, repoName,name))
        val queryModel = QueryModel(
                page = PageLimit(0, 9999999),
                sort = Sort(listOf("path"), Sort.Direction.ASC),
                select = mutableListOf("path"),
                rule = rule
        )
        val result =  nodeResource.query(queryModel).data?: run {
            logger.warn("find artifacts failed:  $fileName found no node")
            return  emptyList()
        }
        return result.records
    }

    fun findManifest(projectId: String, repoName: String, manifestPath:String): NodeDetail? {
        // query node info
        val nodes = nodeResource.queryDetail(projectId, repoName, manifestPath).data ?: run {
            logger.warn("find manifest failed: $projectId, $repoName, $manifestPath found no node")
            return  null
        }
        return nodes
    }


    companion object {
        private val logger = LoggerFactory.getLogger(DockerArtifactoryService::class.java)
    }
}
