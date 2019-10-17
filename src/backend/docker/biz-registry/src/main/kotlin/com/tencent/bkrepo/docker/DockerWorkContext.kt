package com.tencent.bkrepo.docker

import com.tencent.bkrepo.docker.repomd.Artifact
import com.tencent.bkrepo.docker.repomd.WorkContext
import com.tencent.bkrepo.docker.v2.helpers.DockerSearchBlobPolicy
import java.io.InputStream
import java.net.URI

interface DockerWorkContext : WorkContext {

//    val keyPair: KeyPair
    fun cleanup(var1: String, var2: String)
//
    fun onTagPushedSuccessfully(var1: String, var2: String, var3: String)
//
//    @Throws(DockerLockManifestException::class)
    fun obtainManifestLock(var1: String): String
//
    fun releaseManifestLock(var1: String, var2: String)
//
    fun findBlobsGlobally(var1: String, var2: DockerSearchBlobPolicy): Iterable<Artifact>
//
    fun isBlobReadable(var1: Artifact): Boolean
//
    fun readGlobal(var1: String): InputStream
//
    fun copy(var1: String, var2: String): Boolean
//
//    fun copyToCache(sourcePath: String, targetPath: String): Boolean {
//        return false
//    }
//

    fun rewriteRepoURI(repoKey: String, uri: URI, headers: MutableSet<MutableMap.MutableEntry<String, List<String>>>): URI {
        return uri
    }

    fun translateRepoId(var1: String): String
}
