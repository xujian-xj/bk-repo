package com.tencent.bkrepo.common.storage.filesystem.cleanup

import com.tencent.bkrepo.common.storage.filesystem.FileLockExecutor
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.time.Duration

class CleanupFileVisitor(
    private val rootPath: Path,
    private val expireDays: Int
) : SimpleFileVisitor<Path>() {

    val cleanupResult = CleanupResult()

    @Throws(IOException::class)
    override fun visitFile(filePath: Path, attributes: BasicFileAttributes): FileVisitResult {
        if (isExpired(attributes, expireDays)) {
            val size = attributes.size()
            FileLockExecutor.executeInLock(filePath.toFile()) {
                Files.delete(filePath)
            }
            cleanupResult.fileCount += 1
            cleanupResult.size += size
        }
        return FileVisitResult.CONTINUE
    }

    @Throws(IOException::class)
    override fun postVisitDirectory(dirPath: Path, exc: IOException?): FileVisitResult {
        if (!Files.isSameFile(rootPath, dirPath) && !Files.list(dirPath).iterator().hasNext()) {
            Files.delete(dirPath)
            cleanupResult.folderCount += 1
        }
        return FileVisitResult.CONTINUE
    }

    /**
     * 判断文件是否过期
     * 根据上次访问时间和上次修改时间判断
     */
    private fun isExpired(attributes: BasicFileAttributes, expireDays: Int): Boolean {
        val lastAccessTime = attributes.lastAccessTime().toMillis()
        val lastModifiedTime = attributes.lastModifiedTime().toMillis()
        val expiredTime = System.currentTimeMillis() - Duration.ofDays(expireDays.toLong()).toMillis()
        return lastAccessTime < expiredTime && lastModifiedTime < expiredTime
    }
}
