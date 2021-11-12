/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.tencent.bkrepo.maven.util

import com.tencent.bkrepo.maven.PACKAGE_SUFFIX_REGEX
import com.tencent.bkrepo.maven.SNAPSHOT_SUFFIX
import com.tencent.bkrepo.maven.exception.MavenArtifactFormatException
import com.tencent.bkrepo.maven.pojo.MavenVersion
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpStatus
import java.util.regex.Pattern

object MavenStringUtils {

    private const val JAVA_ARCHIVE = "application/java-archive"
    private const val X_MAVEN_POM = "application/x-maven-pom+xml"
    private const val MAVEN_XML = "application/xml"

    fun String.formatSeparator(oldSeparator: String, newSeparator: String): String {
        val strList = this.removePrefix(oldSeparator).removeSuffix(oldSeparator).split(oldSeparator)
        return StringUtils.join(strList, newSeparator)
    }

    fun String.fileMimeType(): String? {
        return if (this.endsWith("jar")) {
            JAVA_ARCHIVE
        } else if (this.endsWith("pom")) {
            X_MAVEN_POM
        } else if (this.endsWith("xml")) {
            MAVEN_XML
        } else null
    }

    fun String.httpStatusCode(): Int {
        return if (this.endsWith("maven-metadata.xml")) {
            HttpStatus.SC_ACCEPTED
        } else if (this.endsWith("maven-metadata.xml.md5") || this.endsWith("maven-metadata.xml.sha1")) {
            HttpStatus.SC_OK
        } else HttpStatus.SC_CREATED
    }

    fun String.resolverName(artifactId: String, version: String): MavenVersion {
        val matcher = Pattern.compile(PACKAGE_SUFFIX_REGEX).matcher(this)
        if (matcher.matches()) {
            val artifactName = matcher.group(1)
            val packaging = matcher.group(2)
            val mavenVersion = MavenVersion(
                artifactId = artifactId,
                version = version,
                packaging = packaging
            )
            val suffix = artifactName.removePrefix("$artifactId-${version.removeSuffix(SNAPSHOT_SUFFIX)}").trim('-')
            if (suffix.isNotBlank() && version.endsWith(SNAPSHOT_SUFFIX)) {
                val strList = suffix.split('-')
                val timestamp = if (strList.isNotEmpty()) strList[0] else null
                val buildNo = if (strList.size > 1) strList[1] else null
                val classifier =
                    if (strList.size > 2) StringUtils.join(strList.subList(2, strList.size), "-") else null
                mavenVersion.timestamp = timestamp
                mavenVersion.buildNo = buildNo
                mavenVersion.classifier = classifier
            } else {
                mavenVersion.classifier = suffix
            }
            return mavenVersion
        }
        throw MavenArtifactFormatException(this)
    }
}