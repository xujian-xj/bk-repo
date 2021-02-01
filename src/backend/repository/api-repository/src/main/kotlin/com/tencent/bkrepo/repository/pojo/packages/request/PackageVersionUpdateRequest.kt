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

package com.tencent.bkrepo.repository.pojo.packages.request

import io.swagger.annotations.ApiModelProperty

data class PackageVersionUpdateRequest(
    @ApiModelProperty("项目id")
    val projectId: String,
    @ApiModelProperty("仓库名称")
    val repoName: String,
    @ApiModelProperty("包唯一标识符")
    val packageKey: String,
    @ApiModelProperty("版本名称")
    val versionName: String,
    @ApiModelProperty("版本大小")
    val size: Long? = null,
    @ApiModelProperty("版本描述文件路径")
    var manifestPath: String? = null,
    @ApiModelProperty("版本内容文件路径")
    var artifactPath: String? = null,
    @ApiModelProperty("版本构件阶段")
    val stageTag: List<String>? = null,
    @ApiModelProperty("版本元数据")
    val metadata: Map<String, Any>? = null,
    @ApiModelProperty("标签")
    val tags: List<String>? = null,
    @ApiModelProperty("版本扩展字段")
    val extension: Map<String, Any>? = null
)