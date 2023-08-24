package com.tencent.bkrepo.replication.pojo.record

import io.swagger.annotations.ApiModel
import io.swagger.annotations.ApiModelProperty

@ApiModel("同步任务执行记录详情总览")
data class RecordOverview(
    @ApiModelProperty("总数量")
    val total: Long,
    @ApiModelProperty("成功数量")
    val success: Long,
    @ApiModelProperty("失败数量")
    val fail: Long,
    @ApiModelProperty("冲突数量")
    val conflict: Long
)
