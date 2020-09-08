package com.tencent.bkrepo.auth.api

import com.tencent.bkrepo.auth.constant.AUTH_API_ROLE_PREFIX
import com.tencent.bkrepo.auth.constant.AUTH_ROLE_PREFIX
import com.tencent.bkrepo.auth.constant.AUTH_SERVICE_ROLE_PREFIX
import com.tencent.bkrepo.auth.constant.SERVICE_NAME
import com.tencent.bkrepo.auth.pojo.CreateRoleRequest
import com.tencent.bkrepo.auth.pojo.Role
import com.tencent.bkrepo.auth.pojo.enums.RoleType
import com.tencent.bkrepo.common.api.pojo.Response
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import org.springframework.cloud.openfeign.FeignClient
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam

@Api(tags = ["SERVICE_ROLE"], description = "服务-角色接口")
@FeignClient(SERVICE_NAME, contextId = "ServiceRoleResource")
@RequestMapping(AUTH_ROLE_PREFIX, AUTH_SERVICE_ROLE_PREFIX, AUTH_API_ROLE_PREFIX)
interface ServiceRoleResource {
    @ApiOperation("创建角色")
    @PostMapping("/create")
    fun createRole(
        @RequestBody request: CreateRoleRequest
    ): Response<String?>

    @ApiOperation("创建项目管理员")
    @PostMapping("/create/project/manage/{projectId}")
    fun createProjectManage(
        @ApiParam(value = "仓库名称")
        @PathVariable projectId: String
    ): Response<String?>

    @ApiOperation("创建仓库管理员")
    @PostMapping("/create/repo/manage/{projectId}/{repoName}")
    fun createRepoManage(
        @ApiParam(value = "仓库ID")
        @PathVariable projectId: String,
        @ApiParam(value = "项目ID")
        @PathVariable repoName: String
    ): Response<String?>

    @ApiOperation("删除角色")
    @DeleteMapping("/delete/{id}")
    fun deleteRole(
        @ApiParam(value = "角色主键id")
        @PathVariable id: String
    ): Response<Boolean>

    @ApiOperation("根据主键id查询角色详情")
    @GetMapping("/detail/{id}")
    fun detail(
        @ApiParam(value = "角色主键id")
        @PathVariable id: String
    ): Response<Role?>

    @ApiOperation("根据角色ID与项目Id查询角色")
    @GetMapping("/detail/{rid}/{projectId}")
    fun detailByRidAndProjectId(
        @ApiParam(value = "角色id")
        @PathVariable rid: String,
        @ApiParam(value = "项目id")
        @PathVariable projectId: String
    ): Response<Role?>

    @ApiOperation("根据角色ID与项目Id,仓库名查询角色")
    @GetMapping("/detail/{rid}/{projectId}/{repoName}")
    fun detailByRidAndProjectIdAndRepoName(
        @ApiParam(value = "角色id")
        @PathVariable rid: String,
        @ApiParam(value = "项目id")
        @PathVariable projectId: String,
        @ApiParam(value = "仓库名")
        @PathVariable repoName: String
    ): Response<Role?>

    @ApiOperation("根据类型和项目id查询角色")
    @GetMapping("/list")
    fun listRole(
        @ApiParam(value = "角色类型")
        @RequestParam type: RoleType? = null,
        @ApiParam(value = "项目ID")
        @RequestParam projectId: String? = null,
        @ApiParam(value = "仓库名")
        @RequestParam repoName: String? = null
    ): Response<List<Role>>
}
