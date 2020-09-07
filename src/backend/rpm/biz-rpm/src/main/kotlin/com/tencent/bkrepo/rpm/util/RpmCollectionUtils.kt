package com.tencent.bkrepo.rpm.util

import com.tencent.bkrepo.repository.pojo.node.NodeInfo

object RpmCollectionUtils {

    fun List<NodeInfo>.filterRpmCustom(set: MutableSet<String>, enabledFileLists: Boolean): List<NodeInfo> {
        val resultList = mutableListOf<NodeInfo>()
        try {
            resultList.add(
                this.first {
                    it.metadata?.get("indexType") == "primary"
                }
            )
            resultList.add(
                this.first {
                    it.metadata?.get("indexType") == "others"
                }
            )
            if (enabledFileLists) {
                resultList.add(
                    this.first {
                        it.metadata?.get("indexType") == "filelists"
                    }
                )
            }
        } catch (noSuchElementException: NoSuchElementException) {
            // todo
            // 仓库中还没有生成索引
        }
        val doubleSet = mutableSetOf<String>()
        for (str in set) {
            doubleSet.add(str)
            doubleSet.add("${str}_gz")
        }

        for (str in doubleSet) {
            try {
                resultList.add(
                    this.first {
                        it.metadata?.get("indexName") == str
                    }
                )
            } catch (noSuchElementException: NoSuchElementException) {
                // todo
                // 用户未上传对应分组文件
            }
        }
        return resultList
    }
}