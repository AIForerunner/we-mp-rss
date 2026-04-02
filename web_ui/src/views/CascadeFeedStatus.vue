<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { getFeedStatus } from '@/api/cascade'
import type { FeedStatus } from '@/api/cascade'
import { Message } from '@arco-design/web-vue'
import { IconRefresh } from '@arco-design/web-vue/es/icon'

const feedStatusColumns = [
  { title: '公众号名称', dataIndex: 'mp_name', ellipsis: true },
  { title: '文章数', dataIndex: 'article_count', width: 80 },
  { title: '更新状态', slotName: 'update_status', width: 100 },
  { title: '最近文章', slotName: 'latest_article_time', width: 160 },
  { title: '最后任务', slotName: 'last_task', width: 120 },
  { title: '执行节点', slotName: 'last_task_node', width: 120 },
  { title: '更新时间', slotName: 'updated_at', width: 160 }
]

const feedStatusList = ref<FeedStatus[]>([])
const totalFeeds = ref(0)
const feedStatusLoading = ref(false)

const feedStatusPagination = reactive({
  limit: 10,
  offset: 0
})

const fetchFeedStatus = async () => {
  try {
    feedStatusLoading.value = true
    const res = await getFeedStatus({
      limit: feedStatusPagination.limit,
      offset: feedStatusPagination.offset
    })
    feedStatusList.value = res?.list || []
    totalFeeds.value = res?.total || 0
  } catch (err) {
    console.error('获取公众号状态失败:', err)
    Message.error('获取公众号状态失败')
  } finally {
    feedStatusLoading.value = false
  }
}

const handleFeedStatusPageChange = (page: number) => {
  feedStatusPagination.offset = (page - 1) * feedStatusPagination.limit
  fetchFeedStatus()
}

const formatDate = (dateStr: string | undefined) => {
  if (!dateStr) return '-'
  try {
    const date = new Date(dateStr)
    return date.toLocaleString('zh-CN')
  } catch {
    return '-'
  }
}

const getUpdateStatusColor = (status: string) => {
  const map: Record<string, string> = {
    fresh: 'green',
    recent: 'blue',
    stale: 'orange',
    outdated: 'red',
    unknown: 'gray'
  }
  return map[status] || 'gray'
}

const getUpdateStatusText = (status: string) => {
  const map: Record<string, string> = {
    fresh: '最新',
    recent: '较新',
    stale: '陈旧',
    outdated: '过期',
    unknown: '未知'
  }
  return map[status] || status
}

const getAllocationStatusColor = (status: string) => {
  const map: Record<string, string> = {
    pending: 'orange',
    executing: 'blue',
    completed: 'green',
    failed: 'red'
  }
  return map[status] || 'gray'
}

const getAllocationStatusText = (status: string) => {
  const map: Record<string, string> = {
    pending: '待认领',
    claimed: '已认领',
    executing: '执行中',
    completed: '已完成',
    failed: '失败',
    timeout: '超时'
  }
  return map[status] || status
}

onMounted(() => {
  fetchFeedStatus()
})
</script>

<template>
  <div class="cascade-feed-status">
    <a-card title="公众号状态" :bordered="false">
      <template #extra>
        <a-button @click="fetchFeedStatus" :loading="feedStatusLoading">
          <template #icon>
            <icon-refresh />
          </template>
          刷新
        </a-button>
      </template>

      <a-alert 
        type="info" 
        show-icon
        style="margin-bottom: 16px;"
      >
        显示系统中所有公众号的更新状态和任务执行情况
      </a-alert>

      <a-table
        :columns="feedStatusColumns"
        :data="feedStatusList"
        :loading="feedStatusLoading"
        row-key="id"
        :pagination="{
          current: Math.floor(feedStatusPagination.offset / feedStatusPagination.limit) + 1,
          pageSize: feedStatusPagination.limit,
          total: totalFeeds,
          showTotal: true,
          onChange: handleFeedStatusPageChange
        }"
      >
        <template #update_status="{ record }">
          <a-tag :color="getUpdateStatusColor(record.update_status)">
            {{ getUpdateStatusText(record.update_status) }}
          </a-tag>
        </template>

        <template #latest_article_time="{ record }">
          {{ formatDate(record.latest_article_time) }}
        </template>

        <template #last_task="{ record }">
          <template v-if="record.last_task">
            <a-tag :color="getAllocationStatusColor(record.last_task.status)" size="small">
              {{ getAllocationStatusText(record.last_task.status) }}
            </a-tag>
          </template>
          <span v-else style="color: #999;">-</span>
        </template>

        <template #last_task_node="{ record }">
          <span v-if="record.last_task?.node_name" style="color: #165dff;">
            {{ record.last_task.node_name }}
          </span>
          <span v-else-if="record.last_task?.node_id" style="color: #999;">
            {{ record.last_task.node_id.substring(0, 8) }}...
          </span>
          <span v-else style="color: #999;">-</span>
        </template>

        <template #updated_at="{ record }">
          {{ formatDate(record.updated_at) }}
        </template>
      </a-table>

      <a-empty v-if="!feedStatusLoading && feedStatusList.length === 0" description="暂无公众号数据" />
    </a-card>
  </div>
</template>

<style scoped>
.cascade-feed-status {
  padding: 20px;
}
</style>
