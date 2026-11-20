<template>
  <section class="interval-panel" data-testid="interval-panel">
    <header class="interval-head">
      <div>
        <h3>回填区间留档</h3>
        <p class="interval-sub">
          接单时按源实例 + 两个已确认端点留档；重开后只领取未覆盖日志，
          延伸段和原范围分开标注归属。
        </p>
      </div>
      <div class="interval-head-actions">
        <label class="interval-pick">
          <span>任务</span>
          <select v-model.number="selectedId" data-testid="interval-task-select" @change="loadRecord">
            <option :value="0">请选择回填任务</option>
            <option v-for="row in rows" :key="row.id" :value="Number(row.id)">
              #{{ row.id }} {{ row['任务名称'] }}（{{ row.status }}）
            </option>
          </select>
        </label>
        <button class="btn" data-testid="interval-refresh" type="button" @click="loadRecord">
          刷新留档
        </button>
      </div>
    </header>

    <p v-if="message" class="interval-message" data-testid="interval-message">{{ message }}</p>
    <p v-if="error" class="error-text" data-testid="interval-error">{{ error }}</p>

    <template v-if="record">
      <div class="interval-grid">
        <article class="interval-card">
          <span class="interval-label">源实例 / Binlog</span>
          <strong>{{ record.instance }}:{{ record.binlog_name }}</strong>
        </article>
        <article class="interval-card">
          <span class="interval-label">接单原范围（不可变）</span>
          <strong data-testid="original-range">{{ record.original_range.label }}</strong>
        </article>
        <article class="interval-card">
          <span class="interval-label">已确认范围</span>
          <strong data-testid="confirmed-range">{{ record.confirmed_range.label }}</strong>
        </article>
        <article class="interval-card">
          <span class="interval-label">剩余可领取</span>
          <strong data-testid="claimable-length">{{ record.claimable_length }} 字节</strong>
          <span v-if="record.pending_unconfirmed" class="interval-hint">
            另有 {{ record.pending_unconfirmed }} 字节在接单范围内、尚未被端点确认
          </span>
        </article>
        <article class="interval-card">
          <span class="interval-label">留档版本 / 重开次数</span>
          <strong>{{ record.version }} / {{ record.reopen_count }}</strong>
        </article>
        <article class="interval-card">
          <span class="interval-label">保存一致性</span>
          <strong :class="record.consistency.ok ? 'ok-text' : 'error-text'">
            {{ record.consistency.message }}
          </strong>
        </article>
      </div>

      <div class="interval-actions">
        <button class="btn" data-testid="interval-reopen" type="button" @click="reopen">
          重新打开回填
        </button>
        <button class="btn primary" data-testid="interval-claim" type="button" @click="claim">
          领取下一段
        </button>
        <form class="interval-confirm" @submit.prevent="extendEndpoint">
          <label>
            <span>结束端点</span>
            <input v-model="endpointValue" data-testid="interval-endpoint" placeholder="例如 1200" />
          </label>
          <label>
            <span>期望端点（可选）</span>
            <input v-model="expectedValue" data-testid="interval-expected" placeholder="乐观并发校验" />
          </label>
          <button class="btn" data-testid="interval-confirm-endpoint" type="submit">
            确认端点
          </button>
        </form>
      </div>

      <div class="interval-columns">
        <section class="interval-block">
          <h4>已领取区间</h4>
          <table class="data-table compact">
            <thead><tr><th>区间</th><th>归属</th><th>批次</th><th>领取时间</th></tr></thead>
            <tbody>
              <tr v-for="claim in record.claims" :key="claim.id" :data-testid="`claim-${claim.id}`">
                <td>{{ claim.range }}</td>
                <td :class="kindClass(claim.segment_kind)">{{ claim.segment_kind }}</td>
                <td>{{ claim.batch_no }}</td>
                <td>{{ claim.claimed_at }}</td>
              </tr>
              <tr v-if="!record.claims.length">
                <td colspan="4" class="empty-state">还没有领取记录</td>
              </tr>
            </tbody>
          </table>
        </section>

        <section class="interval-block">
          <h4>剩余可领取（按归属拆分）</h4>
          <table class="data-table compact">
            <thead><tr><th>区间</th><th>归属</th><th>长度</th></tr></thead>
            <tbody>
              <tr v-for="seg in record.claimable" :key="`${seg.range}-${seg.segment_kind}`">
                <td data-testid="claimable-range">{{ seg.range }}</td>
                <td :class="kindClass(seg.segment_kind)">{{ seg.segment_kind }}</td>
                <td>{{ seg.length }}</td>
              </tr>
              <tr v-if="!record.claimable.length">
                <td colspan="3" class="empty-state">没有剩余日志</td>
              </tr>
            </tbody>
          </table>
        </section>
      </div>

      <section class="interval-block">
        <h4>端点确认记录</h4>
        <table class="data-table compact">
          <thead><tr><th>#</th><th>提交区间</th><th>接纳区间</th><th>来源</th><th>确认时间</th><th>状态</th></tr></thead>
          <tbody>
            <tr v-for="item in record.confirmations" :key="item.id">
              <td>{{ item.id }}</td>
              <td>{{ item.range }}</td>
              <td>{{ item.accepted_start === null ? '—' : `${item.accepted_start}..${item.accepted_end}` }}</td>
              <td>{{ item.source }}</td>
              <td>{{ item.confirmed_at }}</td>
              <td>{{ item.status }}</td>
            </tr>
            <tr v-if="!record.confirmations.length">
              <td colspan="6" class="empty-state">还没有端点确认记录</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="interval-block">
        <h4>留档版本历史</h4>
        <ol class="interval-history">
          <li v-for="item in record.history" :key="item.version">
            <strong>v{{ item.version }}</strong>
            <span class="interval-reason">{{ item.reason }}</span>
            <span>{{ item.saved_at }}</span>
            <span>原范围 {{ item.original_range }}</span>
            <span>确认 {{ item.confirmed_range }}</span>
            <span v-if="item.claim_ranges.length">已领 {{ item.claim_ranges.join('、') }}</span>
          </li>
        </ol>
      </section>
    </template>

    <p v-else-if="selectedId" class="empty-state" data-testid="interval-empty">
      该任务还没有回填区间留档，先执行「开始回填」。
    </p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'

import {
  fetchIntervalRecord,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow, IntervalRecord } from '@/data/types'

const props = defineProps<{ rows: EntryRow[]; taskId?: number }>()
const emit = defineEmits<{ (event: 'changed', row: EntryRow | null): void }>()

const selectedId = ref<number>(props.taskId ?? 0)
const record = ref<IntervalRecord | null>(null)
const message = ref('')
const error = ref('')
const endpointValue = ref('')
const expectedValue = ref('')

// 首次进入时如果还没有选中任务，默认落在第一个已回填任务上，值班人员
// 打开「回填位点」就能看到最近一份接单留档。
watch(
  () => props.rows.length,
  () => {
    if (!selectedId.value && props.rows.length) {
      selectedId.value = Number(props.rows[0].id)
      void loadRecord()
    }
  },
  { immediate: true },
)

watch(
  () => props.taskId,
  (value) => {
    if (value && value !== selectedId.value) {
      selectedId.value = value
      void loadRecord()
    }
  },
)

onMounted(() => {
  if (selectedId.value) void loadRecord()
})

function kindClass(kind: string) {
  if (kind === '延伸段') return 'kind-extension'
  if (kind === '前置段') return 'kind-prefix'
  return 'kind-original'
}

async function loadRecord() {
  error.value = ''
  message.value = ''
  if (!selectedId.value) {
    record.value = null
    return
  }
  try {
    record.value = await fetchIntervalRecord(selectedId.value)
  } catch (err) {
    error.value = err instanceof Error ? err.message : '留档读取失败'
    record.value = null
  }
}

async function submit(action: string) {
  if (!selectedId.value) return
  error.value = ''
  message.value = ''
  try {
    const result = await applyAction('backfill_position', selectedId.value, action)
    message.value = result.message
    if (!result.ok) {
      error.value = result.message
      message.value = ''
      return
    }
    const changed = props.rows.find((row) => Number(row.id) === selectedId.value) ?? null
    if (changed && result.entry) Object.assign(changed, result.entry)
    emit('changed', changed)
    await loadRecord()
  } catch (err) {
    error.value = err instanceof Error ? err.message : '操作失败'
  }
}

function reopen() {
  return submit('重新打开回填')
}

function claim() {
  return submit('领取下一段')
}

async function extendEndpoint() {
  if (!selectedId.value || !endpointValue.value.trim()) return
  error.value = ''
  message.value = ''
  try {
    const values: Record<string, string> = { 结束端点: endpointValue.value.trim() }
    if (expectedValue.value.trim()) values.期望端点 = expectedValue.value.trim()
    const result = await applyAction(
      'backfill_position', selectedId.value, '确认端点', values)
    message.value = result.message
    if (!result.ok) {
      error.value = result.message
      message.value = ''
      return
    }
    endpointValue.value = ''
    expectedValue.value = ''
    await loadRecord()
  } catch (err) {
    error.value = err instanceof Error ? err.message : '端点确认失败'
  }
}
</script>
