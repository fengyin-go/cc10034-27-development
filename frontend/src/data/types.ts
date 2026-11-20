/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
  entry?: EntryRow | null
  details?: Record<string, unknown> | null
}

export type IntervalSegment = {
  start_pos: number
  end_pos: number
  range: string
  length: number
  segment_kind: string
  归属: string
}

export type IntervalClaim = IntervalSegment & {
  id: number
  task_id: number | null
  batch_no: number
  claimed_at: string
  status: string
}

export type EndpointConfirmation = {
  id: number
  instance: string
  binlog_name: string
  start_pos: number
  end_pos: number
  accepted_start: number | null
  accepted_end: number | null
  range: string
  confirmed_at: string
  source: string
  status: string
  note: string
}

export type IntervalRecord = {
  id: number
  task_id: number | null
  task_name: string
  instance: string
  binlog_name: string
  range_start: number
  range_end: number
  confirmed_start: number
  confirmed_end: number
  version: number
  status: string
  created_at: string
  updated_at: string
  reopened_at: string | null
  reopen_count: number
  original_range: { start: number; end: number; label: string }
  confirmed_range: { start: number; end: number; label: string }
  claimed_ranges: { start: number; end: number; range: string; segment_kind: string }[]
  claimed_length: number
  claimable: IntervalSegment[]
  claimable_length: number
  pending_unconfirmed: number
  claims: IntervalClaim[]
  confirmations: EndpointConfirmation[]
  consistency: { ok: boolean; message: string; confirmation_count: number }
  history: {
    version: number
    saved_at: string
    reason: string
    status: string
    original_range: string
    confirmed_range: string
    claim_ranges: string[]
    accepted?: string | null
    claimed?: string[]
  }[]
}


export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}
