import { defineStore } from 'pinia'
import { ref, computed, shallowRef, watch } from 'vue'
import { createTransport, type Transport } from '@/bridge'
import { authedFetch } from '@/bridge/http'
import { isDemoMode } from '@/bridge/demoMode'
import type { AgentEvent } from '@/protocol/events'
import type { ReasoningEffortStats } from '@/protocol/events'
import type { ReasoningEffort } from '@/protocol/commands'
import type { InboundEnvelope } from '@/protocol/commands'
import { nextId } from '@/bridge/envelope'
import { useConnectionStore } from './connection'
import { useConfigStore } from './config'
import { useSessionsStore } from './sessions'
import { router } from '@/router'
import type { AssistantMessage, LoopProgress, ManualCard, QuestionCard, ReasoningBlock, RunState, Timelineitem, ToolCard, ToolDiagnosticTrace } from './viewModel'

export const useSessionStore = defineStore('session', () => {
  const connection = useConnectionStore()
  const config = useConfigStore()
  const sessions = useSessionsStore()

  const sessionId = ref(readActiveSessionId())
  const timeline = ref<Timelineitem[]>(sessions.loadTranscript(sessionId.value))
  const runState = ref<RunState>('idle')
  const usage = ref<{
    total: number; input: number; output: number; contextRatio: number
    contextUsed: number; contextWindow: number
    cachedInput: number; cacheHitRate: number | null; cacheDataAvailable: boolean
    turnCacheHitRate: number | null; turnCacheDataAvailable: boolean
    reasoningEfforts: Partial<Record<ReasoningEffort, ReasoningEffortStats>>
    contextCategories: Partial<Record<'system_tools' | 'messages' | 'skills' | 'mcp_tools' | 'system_prompt' | 'other', number>>
  } | null>(null)
  const retryConfirmation = ref<string | null>(null)
  /** 当前会话的工作目录（会话标记路径，绑定为会话执行目录）。 */
  const cwd = ref('')
  const loop = ref<LoopProgress>({ active: false, currentStep: 0, totalSteps: 0, status: '' })

  let currentAssistant: AssistantMessage | null = null
  let connectedSessionId = ''
  let persistTimer: ReturnType<typeof setTimeout> | null = null
  let turnToolTrace: ToolDiagnosticTrace[] = []
  let consecutiveToolFailures = 0
  let maxConsecutiveToolFailures = 0
  const transport = shallowRef<Transport | null>(null)

  const isBusy = computed(() => runState.value !== 'idle')
  const pendingApproval = computed(() => timeline.value.find((t): t is ToolCard => t.kind === 'tool' && t.status === 'awaiting_approval'))
  const pendingQuestion = computed(() => timeline.value.find((t): t is QuestionCard => t.kind === 'question' && !t.answered))

  persistActiveSessionId(sessionId.value)

  // Native task status is derived from /api/tasks in the sessions store. A
  // foreground idle session must not overwrite another session's running state.

  /** 发送内置引导（EmptyState 引导卡）：先置用户标题消息，再让引擎流式推正文。 */
  const GUIDE_TITLES: Record<string, string> = {
    newbie: 'Coomi 新手使用指南',
    extension: '自定义拓展进化指南',
  }
  function sendGuide(key: string) {
    const trimmed = (GUIDE_TITLES[key] ?? 'Coomi 指南').trim()
    // 首条用户消息作为会话标题，抽屉里就不会全是「新对话」。
    const isFirst = !timeline.value.some(t => t.kind === 'user')
    if (isFirst) sessions.touch(sessionId.value, { title: sessions.deriveTitle(trimmed) })
    timeline.value.push({ kind: 'user', id: nextId(), content: trimmed })
    runState.value = 'thinking'
    transport.value?.send({ command: 'send_guide', key })
    persistSoon()
  }

  /** 时间线写回 localStorage 有节流：流式期间不要每个 chunk 都序列化。 */
  function persistSoon() {
    if (isDemoMode()) return // 演示内容不该混进真实历史
    if (persistTimer) return
    persistTimer = setTimeout(() => {
      persistTimer = null
      const items = timeline.value.filter(t => t.kind !== 'notice')
      if (items.length === 0) return
      sessions.touch(sessionId.value, { turns: timeline.value.filter(t => t.kind === 'user').length })
      sessions.saveTranscript(sessionId.value, timeline.value)
    }, 1200)
  }

  function flushPersistence() {
    if (persistTimer) {
      clearTimeout(persistTimer)
      persistTimer = null
    }
    if (isDemoMode()) return
    const items = timeline.value.filter(t => t.kind !== 'notice')
    if (items.length === 0) return
    sessions.touch(sessionId.value, { turns: timeline.value.filter(t => t.kind === 'user').length })
    sessions.saveTranscript(sessionId.value, timeline.value)
  }

  /** 换 sessionId 后必须重连：WS 的路径里带着 session id。 */
  function connect(wsUrl?: string) {
    if (transport.value && connectedSessionId === sessionId.value) return
    const targetSessionId = sessionId.value
    const previous = transport.value
    transport.value = null
    connectedSessionId = ''
    previous?.close()
    if (wsUrl) connection.setWsUrl(wsUrl)
    const t = createTransport(targetSessionId, wsUrl)
    transport.value = t
    connectedSessionId = targetSessionId
    let lastEventSeq = 0
    t.onStateChange(status => {
      if (transport.value !== t || sessionId.value !== targetSessionId) return
      connection.setStatus(status)
      if (status.state === 'open') {
        t.send({ command: 'set_permission_mode', mode: config.permissionMode })
        if (config.currentProviderId && config.currentModel) {
          t.send({ command: 'select_model', provider_id: config.currentProviderId, model: config.currentModel })
        }
        t.send({ command: 'set_reasoning_effort', effort: config.reasoningEffort })
        t.send({ command: 'set_max_tool_rounds', rounds: config.maxToolRounds })
      }
    })
    t.onMessage(env => {
      if (transport.value !== t || sessionId.value !== targetSessionId) return
      if (env.type === 'event' && env.payload.event_seq) {
        const seq = env.payload.event_seq
        if (seq <= lastEventSeq) {
          t.send({ command: 'ack_event', event_seq: seq })
          return
        }
        lastEventSeq = seq
        onInbound(env)
        t.send({ command: 'ack_event', event_seq: seq })
        return
      }
      onInbound(env)
    })
    t.connect()
  }

  function disconnect() { transport.value?.close(); transport.value = null; connectedSessionId = '' }

  function onInbound(env: InboundEnvelope) {
    if (env.type === 'event') applyEvent(env.payload)
    else if (env.type === 'error') pushNotice('error', env.payload.message)
  }

  function applyEvent(ev: AgentEvent) {
    switch (ev.event_type) {
      // 兜底：turn_end 之后又开始吐字（引擎续了一轮），状态得跟着回到忙。
      case 'text_chunk': if (runState.value === 'idle') runState.value = 'thinking'; appendAssistant(ev.content); break
      case 'reasoning_chunk': if (runState.value === 'idle') runState.value = 'thinking'; appendReasoning(ev.content); break
      case 'tool_start':
        endAssistantStream()
        timeline.value.push({ kind: 'tool', callId: ev.call_id, toolName: ev.tool_name, arguments: ev.arguments, status: 'starting', expanded: ev.tool_name === 'show_image' })
        turnToolTrace.push({
          callId: ev.call_id,
          sequence: turnToolTrace.length + 1,
          tool: sanitizeToolName(ev.tool_name),
          argumentShape: summarizeArguments(ev.arguments),
          status: 'running',
        })
        runState.value = 'executing'
        break
      case 'tool_running': patchTool(ev.call_id, c => c.status = 'running'); runState.value = 'executing'; break
      case 'tool_done':
        patchTool(ev.call_id, c => {
          c.status = ev.is_error ? 'error' : 'success'
          c.elapsed = ev.elapsed
          c.resultPreview = ev.result_preview
          c.isError = ev.is_error
          // 工具产生的图片：瀑布流渲染（历史恢复时由 messages.images 补回）
          if (Array.isArray(ev.images) && ev.images.length > 0) c.images = ev.images
        })
        // 工具跑完不等于一轮结束 —— 模型接着想下一步。回 idle 只认 turn_end /
        // 取消 / 致命错误，否则输入区会在循环中途闪回「下达任务」和发送箭头。
        runState.value = 'thinking'
        {
          const trace = turnToolTrace.find(item => item.callId === ev.call_id)
          if (trace) {
            trace.status = ev.is_error ? 'error' : 'success'
            trace.elapsedMs = Math.max(0, Math.round(ev.elapsed * 1000))
            if (ev.is_error) {
              consecutiveToolFailures += 1
              maxConsecutiveToolFailures = Math.max(maxConsecutiveToolFailures, consecutiveToolFailures)
              trace.category = classifyToolError(ev.result_preview)
              trace.errorSummary = sanitizeDiagnosticText(ev.result_preview)
            } else consecutiveToolFailures = 0
          }
        }
        break
      case 'tool_cache_hit':
        patchTool(ev.call_id, c => c.status = 'cache_hit')
        {
          const trace = turnToolTrace.find(item => item.callId === ev.call_id)
          if (trace) trace.status = 'success'
          consecutiveToolFailures = 0
        }
        break
      case 'tool_approval_request':
        endAssistantStream()
        if (!patchTool(ev.call_id, c => { c.status = 'awaiting_approval'; c.access = ev.access; c.riskSummary = ev.risk_summary; c.expanded = true })) {
          timeline.value.push({ kind: 'tool', callId: ev.call_id, toolName: ev.tool_name, arguments: ev.arguments, status: 'awaiting_approval', access: ev.access, riskSummary: ev.risk_summary, expanded: true })
        }
        runState.value = 'awaiting_approval'
        break
      case 'user_question_request':
        endAssistantStream()
        timeline.value.push({ kind: 'question', callId: ev.call_id, questions: ev.questions, answered: false })
        runState.value = 'awaiting_question'
        break
      case 'file_transfer_request':
        if (ev.operation === 'import') {
          window.CoomiAndroid?.importFilesForRequest?.(ev.request_id)
        } else if (ev.path) {
          window.CoomiAndroid?.exportFileForRequest?.(
            ev.request_id,
            ev.path,
            ev.suggested_name ?? ev.path.split('/').pop() ?? 'coomi-export',
          )
        }
        break
      case 'usage_update': {
        const previous = usage.value
        usage.value = {
          total: ev.usage.total_tokens ?? previous?.total ?? 0,
          input: ev.usage.input_tokens ?? previous?.input ?? 0,
          output: ev.usage.output_tokens ?? previous?.output ?? 0,
          contextRatio: ev.usage.context_ratio ?? previous?.contextRatio ?? 0,
          contextUsed: ev.usage.context_used_tokens ?? previous?.contextUsed ?? 0,
          contextWindow: ev.usage.context_window_tokens ?? previous?.contextWindow ?? 0,
          cachedInput: ev.usage.cached_input_tokens ?? previous?.cachedInput ?? 0,
          cacheHitRate: ev.usage.cache_hit_rate ?? previous?.cacheHitRate ?? null,
          cacheDataAvailable: ev.usage.cache_data_available ?? previous?.cacheDataAvailable ?? false,
          turnCacheHitRate: ev.usage.turn_cache_hit_rate ?? previous?.turnCacheHitRate ?? null,
          turnCacheDataAvailable: ev.usage.turn_cache_data_available ?? previous?.turnCacheDataAvailable ?? false,
          reasoningEfforts: ev.reasoning_efforts ?? previous?.reasoningEfforts ?? {},
          contextCategories: ev.context_categories ?? previous?.contextCategories ?? {},
        }
        break
      }
      case 'compression': pushNotice('info', `上下文已压缩 ${fmtTokens(ev.before)} → ${fmtTokens(ev.after)}`); break
      case 'connection_retry': connection.setRetry(`${ev.message}（${ev.attempt}/${ev.max_attempts}）`); break
      case 'stream_reset':
        endAssistantStream()
        while (timeline.value.length > 0) {
          const last = timeline.value[timeline.value.length - 1]
          if (last.kind === 'assistant' || last.kind === 'reasoning') timeline.value.pop()
          else break
        }
        break
      case 'retry_confirmation':
        endAssistantStream()
        runState.value = 'idle'
        retryConfirmation.value = ev.message
        break
      case 'agent_error': endAssistantStream(); pushNotice('error', ev.message); if (ev.is_fatal) runState.value = 'idle'; persistSoon(); break
      case 'configuration_required': endAssistantStream(); runState.value = 'idle'; pushNotice('warn', ev.message); void router.push(ev.route); break
      case 'agent_cancelled': endAssistantStream(); cancelRunningTools(); pushNotice('warn', '已停止本轮执行'); break
      case 'bg_task_detached': pushNotice('info', `↪ 已转入后台任务 #${ev.task_id}（${ev.tool_name}）`); break
      case 'bg_task_completed': pushNotice(ev.is_error ? 'error' : 'success', `${ev.is_error ? '✕' : '✓'} 后台任务 #${ev.task_id} ${ev.is_error ? '失败' : '完成'}`); break
      case 'loop_progress':
        loop.value = { active: ev.status !== 'done', currentStep: ev.current_step, totalSteps: ev.total_steps, status: ev.status, currentDescription: loop.value.currentDescription }
        break
      case 'loop_step_start':
        loop.value = { ...loop.value, active: true, totalSteps: ev.total_steps, currentStep: ev.step_index, currentDescription: ev.step_description }
        break
      case 'manual_request':
        // 人工模式：引擎拼好提示词，等用户复制 → 粘贴外部 AI 回答。
        endAssistantStream()
        runState.value = 'awaiting_manual'
        // 重连补发 / 重复推送按 seq 去重：同一请求只保留一张卡片。
        {
          const id = 'manual-' + ev.seq
          const existing = timeline.value.find(t => t.kind === 'manual' && t.id === id)
          if (existing && existing.kind === 'manual') {
            existing.prompt = ev.prompt
            if (existing.status === 'submitted') {
              existing.status = 'awaiting'
              existing.response = undefined
            }
          } else {
            timeline.value.push({ kind: 'manual', id, seq: ev.seq, prompt: ev.prompt, status: 'awaiting' })
          }
        }
        persistSoon()
        break
      case 'manual_warning':
        pushNotice('warn', ev.message)
        persistSoon()
        break
      case 'turn_end':
        endAssistantStream(); cancelRunningTools(); connection.setRetry(null); runState.value = 'idle'
        // 人工模式：本轮结束时仍处于「等待粘贴」的卡片收尾为已取消（停止/中断场景）。
        for (const item of timeline.value) {
          if (item.kind === 'manual' && item.status === 'awaiting') item.status = 'cancelled'
        }
        {
          const failures = turnToolTrace.filter(item => item.status === 'error').length
          if (maxConsecutiveToolFailures >= 3) {
            const trace = turnToolTrace.map(item => ({ ...item, callId: undefined }))
            const noticeId = nextId()
            timeline.value.push({
              kind: 'notice', id: noticeId, tone: 'warn', analysisStatus: 'consent', feedbackEligible: true,
              text: `同一任务链连续 ${maxConsecutiveToolFailures} 次工具调用未恢复，建议反馈脱敏错误记录。`,
              analysisTrace: trace,
              failureCount: failures,
            })
          }
        }
        turnToolTrace = []
        consecutiveToolFailures = 0
        maxConsecutiveToolFailures = 0
        persistSoon()
        break
      case 'session_state': {
        // 重连后引擎告知本会话是否仍在后台执行（切走会话后任务继续跑）。
        sessions.refreshRunning()
        runState.value = ev.running ? 'thinking' : 'idle'
        break
      }
      case 'session_loaded': {
        // 打开历史会话时，引擎把持久化的累计用量推过来，避免显示 0。
        const u = ev.usage ?? {}
        usage.value = {
          total: u.total_tokens ?? usage.value?.total ?? 0,
          input: u.input_tokens ?? usage.value?.input ?? 0,
          output: u.output_tokens ?? usage.value?.output ?? 0,
          contextRatio: usage.value?.contextRatio ?? 0,
          contextUsed: usage.value?.contextUsed ?? 0,
          contextWindow: usage.value?.contextWindow ?? 0,
          cachedInput: usage.value?.cachedInput ?? 0,
          cacheHitRate: usage.value?.cacheHitRate ?? null,
          cacheDataAvailable: usage.value?.cacheDataAvailable ?? false,
          turnCacheHitRate: usage.value?.turnCacheHitRate ?? null,
          turnCacheDataAvailable: usage.value?.turnCacheDataAvailable ?? false,
          reasoningEfforts: usage.value?.reasoningEfforts ?? {},
          contextCategories: usage.value?.contextCategories ?? {},
        }
        if (typeof ev.cwd === 'string' && ev.cwd) cwd.value = ev.cwd
        break
      }
    }
  }

  function activateSession(id: string) {
    sessionId.value = id
    persistActiveSessionId(id)
  }

  function retryInterruptedTurn() {
    retryConfirmation.value = null
    runState.value = 'thinking'
    transport.value?.send({ command: 'retry_turn' })
  }

  function dismissRetry() { retryConfirmation.value = null }

  function setReasoningEffort(effort: ReasoningEffort) {
    config.setReasoningEffort(effort)
    transport.value?.send({ command: 'set_reasoning_effort', effort })
  }

  function setMaxToolRounds(rounds: number) {
    config.setMaxToolRounds(rounds)
    transport.value?.send({ command: 'set_max_tool_rounds', rounds: config.maxToolRounds })
  }

  function cancelRunningTools() {
    // 停止后引擎可能不会逐个补发 tool_done：把仍在运行/准备中的工具卡片
    // 收尾为「已取消」，否则卡片会永远停在旋转的「运行中」状态。
    let changed = false
    for (const item of timeline.value) {
      if (item.kind === 'tool' && (item.status === 'running' || item.status === 'starting')) {
        item.status = 'cancelled'
        item.isError = true
        changed = true
      }
    }
    if (changed) persistSoon()
  }

  function sendMessage(text: string) {
    const trimmed = text.trim()
    if (!trimmed) return
    // 首条用户消息作为会话标题，抽屉里就不会全是「新对话」。
    const isFirst = !timeline.value.some(t => t.kind === 'user')
    if (isFirst) sessions.touch(sessionId.value, { title: sessions.deriveTitle(trimmed) })
    if (isBusy.value) {
      timeline.value.push({ kind: 'user', id: nextId(), content: trimmed })
      transport.value?.send({ command: 'jump_in', text: trimmed })
      persistSoon()
      return
    }
    turnToolTrace = []
    timeline.value.push({ kind: 'user', id: nextId(), content: trimmed })
    runState.value = 'thinking'
    transport.value?.send({ command: 'send_message', text: trimmed })
    persistSoon()
  }

  function cancel() { transport.value?.send({ command: 'cancel' }) }
  function approve(callId: string, decision: 'allow' | 'deny' | 'always') {
    patchTool(callId, c => { c.status = decision === 'deny' ? 'error' : 'running'; if (decision === 'deny') { c.resultPreview = '（用户拒绝执行）'; c.isError = true } })
    transport.value?.send({ command: 'approve_tool', call_id: callId, decision })
    if (runState.value === 'awaiting_approval') runState.value = 'executing'
  }
  function answerQuestion(callId: string, answers: Record<string, string>) {
    patchQuestion(callId, q => { q.answered = true; q.answers = answers })
    transport.value?.send({ command: 'answer_question', call_id: callId, answers })
    if (runState.value === 'awaiting_question') runState.value = 'thinking'
  }
  function setPermissionMode(mode: 'ask' | 'auto' | 'full') { config.setPermissionMode(mode); transport.value?.send({ command: 'set_permission_mode', mode }) }
  function togglePlanMode() { const entering = !config.planMode; config.togglePlanMode(); transport.value?.send({ command: entering ? 'enter_plan_mode' : 'exit_plan_mode' }) }
  function selectModel(providerId: string, model: string) { config.selectModel(providerId, model); transport.value?.send({ command: 'select_model', provider_id: providerId, model }) }
  function completeFileTransfer(requestId: string, paths: string[]) {
    transport.value?.send({ command: 'file_transfer_result', request_id: requestId, paths })
  }

  /** 人工模式：提交从外部 AI 粘贴回来的回答，交给引擎解析并执行工具。 */
  function submitManualResponse(cardId: string, text: string) {
    const trimmed = text.trim()
    if (!trimmed) return
    const card = timeline.value.find(t => t.kind === 'manual' && t.id === cardId)
    if (card && card.kind === 'manual') {
      card.status = 'submitted'
      card.response = trimmed
    }
    runState.value = 'thinking'
    transport.value?.send({ command: 'manual_response', text: trimmed })
    persistSoon()
  }

  function newSession() {
    flushPersistence()
    endAssistantStream(); timeline.value = []; usage.value = null
    loop.value = { active: false, currentStep: 0, totalSteps: 0, status: '' }; runState.value = 'idle'
    activateSession(createSessionId())
    connect()
  }

  /** 从引擎 /api/sessions/{id} 恢复完整历史；成功返回 true。 */
  async function restoreFromEngine(id: string): Promise<boolean> {
    try {
      const res = await authedFetch(`/api/sessions/${id}`)
      if (!res.ok) return false
      const session = await res.json()
      const messages = (session.messages ?? []) as ChatMessageJson[]
      if (messages.length === 0) return false
      if (messages.some(m => m.compaction_summary)) {
        // 上下文已压缩：引擎只剩摘要 + 截断的部分历史。前端从未收到压缩版，
        // 本机 localStorage 缓存仍是完整时间线 —— 优先用它恢复展示，
        // 并把压缩摘要折叠成一条提示附在末尾。
        const cached = sessions.loadTranscript(id)
        if (cached && cached.length > 0) {
          const detail = messages.find(m => m.compaction_summary)?.content ?? ''
          timeline.value = [
            ...cached,
            { kind: 'notice', id: nextId(), tone: 'info', text: '（上下文已压缩 · 点击查看摘要）', detail },
          ]
          return true
        }
      }
      timeline.value = messagesToTimeline(messages)
      return true
    } catch {
      return false
    }
  }

  /** 把引擎磁盘会话消息转换为前端时间线（含工具调用卡片与结果回填）。 */
  function messagesToTimeline(messages: ChatMessageJson[]): Timelineitem[] {
    const items: Timelineitem[] = []
    const toolResults = new Map<string, string>()
    const toolImages = new Map<string, string[]>()
    for (const m of messages) {
      if (m.internal) continue
      if (m.compaction_summary) {
        items.push({ kind: 'notice', id: nextId(), tone: 'info', text: '（上下文已压缩 · 点击查看摘要）', detail: m.content ?? '' })
        continue
      }
      if (m.role === 'user') {
        items.push({ kind: 'user', id: nextId(), content: m.content })
      } else if (m.role === 'assistant') {
        if (m.content) items.push({ kind: 'assistant', id: nextId(), content: m.content, streaming: false })
        for (const tc of m.tool_calls ?? []) {
          items.push({
            kind: 'tool', callId: tc.id, toolName: tc.name,
            arguments: tc.arguments as Record<string, unknown>,
            status: 'success', expanded: tc.name === 'show_image',
            images: (tc.images ?? []).map((img: { media_type: string; data: string }) =>
              `data:${img.media_type};base64,${img.data}`),
          })
        }
      } else if (m.role === 'tool' && m.tool_call_id) {
        toolResults.set(m.tool_call_id, m.content)
        if (m.images?.length) {
          toolImages.set(m.tool_call_id, m.images.map((img: { media_type: string; data: string }) =>
            `data:${img.media_type};base64,${img.data}`))
        }
      }
    }
    for (const item of items) {
      if (item.kind === 'tool') {
        const result = toolResults.get(item.callId)
        if (result != null) {
          const preview = result.length > 200 ? result.slice(0, 200) + '…' : result
          item.resultPreview = preview
          item.isError = /error|fail|exception|panic/i.test(result.slice(0, 500))
        } else {
          // 没有结果回填（比如被取消/未执行）的调用收尾为已取消
          item.status = 'cancelled'
          item.isError = true
        }
        const imgs = toolImages.get(item.callId)
        if (imgs?.length) {
          item.images = imgs
        } else if (item.toolName === 'show_image' && item.expanded && item.status === 'success') {
          // show_image 历史恢复但图片数据不可用（如已被上下文压缩清理）
          item.imageMissing = true
        }
      }
    }
    return items
  }

  /**
   * 打开一条历史会话：优先从引擎磁盘拉完整历史（权威源，修复“会话消失/串话”），
   * 引擎不可用才回退本机 localStorage 记录。
   */
  async function openSession(id: string) {
    if (id === sessionId.value) return
    flushPersistence()
    endAssistantStream()
    usage.value = null
    loop.value = { active: false, currentStep: 0, totalSteps: 0, status: '' }
    runState.value = 'syncing'
    const targetId = isUuid(id) ? id : sessions.migrateId(id, createSessionId())
    activateSession(targetId)
    const restoredFromEngine = await restoreFromEngine(targetId)
    if (!restoredFromEngine) {
      const restored = sessions.loadTranscript(targetId)
      timeline.value = restored
      if (restored.length > 0) {
        timeline.value.push({
          kind: 'notice', id: nextId(), tone: 'info',
          text: '已恢复本机记录。若引擎重启过，模型这边的上下文可能已经清空。',
        })
      }
    }
    connect()
  }

  function deleteSession(id: string) {
    // 先停掉待落盘的持久化定时器：被删会话不应再写回（否则会“复活”成空标题的新会话）。
    if (persistTimer) { clearTimeout(persistTimer); persistTimer = null }
    if (id === sessionId.value) {
      // 删除的是当前会话：先切到新会话并重连（关闭旧 id 的 WS 连接、清空时间线），
      // 避免 flushPersistence 把已删会话写回，也避免引擎在文件删除后重建同 id 会话。
      endAssistantStream(); timeline.value = []; usage.value = null
      loop.value = { active: false, currentStep: 0, totalSteps: 0, status: '' }; runState.value = 'idle'
      activateSession(createSessionId())
      connect()
    }
    sessions.remove(id)
    try { localStorage.removeItem(`coomi.draft.${id}`) } catch { /* ignore */ }
  }

  /** 更新当前会话的工作目录（会话标记路径）。成功后引擎后续 turn 都在该目录执行。 */
  async function setSessionCwd(path: string): Promise<boolean> {
    const id = sessionId.value
    try {
      const res = await authedFetch(`/api/sessions/${id}/cwd`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cwd: path }),
      })
      if (!res.ok) return false
      cwd.value = path
      const meta = sessions.find(id)
      if (meta) { meta.cwd = path; sessions.setCurrentCwd(path) }
      return true
    } catch {
      return false
    }
  }

  function appendAssistant(content: string) {
    if (!currentAssistant) {
      timeline.value.push({ kind: 'assistant', id: nextId(), content: '', streaming: true })
      // 必须拿 push 之后数组里的那个对象：ref 会把它包成代理，
      // 直接改 push 进去的原始对象不触发渲染，流式文本就只会停在第一片。
      currentAssistant = timeline.value[timeline.value.length - 1] as AssistantMessage
    }
    currentAssistant.content += content
  }
  function endAssistantStream() { if (currentAssistant) { currentAssistant.streaming = false; currentAssistant = null } }
  function appendReasoning(content: string) {
    const last = timeline.value[timeline.value.length - 1]
    if (last && last.kind === 'reasoning') { (last as ReasoningBlock).content += content }
    else { timeline.value.push({ kind: 'reasoning', id: nextId(), content, expanded: false }) }
  }
  function patchTool(callId: string, fn: (c: ToolCard) => void): boolean {
    for (let i = timeline.value.length - 1; i >= 0; i--) { const t = timeline.value[i]; if (t.kind === 'tool' && t.callId === callId) { fn(t); return true } }
    return false
  }
  function patchQuestion(callId: string, fn: (q: QuestionCard) => void) {
    for (let i = timeline.value.length - 1; i >= 0; i--) { const t = timeline.value[i]; if (t.kind === 'question' && t.callId === callId) { fn(t); return } }
  }
  function pushNotice(tone: 'info' | 'warn' | 'error' | 'success', text: string) { timeline.value.push({ kind: 'notice', id: nextId(), tone, text }) }

  async function consentToolFailureFeedback(noticeId: string): Promise<boolean> {
    const notice = timeline.value.find(item => item.kind === 'notice' && item.id === noticeId)
    if (notice?.kind !== 'notice' || !notice.analysisTrace?.length) return false
    if (!['consent', 'failed'].includes(notice.analysisStatus ?? '')) return false
    const trace = notice.analysisTrace
    const failureCount = notice.failureCount ?? trace.filter(item => item.status === 'error').length
    updateAnalysisNotice(noticeId, {
      analysisStatus: 'analyzing', feedbackEligible: false, detail: undefined,
      text: '正在后台轻量整理工具调用错误，完成后将自动上传。您可以继续对话。',
    })
    persistSoon()
    try {
      const response = await authedFetch('/api/tool-failure-analysis', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          provider_id: config.currentProviderId,
          trace: trace.map(({ callId: _callId, ...item }) => item),
        }),
      })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const data = await response.json()
      const analysis = typeof data.analysis === 'string' ? data.analysis.trim() : ''
      if (!analysis) throw new Error('empty analysis')
      updateAnalysisNotice(noticeId, {
        analysisStatus: 'ready', feedbackEligible: false,
        text: `已完成 ${failureCount} 次工具失败的脱敏整理，正在自动上传。`,
        detail: `${analysis}\n\n${buildLocalEvidence(trace)}`,
      })
      persistSoon()
      return true
    } catch (error) {
      updateAnalysisNotice(noticeId, {
        analysisStatus: 'failed', feedbackEligible: true, detail: undefined,
        text: `反馈整理失败，未上传任何内容：${error instanceof Error ? error.message : String(error)}。可点击重试。`,
      })
      persistSoon()
      return false
    }
  }

  function finishToolFailureFeedback(noticeId: string, ok: boolean, reason = '') {
    updateAnalysisNotice(noticeId, ok ? {
      analysisStatus: 'complete', feedbackEligible: false,
      text: '工具调用错误记录已完成脱敏整理并自动上传，感谢您的反馈。',
      analysisTrace: undefined,
    } : {
      analysisStatus: 'ready', feedbackEligible: true,
      text: `整理已完成，但自动上传失败${reason ? `：${reason}` : ''}。可直接重试上传，无需再次调用模型。`,
    })
    persistSoon()
  }

  function updateAnalysisNotice(id: string, patch: Partial<Extract<Timelineitem, { kind: 'notice' }>>) {
    const notice = timeline.value.find(item => item.kind === 'notice' && item.id === id)
    if (notice?.kind === 'notice') Object.assign(notice, patch)
  }

  return { sessionId, timeline, runState, usage, retryConfirmation, cwd, loop, isBusy, pendingApproval, pendingQuestion, connect, disconnect, flushPersistence, sendMessage, cancel, approve, answerQuestion, setPermissionMode, setReasoningEffort, setMaxToolRounds, togglePlanMode, selectModel, retryInterruptedTurn, dismissRetry, completeFileTransfer, submitManualResponse, newSession, openSession, deleteSession, setSessionCwd, sendGuide, consentToolFailureFeedback, finishToolFailureFeedback }
})

function fmtTokens(n: number): string { return n >= 1000 ? (n / 1000).toFixed(1) + 'k' : String(n) }

function sanitizeToolName(name: string): string {
  return name.replace(/[^a-zA-Z0-9_.:-]/g, '').slice(0, 80) || 'unknown_tool'
}

function classifyToolError(message: string): string {
  const text = message.toLowerCase()
  if (/permission|denied|allowed area/.test(text)) return 'permission_or_sandbox'
  if (/timeout|timed out/.test(text)) return 'timeout'
  if (/not found|enoent/.test(text)) return 'not_found'
  if (/invalid|schema|argument|parse/.test(text)) return 'invalid_arguments'
  if (/network|connect|dns|http/.test(text)) return 'network_or_upstream'
  return 'execution_error'
}

function summarizeArguments(value: unknown, key = '', depth = 0): unknown {
  if (depth > 4) return '[max_depth]'
  if (value === null) return '[null]'
  if (Array.isArray(value)) return value.slice(0, 12).map(item => summarizeArguments(item, key, depth + 1))
  if (typeof value === 'object') {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>).slice(0, 30).map(([childKey, child]) => [
      childKey.replace(/[^a-zA-Z0-9_.:-]/g, '').slice(0, 80) || 'field',
      summarizeArguments(child, childKey, depth + 1),
    ]))
  }
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return '[number]'
  if (typeof value !== 'string') return `[${typeof value}]`
  const text = value.trim()
  const lowerKey = key.toLowerCase()
  if (/key|token|secret|password|authorization|credential/.test(lowerKey)) return '[redacted_secret]'
  if (/path|file|dir|cwd|destination|source/.test(lowerKey) || /^(?:\/|[a-z]:\\)/i.test(text)) {
    const extension = text.match(/\.([a-zA-Z0-9]{1,8})$/)?.[1]?.toLowerCase()
    return `[${/^(?:\/|[a-z]:\\)/i.test(text) ? 'absolute' : 'relative'}_path${extension ? ` ext=.${extension}` : ''}]`
  }
  if (/command|cmd|script/.test(lowerKey) || /[\s;&|><]/.test(text)) {
    const tokens = text.split(/\s+/).filter(Boolean)
    const executable = tokens[0]?.split(/[\\/]/).pop()?.replace(/[^a-zA-Z0-9_.+-]/g, '') || 'unknown'
    const flags = tokens.slice(1).filter(token => /^--?[a-zA-Z0-9_-]+$/.test(token)).slice(0, 12)
    return { kind: 'command_shape', executable, flags, token_count: tokens.length, has_shell_operators: /[;&|><]/.test(text) }
  }
  if (/^https?:\/\//i.test(text)) return '[url_redacted]'
  if (/^[a-zA-Z][a-zA-Z0-9_.:-]{0,31}$/.test(text)) return text
  return `[string length=${text.length}]`
}

function sanitizeDiagnosticText(message: string): string {
  return message
    .slice(0, 1200)
    .replace(/\b(?:sk-|Bearer\s+)[a-zA-Z0-9._-]{8,}\b/gi, '[redacted_secret]')
    .replace(/https?:\/\/[^\s"']+/gi, '[redacted_url]')
    .replace(/(?:[a-zA-Z]:\\|\/data\/|\/storage\/|\/sdcard\/|\/home\/)[^\s"']+/g, '[redacted_path]')
    .replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g, '[redacted_email]')
    .replace(/\b[0-9a-f]{24,}\b/gi, '[redacted_identifier]')
    .replace(/(["'])(?:(?!\1).){41,}\1/g, '[redacted_text]')
}

function buildLocalEvidence(items: ToolDiagnosticTrace[]): string {
  return [
    '【程序采集的脱敏证据】',
    '不含用户消息、原始参数值、文件内容、真实路径、URL、密钥或模型隐藏思维。',
    ...items.map(item => `#${item.sequence} ${item.tool} | ${item.status}${item.category ? ` | ${item.category}` : ''}${item.elapsedMs !== undefined ? ` | ${item.elapsedMs}ms` : ''}\n参数结构: ${JSON.stringify(item.argumentShape)}${item.errorSummary ? `\n错误摘要: ${item.errorSummary}` : ''}`),
  ].join('\n')
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
const ACTIVE_SESSION_KEY = 'coomi.activeSessionId.v1'

function isUuid(value: string): boolean {
  return UUID_PATTERN.test(value)
}

function readActiveSessionId(): string {
  try {
    const saved = localStorage.getItem(ACTIVE_SESSION_KEY) ?? ''
    if (isUuid(saved)) return saved
  } catch {
    // WebView storage can be unavailable during early startup; create a valid fallback.
  }
  return createSessionId()
}

function persistActiveSessionId(id: string) {
  try {
    localStorage.setItem(ACTIVE_SESSION_KEY, id)
  } catch {
    // Keeping the in-memory id is enough for this process lifetime.
  }
}

/** 引擎磁盘上会话文件的原始消息结构（与 coomi-engine 的 ChatMessage 对应）。 */
interface ChatMessageJson {
  role: 'system' | 'user' | 'assistant' | 'tool'
  content: string
  tool_calls?: Array<{
    id: string
    name: string
    arguments: unknown
    images?: Array<{ media_type: string; data: string }>
  }>
  tool_call_id?: string
  compaction_summary?: boolean
  internal?: boolean
  images?: Array<{ media_type: string; data: string }>
}

function createSessionId(): string {
  const cryptoApi = globalThis.crypto
  if (typeof cryptoApi?.randomUUID === 'function') return cryptoApi.randomUUID()
  const bytes = new Uint8Array(16)
  cryptoApi.getRandomValues(bytes)
  bytes[6] = (bytes[6] & 0x0f) | 0x40
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
