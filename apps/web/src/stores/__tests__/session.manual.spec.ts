import { describe, it, expect, vi, beforeEach } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

/**
 * 用一个可控的假 Transport 替换真实 WebSocket，模拟「引擎 → 事件流」，
 * 从而在 jsdom 里跑通完整的人工模式用户流程：
 *   发送任务 → 收到提示词卡片 → 粘贴回答 → 引擎执行工具 → 本轮结束。
 */
const h = vi.hoisted(() => ({
  fake: {
    sent: [] as Array<Record<string, unknown>>,
    msgHandlers: [] as Array<(env: any) => void>,
    stateHandlers: [] as Array<(status: any) => void>,
    connect() {
      this.stateHandlers.forEach((fn) => fn({ state: 'open' }))
    },
    close() {},
    send(cmd: Record<string, unknown>) {
      this.sent.push(cmd)
    },
    onMessage(fn: (env: any) => void) {
      this.msgHandlers.push(fn)
    },
    onStateChange(fn: (status: any) => void) {
      this.stateHandlers.push(fn)
    },
    /** 模拟引擎推送一条入站事件。 */
    deliver(env: any) {
      this.msgHandlers.forEach((fn) => fn(env))
    },
  },
}))

vi.mock('@/bridge', () => ({
  createTransport: () => h.fake,
}))

vi.mock('@/bridge/demoMode', () => ({
  isDemoMode: () => false,
  setDemoMode: vi.fn(),
  shouldAutoplay: () => false,
  isUnattended: () => false,
}))

vi.mock('@/bridge/http', () => ({
  authedFetch: vi.fn(async () => ({ ok: true, json: async () => ({}) })),
  apiGet: vi.fn(async () => ({})),
  apiSend: vi.fn(async () => ({})),
  engineToken: () => '',
  API_BASE: '',
}))

import { useSessionStore } from '../session'
import { useSessionsStore } from '../sessions'

beforeEach(() => {
  h.fake.sent.length = 0
  h.fake.msgHandlers.length = 0
  h.fake.stateHandlers.length = 0
  setActivePinia(createPinia())
  localStorage.clear()
})

describe('人工模式功能测试（session store）', () => {
  it('manual_request 事件创建等待卡片并把状态切到 awaiting_manual', () => {
    const session = useSessionStore()
    session.connect()
    h.fake.deliver({
      type: 'event',
      payload: { event_type: 'manual_request', seq: 1, prompt: '请继续分析…' },
    })
    expect(session.runState).toBe('awaiting_manual')
    const card = session.timeline.find((t) => t.kind === 'manual') as any
    expect(card).toBeTruthy()
    expect(card.seq).toBe(1)
    expect(card.prompt).toBe('请继续分析…')
    expect(card.status).toBe('awaiting')
  })

  it('同一 seq 重复推送去重，不产生第二张卡片', () => {
    const session = useSessionStore()
    session.connect()
    const ev = { type: 'event', payload: { event_type: 'manual_request', seq: 3, prompt: 'x' } }
    h.fake.deliver(ev)
    h.fake.deliver(ev)
    expect(session.timeline.filter((t) => t.kind === 'manual')).toHaveLength(1)
  })

  it('manual_warning 渲染为告警 notice', () => {
    const session = useSessionStore()
    session.connect()
    h.fake.deliver({
      type: 'event',
      payload: { event_type: 'manual_warning', message: '忽略未知工具: foo' },
    })
    const notice = session.timeline.find((t) => t.kind === 'notice') as any
    expect(notice).toBeTruthy()
    expect(notice.tone).toBe('warn')
    expect(notice.text).toBe('忽略未知工具: foo')
  })

  it('turn_end 把仍等待的卡片收尾为已取消并回到 idle', () => {
    const session = useSessionStore()
    session.connect()
    session.sendMessage('任务')
    h.fake.deliver({ type: 'event', payload: { event_type: 'manual_request', seq: 2, prompt: 'p' } })
    expect(session.runState).toBe('awaiting_manual')
    h.fake.deliver({ type: 'event', payload: { event_type: 'turn_end' } })
    const card = session.timeline.find((t) => t.kind === 'manual') as any
    expect(card.status).toBe('cancelled')
    expect(session.runState).toBe('idle')
  })
})

describe('人工模式用户模拟流程测试', () => {
  it('发送任务 → 提示词 → 粘贴回答 → 执行工具 → 结束（全链路）', () => {
    const session = useSessionStore()
    session.connect()
    // 连接建立后自动同步权限模式等配置。
    expect(h.fake.sent.some((c) => c.command === 'set_permission_mode')).toBe(true)

    // 1) 用户下达任务。
    session.sendMessage('帮我写一个文件')
    expect(session.runState).toBe('thinking')
    expect(h.fake.sent.some((c) => c.command === 'send_message')).toBe(true)
    expect(session.timeline.some((t) => t.kind === 'user')).toBe(true)

    // 2) 引擎推来提示词。
    h.fake.deliver({
      type: 'event',
      payload: { event_type: 'manual_request', seq: 1, prompt: '请继续…' },
    })
    expect(session.runState).toBe('awaiting_manual')

    // 3) 用户粘贴外部 AI 回答并确认（首尾空白应被裁剪）。
    session.submitManualResponse('manual-1', '  [{"name":"write_file","arguments":{"path":"a.txt","content":"hi"}}]  ')
    const manualCmd = h.fake.sent.find((c) => c.command === 'manual_response') as any
    expect(manualCmd).toBeTruthy()
    expect(manualCmd.text).toBe('[{"name":"write_file","arguments":{"path":"a.txt","content":"hi"}}]')
    const card = session.timeline.find((t) => t.kind === 'manual') as any
    expect(card.status).toBe('submitted')
    expect(session.runState).toBe('thinking')

    // 4) 引擎执行工具并给出最终回答。
    h.fake.deliver({ type: 'event', payload: { event_type: 'tool_start', call_id: 'c1', tool_name: 'write_file', arguments: {} } })
    h.fake.deliver({ type: 'event', payload: { event_type: 'tool_done', call_id: 'c1', tool_name: 'write_file', elapsed: 0.1, result_preview: 'wrote 2 bytes', is_error: false } })
    h.fake.deliver({ type: 'event', payload: { event_type: 'text_chunk', content: '文件已写好。' } })
    h.fake.deliver({ type: 'event', payload: { event_type: 'turn_end' } })

    expect(session.runState).toBe('idle')
    expect(session.timeline.some((t) => t.kind === 'assistant')).toBe(true)
    expect(session.timeline.some((t) => t.kind === 'tool')).toBe(true)
  })

  it('最终结论（无工具调用）时 timeline 仍保留人工卡片与助手回复', () => {
    const session = useSessionStore()
    session.connect()
    session.sendMessage('简单问题')
    h.fake.deliver({ type: 'event', payload: { event_type: 'manual_request', seq: 1, prompt: 'p' } })
    session.submitManualResponse('manual-1', '任务完成，无需工具。')
    h.fake.deliver({ type: 'event', payload: { event_type: 'text_chunk', content: '任务完成，无需工具。' } })
    h.fake.deliver({ type: 'event', payload: { event_type: 'turn_end' } })
    expect(session.runState).toBe('idle')
    expect(session.timeline.filter((t) => t.kind === 'manual')).toHaveLength(1)
    expect(session.timeline.some((t) => t.kind === 'assistant')).toBe(true)
  })
})

describe('人工模式本地快照', () => {
  it('保存转录时剥离人工卡片提示词全文（防 localStorage 撑爆）', () => {
    const sessions = useSessionsStore()
    const items: any[] = [
      { kind: 'manual', id: 'manual-1', seq: 1, prompt: '很长的提示词'.repeat(500), status: 'awaiting' },
      { kind: 'user', id: 'u1', content: '你好' },
    ]
    sessions.saveTranscript('sid-1', items)
    const loaded = sessions.loadTranscript('sid-1')
    expect(loaded).toHaveLength(2)
    const card = loaded.find((t) => t.kind === 'manual') as any
    expect(card.prompt).toBe('')
    expect(card.status).toBe('awaiting')
    const user = loaded.find((t) => t.kind === 'user') as any
    expect(user.content).toBe('你好')
  })
})
