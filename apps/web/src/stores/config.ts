import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { PermissionMode, ReasoningEffort } from '@/protocol/commands'
import { apiGet, apiSend } from '@/bridge/http'

export interface ProviderConfig {
  id: string; name: string; apiKeyMasked: string; hasKey?: boolean
  models: string[]; baseUrl?: string
  type?: string; model?: string; fastModel?: string | null; toolProtocol?: string
  contextWindow?: number
  modelContextWindows?: Record<string, number>
  supportsWebSearch?: boolean
  supportsVision?: boolean
  active?: boolean
  builtin?: boolean
  status?: ProviderStatus
}

export type ProviderProtocol = 'openai_compatible' | 'openai_responses' | 'anthropic_messages' | 'gemini_native'
export type ProviderStatus = 'unconfigured' | 'configured' | 'current'

export interface ConnectionSettings {
  providerRetryCount: number
  wsRetryCount: number
  reconnectInitialDelayMs: number
  reconnectMaxDelayMs: number
}

export const DEFAULT_CONNECTION_SETTINGS: ConnectionSettings = {
  providerRetryCount: 2,
  wsRetryCount: 10,
  reconnectInitialDelayMs: 500,
  reconnectMaxDelayMs: 10_000,
}

export interface ProviderPreset {
  id: string
  name: string
  baseUrl: string
  protocol: ProviderProtocol
}

export const BUILTIN_PROVIDER_PRESETS: ProviderPreset[] = [
  { id: 'deepseek', name: 'DeepSeek', baseUrl: 'https://api.deepseek.com/v1', protocol: 'openai_compatible' },
  { id: 'zhipu', name: '智谱', baseUrl: 'https://open.bigmodel.cn/api/paas/v4', protocol: 'openai_compatible' },
  { id: 'minimax', name: 'MiniMax', baseUrl: 'https://api.minimaxi.com/v1', protocol: 'openai_compatible' },
  { id: 'openai', name: 'OpenAI', baseUrl: 'https://api.openai.com/v1', protocol: 'openai_responses' },
  { id: 'anthropic', name: 'Anthropic', baseUrl: 'https://api.anthropic.com/v1', protocol: 'anthropic_messages' },
  { id: 'google', name: 'Gemini', baseUrl: 'https://generativelanguage.googleapis.com/v1beta', protocol: 'gemini_native' },
  { id: 'opencode', name: 'OpenCode', baseUrl: 'https://opencode.ai/zen/go/v1', protocol: 'openai_compatible' },
]

export interface ProviderInput {
  id: string; name: string; apiKey: string; models: string[]
  baseUrl?: string; type?: string; toolProtocol?: string; contextWindow?: number
  modelContextWindows?: Record<string, number>
  fastModel?: string | null; activate?: boolean; supportsWebSearch?: boolean; supportsVision?: boolean
}

export function providerStatus(provider: ProviderConfig, activeId: string): ProviderStatus {
  const configured = Boolean(provider.hasKey && provider.models.length > 0)
  if (configured && provider.id === activeId) return 'current'
  return configured ? 'configured' : 'unconfigured'
}

export function mergeProviderList(configured: ProviderConfig[], activeId: string): ProviderConfig[] {
  const configuredById = new Map(configured.map(provider => [provider.id, provider]))
  const builtInIds = new Set(BUILTIN_PROVIDER_PRESETS.map(preset => preset.id))
  const builtIns = BUILTIN_PROVIDER_PRESETS.map(preset => {
    const saved = configuredById.get(preset.id)
    const provider: ProviderConfig = {
      id: preset.id,
      name: saved?.name || preset.name,
      apiKeyMasked: saved?.apiKeyMasked || '',
      hasKey: Boolean(saved?.hasKey),
      models: saved?.models ?? [],
      baseUrl: saved?.baseUrl || preset.baseUrl,
      type: saved?.type || preset.protocol,
      model: saved?.model,
      fastModel: saved?.fastModel,
      toolProtocol: saved?.toolProtocol || preset.protocol,
      contextWindow: saved?.contextWindow ?? 256000,
      modelContextWindows: { ...(saved?.modelContextWindows ?? {}) },
      supportsWebSearch: saved?.supportsWebSearch ?? false,
      supportsVision: saved?.supportsVision ?? false,
      active: activeId === preset.id,
      builtin: true,
    }
    provider.status = providerStatus(provider, activeId)
    return provider
  })
  const custom = configured
    .filter(provider => !builtInIds.has(provider.id))
    .map(provider => ({ ...provider, builtin: false, status: providerStatus(provider, activeId) }))
  return [...builtIns, ...custom]
}

export const PERMISSION_MODES: { mode: PermissionMode; label: string; desc: string }[] = [
  { mode: 'ask', label: '询问', desc: '每个写入/破坏性操作前都确认' },
  { mode: 'auto', label: '自动', desc: '读写自动放行，仅破坏性需确认' },
  { mode: 'full', label: '放行', desc: '全部自动执行（仅信任场景）' },
]

/** 主题三档：system 跟随系统、light 明亮、dark 夜间。 */
export type ThemeMode = 'system' | 'light' | 'dark' | 'book' | 'orange'
export const THEME_MODES: { mode: ThemeMode; label: string; desc: string }[] = [
  { mode: 'system', label: '跟随系统', desc: '与手机系统深浅色保持一致' },
  { mode: 'light', label: '明亮模式', desc: '始终使用浅色界面' },
  { mode: 'dark', label: '夜间模式', desc: '始终使用深色界面' },
  { mode: 'book', label: '书卷纸', desc: '柔和纸张底色与墨绿色点缀' },
  { mode: 'orange', label: '橙白', desc: '明快白色底面与暖橙色点缀' },
]

export const REASONING_EFFORTS: { value: ReasoningEffort; label: string }[] = [
  { value: 'auto', label: '自动' },
  { value: 'low', label: '低' },
  { value: 'medium', label: '中' },
  { value: 'high', label: '高' },
  { value: 'xhigh', label: '超高' },
]

/** 取当前主题档位：优先 Android 原生偏好（JS 桥），其次 localStorage，默认跟随系统。 */
export function readThemeMode(): ThemeMode {
  const bridge = (window as any).CoomiAndroid
  if (bridge && typeof bridge.getThemeMode === 'function') {
    try {
      const v = String(bridge.getThemeMode() ?? '')
      if (['light', 'dark', 'system', 'book', 'orange'].includes(v)) return v as ThemeMode
    } catch { /* 桥未就绪时走 localStorage */ }
  }
  const saved = localStorage.getItem('coomi.themeMode')
  return ['light', 'dark', 'system', 'book', 'orange'].includes(saved ?? '') ? saved as ThemeMode : 'system'
}

/** 写入 <html data-theme>，前端 global.css 据此切换暗色主题。 */
export function applyTheme(mode: ThemeMode) {
  const dark = mode === 'dark'
    || (mode === 'system' && window.matchMedia?.('(prefers-color-scheme: dark)').matches)
  document.documentElement.setAttribute('data-theme', dark ? 'dark' : mode === 'book' || mode === 'orange' ? mode : 'light')
}

// 浏览器独立开发时的兜底数据（后端不可达时使用）
const MOCK_PROVIDERS: ProviderConfig[] = [
  { id: 'openai', name: 'OpenAI', apiKeyMasked: '****a1b2', hasKey: true, models: ['gpt-4o', 'gpt-4o-mini'], baseUrl: 'https://api.openai.com/v1' },
  { id: 'anthropic', name: 'Anthropic', apiKeyMasked: '****9f3c', hasKey: true, models: ['claude-sonnet-4', 'claude-opus-4'] },
]

export const useConfigStore = defineStore('config', () => {
  const savedPermission = localStorage.getItem('coomi.permissionMode') as PermissionMode | null
  const permissionMode = ref<PermissionMode>(['ask', 'auto', 'full'].includes(savedPermission ?? '') ? savedPermission! : 'ask')
  const planMode = ref(false)
  const themeMode = ref<ThemeMode>(readThemeMode())
  const savedEffort = localStorage.getItem('coomi.reasoningEffort') as ReasoningEffort | null
  const reasoningEffort = ref<ReasoningEffort>(REASONING_EFFORTS.some(item => item.value === savedEffort) ? savedEffort! : 'auto')
  const savedRounds = Number(localStorage.getItem('coomi.maxToolRounds'))
  const maxToolRounds = ref([192, 256, 512].includes(savedRounds) ? savedRounds : 192)
  const connectionSettings = ref<ConnectionSettings>({
    providerRetryCount: readStoredInt('coomi.providerRetryCount', 0, 10, DEFAULT_CONNECTION_SETTINGS.providerRetryCount),
    wsRetryCount: readStoredInt('coomi.wsRetryCount', 0, 30, DEFAULT_CONNECTION_SETTINGS.wsRetryCount),
    reconnectInitialDelayMs: readStoredInt('coomi.reconnectInitialDelayMs', 500, 60_000, DEFAULT_CONNECTION_SETTINGS.reconnectInitialDelayMs),
    reconnectMaxDelayMs: readStoredInt('coomi.reconnectMaxDelayMs', 1_000, 120_000, DEFAULT_CONNECTION_SETTINGS.reconnectMaxDelayMs),
  })

  const providers = ref<ProviderConfig[]>([])
  const activeId = ref('')
  const loading = ref(false)
  const usingMock = ref(false)
  const lastError = ref<string | null>(null)

  const currentProviderId = ref('')
  const currentModel = ref('')
  const currentProvider = computed(() => providers.value.find(p => p.id === currentProviderId.value) ?? null)
  const mergedProviders = computed(() => mergeProviderList(providers.value, activeId.value))

  function applyList(list: ProviderConfig[], active: string) {
    providers.value = list
    activeId.value = active
    // 同步当前选择：优先 active，其次第一个
    const sel = list.find(p => p.id === active) ?? list[0]
    if (sel) {
      const savedProvider = localStorage.getItem('coomi.providerId')
      const savedModel = localStorage.getItem('coomi.model')
      const saved = list.find(p => p.id === savedProvider && p.models.includes(savedModel ?? ''))
      currentProviderId.value = saved?.id ?? sel.id
      currentModel.value = savedModel && saved ? savedModel : (sel.model || sel.models[0] || '')
    } else {
      currentProviderId.value = ''
      currentModel.value = ''
    }
  }

  /** 从后端拉取 Provider 列表；失败则用 mock 兜底（浏览器独立开发）。 */
  async function fetchProviders() {
    loading.value = true
    lastError.value = null
    try {
      const data = await apiGet<{ providers: ProviderConfig[]; active: string }>('/api/providers')
      usingMock.value = false
      applyList(data.providers ?? [], data.active ?? '')
    } catch (e) {
      usingMock.value = true
      lastError.value = String(e)
      applyList(MOCK_PROVIDERS, 'openai')
    } finally {
      loading.value = false
    }
  }

  function selectModel(providerId: string, model: string) {
    currentProviderId.value = providerId; currentModel.value = model
    localStorage.setItem('coomi.providerId', providerId)
    localStorage.setItem('coomi.model', model)
  }
  function setPermissionMode(mode: PermissionMode) {
    permissionMode.value = mode
    localStorage.setItem('coomi.permissionMode', mode)
  }
  function setReasoningEffort(effort: ReasoningEffort) {
    reasoningEffort.value = effort
    localStorage.setItem('coomi.reasoningEffort', effort)
  }
  function setMaxToolRounds(rounds: number) {
    maxToolRounds.value = [192, 256, 512].includes(rounds) ? rounds : 192
    localStorage.setItem('coomi.maxToolRounds', String(maxToolRounds.value))
  }

  function cacheConnectionSettings(value: ConnectionSettings) {
    connectionSettings.value = { ...value }
    localStorage.setItem('coomi.providerRetryCount', String(value.providerRetryCount))
    localStorage.setItem('coomi.wsRetryCount', String(value.wsRetryCount))
    localStorage.setItem('coomi.reconnectInitialDelayMs', String(value.reconnectInitialDelayMs))
    localStorage.setItem('coomi.reconnectMaxDelayMs', String(value.reconnectMaxDelayMs))
  }

  async function fetchConnectionSettings(): Promise<boolean> {
    try {
      const value = await apiGet<ConnectionSettings>('/api/settings/connection')
      cacheConnectionSettings(value)
      return true
    } catch {
      return false
    }
  }

  async function saveConnectionSettings(value: ConnectionSettings): Promise<boolean> {
    const normalized: ConnectionSettings = {
      providerRetryCount: Math.trunc(value.providerRetryCount),
      wsRetryCount: Math.trunc(value.wsRetryCount),
      reconnectInitialDelayMs: Math.trunc(value.reconnectInitialDelayMs),
      reconnectMaxDelayMs: Math.trunc(value.reconnectMaxDelayMs),
    }
    if (normalized.providerRetryCount < 0 || normalized.providerRetryCount > 10
      || normalized.wsRetryCount < 0 || normalized.wsRetryCount > 30
      || normalized.reconnectInitialDelayMs < 500 || normalized.reconnectInitialDelayMs > 60_000
      || normalized.reconnectMaxDelayMs < 1_000 || normalized.reconnectMaxDelayMs > 120_000
      || normalized.reconnectMaxDelayMs < normalized.reconnectInitialDelayMs) return false
    try {
      const saved = await apiSend<ConnectionSettings>('/api/settings/connection', 'PUT', normalized)
      cacheConnectionSettings(saved)
      return true
    } catch {
      return false
    }
  }

  /**
   * 三档主题。应用后：
   * - 写入 <html data-theme>（前端样式即时切换）；
   * - Android WebView 内通知原生（CoomiAndroid.setThemeMode），原生据此改状态栏
   *   颜色并重新注入 data-theme；桌面浏览器直接由 applyTheme 生效。
   */
  function setThemeMode(mode: ThemeMode) {
    if (document.documentElement.dataset.customAppearance === 'true') return
    themeMode.value = mode
    localStorage.setItem('coomi.themeMode', mode)
    applyTheme(mode)
    const bridge = (window as any).CoomiAndroid
    if (bridge && typeof bridge.setThemeMode === 'function') {
      try { bridge.setThemeMode(mode) } catch { /* 忽略桥异常 */ }
    }
  }
  function cyclePermissionMode(): PermissionMode {
    const order: PermissionMode[] = ['ask', 'auto', 'full']
    const idx = order.indexOf(permissionMode.value)
    permissionMode.value = order[(idx + 1) % order.length]
    return permissionMode.value
  }
  function togglePlanMode() { planMode.value = !planMode.value }

  /**
   * 全局会话记忆：关闭（默认）时 Coomi 无法读取任何历史会话文件；
   * 开启后它才能读取所有历史会话记录。历史会话列表始终可见，与本开关无关。
   * 引擎 settings.json 是权威值；localStorage 只是 UI 缓存，启动时以引擎为准。
   */
  const globalMemory = ref(localStorage.getItem('coomi.globalMemory') === '1')
  /** 从引擎拉取权威值（应用启动时调用），覆盖本地缓存与开关显示。 */
  async function syncGlobalMemoryFromEngine() {
    try {
      const data = await apiGet<{ enabled: boolean }>('/api/runtime/global-memory')
      const enabled = !!data?.enabled
      globalMemory.value = enabled
      localStorage.setItem('coomi.globalMemory', enabled ? '1' : '0')
    } catch {
      /* 引擎未就绪：保持本地缓存，稍后用户操作开关时会再次同步 */
    }
  }
  async function toggleGlobalMemory() {
    const previous = globalMemory.value
    const next = !previous
    globalMemory.value = next
    localStorage.setItem('coomi.globalMemory', next ? '1' : '0')
    // 同步引擎侧：关闭时引擎屏蔽会话/配置目录的工具访问 + 系统提示加隐私禁令。
    // 失败必须回滚并提示，否则会出现「开关显示关、引擎实际开着」的脱节。
    try {
      await apiSend('/api/runtime/global-memory', 'POST', { enabled: next })
    } catch {
      globalMemory.value = previous
      localStorage.setItem('coomi.globalMemory', previous ? '1' : '0')
      throw new Error('同步引擎失败，开关已还原')
    }
  }

  /**
   * 人工模式（面向无 API 用户）：
   * 开启后引擎不再调用任何模型 API，而是把拼装好的提示词推给界面，
   * 用户复制到任意免费外部 AI（ChatGPT / Claude / 文心一言等），再把回答粘贴回来，
   * 由引擎解析其中的工具调用并执行。引擎 settings.json 是权威值。
   */
  const manualMode = ref(false)
  /** 从引擎拉取权威值（应用启动 / 进入设置页时调用）。 */
  async function syncManualModeFromEngine() {
    try {
      const data = await apiGet<{ enabled: boolean }>('/api/runtime/manual-mode')
      manualMode.value = !!data?.enabled
      localStorage.setItem('coomi.manualMode', manualMode.value ? '1' : '0')
    } catch {
      /* 引擎未就绪：保持本地状态，稍后用户操作时再次同步 */
    }
  }
  /** 切换人工模式：失败必须回滚并提示，避免「开关显示开、引擎实际关」的脱节。 */
  async function toggleManualMode() {
    const previous = manualMode.value
    const next = !previous
    manualMode.value = next
    localStorage.setItem('coomi.manualMode', next ? '1' : '0')
    try {
      await apiSend('/api/runtime/manual-mode', 'POST', { enabled: next })
    } catch {
      manualMode.value = previous
      localStorage.setItem('coomi.manualMode', previous ? '1' : '0')
      throw new Error('同步引擎失败，开关已还原')
    }
  }

  /**
   * 定制身份提示词：用户设置的专属身份/定位指令，保存后注入系统提示词，
   * 让 AI 认知自己的身份与定位。引擎 settings.json 是权威值；
   * localStorage 只做 UI 缓存。
   */
  const customPrompt = ref(localStorage.getItem('coomi.customPrompt') ?? '')
  /** 从引擎拉取权威值（应用启动 / 进入设置页时调用）。 */
  async function fetchCustomPrompt() {
    try {
      const data = await apiGet<{ text: string }>('/api/runtime/custom-prompt')
      customPrompt.value = data?.text ?? ''
      localStorage.setItem('coomi.customPrompt', customPrompt.value)
      return true
    } catch {
      return false
    }
  }
  /** 保存定制提示词；空文本表示清除。成功返回 true。 */
  async function saveCustomPrompt(text: string): Promise<boolean> {
    try {
      const data = await apiSend<{ text: string }>('/api/runtime/custom-prompt', 'POST', { text })
      customPrompt.value = data?.text ?? text
      localStorage.setItem('coomi.customPrompt', customPrompt.value)
      return true
    } catch {
      return false
    }
  }

  /** 新增/更新 Provider。空 apiKey 表示沿用旧 key（后端语义）。 */
  async function upsertProvider(input: ProviderInput): Promise<boolean> {
    if (usingMock.value) {
      // 浏览器兜底：仅本地更新，不落盘
      const existing = providers.value.find(p => p.id === input.id)
      const apiKeyMasked = input.apiKey ? '****' + input.apiKey.slice(-4) : (existing?.apiKeyMasked ?? '')
      const hasKey = input.apiKey ? true : (existing?.hasKey ?? false)
      if (existing) {
        Object.assign(existing, {
          name: input.name, apiKeyMasked, hasKey, models: input.models,
          baseUrl: input.baseUrl, type: input.type, toolProtocol: input.toolProtocol,
          contextWindow: input.contextWindow, fastModel: input.fastModel,
          modelContextWindows: { ...(input.modelContextWindows ?? {}) },
          supportsWebSearch: input.supportsWebSearch, supportsVision: input.supportsVision,
          model: input.models[0],
        })
      } else {
        providers.value.push({
          id: input.id, name: input.name, apiKeyMasked, hasKey, models: input.models,
          baseUrl: input.baseUrl, type: input.type, toolProtocol: input.toolProtocol,
          contextWindow: input.contextWindow, fastModel: input.fastModel,
          modelContextWindows: { ...(input.modelContextWindows ?? {}) },
          supportsWebSearch: input.supportsWebSearch, supportsVision: input.supportsVision,
          model: input.models[0],
        })
      }
      if (input.activate) activeId.value = input.id
      return true
    }
    try {
      await apiSend('/api/providers', 'POST', {
        id: input.id,
        name: input.name,
        apiKey: input.apiKey,
        models: input.models,
        model: input.models[0],
        baseUrl: input.baseUrl,
        type: input.type,
        toolProtocol: input.toolProtocol,
        contextWindow: input.contextWindow,
        modelContextWindows: input.modelContextWindows,
        fastModel: input.fastModel,
        supportsWebSearch: input.supportsWebSearch,
        supportsVision: input.supportsVision,
        activate: input.activate,
      })
      await fetchProviders()
      return true
    } catch (e) {
      lastError.value = String(e)
      return false
    }
  }

  async function deleteProvider(id: string): Promise<boolean> {
    if (!id.trim()) return true
    if (usingMock.value) {
      const remaining = providers.value.filter(p => p.id !== id)
      applyList(remaining, activeId.value === id ? (remaining[0]?.id ?? '') : activeId.value)
      return true
    }
    try {
      await apiSend(`/api/providers/${encodeURIComponent(id)}`, 'DELETE')
      await fetchProviders()
      return true
    } catch (e) {
      lastError.value = String(e)
      return false
    }
  }

  async function activateProvider(id: string): Promise<boolean> {
    if (usingMock.value) {
      const provider = providers.value.find(item => item.id === id)
      if (!provider) return false
      activeId.value = id
      selectModel(id, provider.model || provider.models[0] || '')
      return true
    }
    try {
      await apiSend(`/api/providers/${encodeURIComponent(id)}/activate`, 'POST')
      await fetchProviders()
      const provider = providers.value.find(item => item.id === id)
      if (!provider) throw new Error('已激活的提供商未出现在配置列表中')
      const savedProvider = localStorage.getItem('coomi.providerId')
      const savedModel = localStorage.getItem('coomi.model')
      const model = savedProvider === id && provider.models.includes(savedModel ?? '')
        ? savedModel!
        : (provider.model || provider.models[0] || '')
      selectModel(id, model)
      return true
    } catch (e) {
      lastError.value = String(e)
      return false
    }
  }

  async function copyProvider(id: string): Promise<string | null> {
    try {
      const result = await apiSend<{ id: string }>(`/api/providers/${encodeURIComponent(id)}/copy`, 'POST')
      await fetchProviders()
      return result.id
    } catch (e) {
      lastError.value = String(e)
      return null
    }
  }

  async function revealProviderKey(id: string): Promise<string | null> {
    if (usingMock.value) return null
    try {
      const result = await apiSend<{ apiKey: string }>(`/api/providers/${encodeURIComponent(id)}/reveal`, 'POST')
      return result.apiKey
    } catch (e) {
      lastError.value = String(e)
      return null
    }
  }

  async function discoverModels(id: string, persist = false): Promise<string[] | null> {
    if (usingMock.value) return providers.value.find(provider => provider.id === id)?.models ?? []
    try {
      const result = await apiSend<{ models: string[] }>(
        `/api/providers/${encodeURIComponent(id)}/discover-models`,
        'POST',
        { persist },
      )
      if (persist) await fetchProviders()
      return result.models
    } catch (e) {
      lastError.value = String(e)
      return null
    }
  }

  return {
    permissionMode, planMode, themeMode, reasoningEffort, maxToolRounds, connectionSettings, globalMemory, manualMode, customPrompt, providers, activeId, loading, usingMock, lastError,
    currentProviderId, currentModel, currentProvider, mergedProviders,
    fetchProviders, selectModel, setPermissionMode, setThemeMode, setReasoningEffort, setMaxToolRounds, fetchConnectionSettings, saveConnectionSettings, cyclePermissionMode, togglePlanMode,
    toggleGlobalMemory, syncGlobalMemoryFromEngine, toggleManualMode, syncManualModeFromEngine, fetchCustomPrompt, saveCustomPrompt,
    upsertProvider, deleteProvider, activateProvider, copyProvider, revealProviderKey, discoverModels,
  }
})

function readStoredInt(key: string, min: number, max: number, fallback: number): number {
  const value = Number(localStorage.getItem(key))
  return Number.isInteger(value) && value >= min && value <= max ? value : fallback
}
