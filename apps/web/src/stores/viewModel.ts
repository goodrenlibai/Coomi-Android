import type { ToolAccess, UserQuestion } from '@/protocol/events'

export type ToolCardStatus = 'starting' | 'running' | 'success' | 'error' | 'awaiting_approval' | 'cache_hit' | 'cancelled'

export interface ToolCard {
  kind: 'tool'
  callId: string
  toolName: string
  arguments: Record<string, unknown>
  status: ToolCardStatus
  elapsed?: number
  resultPreview?: string
  isError?: boolean
  access?: ToolAccess
  riskSummary?: string
  expanded?: boolean
  /** 工具产生的图片（data URL），瀑布流渲染用。 */
  images?: string[]
  /** show_image 历史恢复但图片数据不可用（如已被上下文压缩清理）。 */
  imageMissing?: boolean
}

export interface AssistantMessage { kind: 'assistant'; id: string; content: string; streaming: boolean }
export interface UserMessage { kind: 'user'; id: string; content: string }
export interface ReasoningBlock { kind: 'reasoning'; id: string; content: string; expanded: boolean }

export interface QuestionCard {
  kind: 'question'; callId: string; questions: UserQuestion[]
  answered: boolean; answers?: Record<string, string>
}

export interface NoticeItem {
  kind: 'notice'
  id: string
  tone: 'info' | 'warn' | 'error' | 'success'
  text: string
  detail?: string
  feedbackEligible?: boolean
  analysisStatus?: 'consent' | 'analyzing' | 'ready' | 'uploading' | 'complete' | 'failed'
  analysisTrace?: ToolDiagnosticTrace[]
  failureCount?: number
}

export interface ToolDiagnosticTrace {
  callId?: string
  sequence: number
  tool: string
  argumentShape: unknown
  status: 'running' | 'error' | 'success'
  category?: string
  errorSummary?: string
  elapsedMs?: number
}

/** 人工模式：一次「复制提示词 → 粘贴外部 AI 回答」的交互卡片。 */
export interface ManualCard {
  kind: 'manual'
  id: string
  seq: number
  prompt: string
  status: 'awaiting' | 'submitted' | 'cancelled'
  /** 用户粘贴回来的回答（提交后回显，默认折叠）。 */
  response?: string
}

export type Timelineitem = UserMessage | AssistantMessage | ReasoningBlock | ToolCard | QuestionCard | NoticeItem | ManualCard

export type RunState = 'idle' | 'syncing' | 'thinking' | 'executing' | 'awaiting_approval' | 'awaiting_question' | 'awaiting_manual'

export interface LoopProgress {
  active: boolean; currentStep: number; totalSteps: number
  status: string; currentDescription?: string
}
