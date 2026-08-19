<script setup lang="ts">
/**
 * 人工模式卡片（面向无 API 用户）。
 *
 * 引擎每轮「模型调用」都会推来一份拼装好的提示词（manual_request 事件），
 * 用户：
 *   1. 点击「复制提示词」→ 粘贴到任意免费外部 AI（ChatGPT / Claude / 文心一言等）；
 *   2. 把外部 AI 的回答粘贴回卡片输入框；
 *   3. 点击「确认执行」→ 引擎解析其中的工具调用并执行，然后进入下一轮。
 * 循环往复，直到外部 AI 给出最终结论（无工具调用时本轮结束）。
 *
 * 设计借鉴 JsxposedX 的「人工发送」交互：复制卡片 + 粘贴输入 + 步骤提示。
 */
import { computed, ref, watch } from 'vue'
import type { ManualCard } from '@/stores/viewModel'
import CoomiIcon from './CoomiIcon.vue'

const props = defineProps<{ card: ManualCard }>()
const emit = defineEmits<{ submit: [cardId: string, text: string] }>()

const promptOpen = ref(false)
const responseOpen = ref(false)
const pasted = ref('')
const copied = ref(false)

// 换卡片（新请求）时清空输入与折叠状态。
watch(() => props.card.seq, () => {
  pasted.value = ''
  promptOpen.value = false
  responseOpen.value = false
  copied.value = false
})

const isAwaiting = computed(() => props.card.status === 'awaiting')
const isCancelled = computed(() => props.card.status === 'cancelled')
const canSubmit = computed(() => isAwaiting.value && pasted.value.trim().length > 0)
/** 本地快照恢复的旧卡片不保留提示词全文（引擎磁盘会话才是权威源）。 */
const promptMissing = computed(() => props.card.prompt.length === 0)

/** 步骤提示：与 JsxposedX 的步骤横幅对应。 */
const stepHint = computed(() => {
  if (isCancelled.value) return '本轮已取消，可重新发送任务'
  if (!isAwaiting.value) return '已提交回答'
  return '复制左侧提示词发给外部 AI，再把回答粘贴回来点击「确认执行」'
})

async function copyPrompt() {
  const text = props.card.prompt
  try {
    await navigator.clipboard.writeText(text)
  } catch {
    // WebView 里 clipboard API 偶发不可用，退回 execCommand。
    const ta = document.createElement('textarea')
    ta.value = text
    ta.style.position = 'fixed'
    ta.style.opacity = '0'
    document.body.appendChild(ta)
    ta.select()
    try { document.execCommand('copy') } catch { /* 放弃 */ }
    document.body.removeChild(ta)
  }
  copied.value = true
  setTimeout(() => { copied.value = false }, 1400)
}

function submit() {
  if (!canSubmit.value) return
  emit('submit', props.card.id, pasted.value)
}
</script>

<template>
  <div class="manual-card cascade" :class="{ done: !isAwaiting }">
    <div class="mhead">
      <span class="micon"><CoomiIcon name="user" :size="15" /></span>
      <span class="mtitle">人工模式</span>
      <span class="mstep">{{ isCancelled ? '已取消' : (isAwaiting ? '等待粘贴回答' : '已提交') }}</span>
    </div>

    <p class="mhint">{{ stepHint }}</p>

    <!-- 提示词（可折叠，默认折叠） -->
    <div class="mblock prompt">
      <div class="mblock-head" role="button" @click="promptOpen = !promptOpen">
        <CoomiIcon name="chevronRight" :size="13" class="chev" :class="{ open: promptOpen }" />
        <span v-if="promptMissing">历史会话：提示词未保留</span>
        <span v-else>要复制给外部 AI 的提示词（{{ card.prompt.length }} 字符）</span>
        <button
          v-if="!promptMissing && !isCancelled"
          class="copy-btn"
          :class="{ ok: copied }"
          @click.stop="copyPrompt"
        >{{ copied ? '已复制' : '复制提示词' }}</button>
      </div>
      <pre v-if="promptOpen && !promptMissing" class="mbody">{{ card.prompt }}</pre>
    </div>

    <!-- 输入：粘贴外部 AI 的回答 -->
    <template v-if="isAwaiting">
      <textarea
        v-model="pasted"
        class="mpaste"
        rows="4"
        placeholder="把外部 AI 的回答粘贴到这里…（可以是最终结论，也可以是需要执行的工具调用 JSON）"
      />
      <div class="mactions">
        <button class="btn btn-primary msubmit" :disabled="!canSubmit" @click="submit">
          <CoomiIcon name="check" :size="15" />
          <span>确认执行</span>
        </button>
      </div>
    </template>

    <!-- 已提交：回显粘贴内容（默认折叠）；已取消则不显示输入与回显 -->
    <template v-else-if="!isCancelled">
      <div class="mblock response">
        <div class="mblock-head" role="button" @click="responseOpen = !responseOpen">
          <CoomiIcon name="chevronRight" :size="13" class="chev" :class="{ open: responseOpen }" />
          <span>你粘贴的回答（点击展开）</span>
        </div>
        <pre v-if="responseOpen" class="mbody resp">{{ card.response }}</pre>
      </div>
    </template>
  </div>
</template>

<style scoped>
.manual-card {
  margin: 2px 12px 8px;
  border: 1px solid var(--blue-border);
  border-radius: var(--r-md);
  background: var(--blue-soft);
  overflow: hidden;
}
.manual-card.done { border-color: var(--border); background: var(--fill); }
.mhead {
  display: flex; align-items: center; gap: 7px;
  padding: 9px 12px 0;
}
.micon {
  display: grid; place-items: center;
  width: 24px; height: 24px; border-radius: 7px;
  background: var(--blue); color: #fff;
}
.mtitle { font-size: 13px; font-weight: 650; color: var(--blue); }
.done .mtitle { color: var(--text-2); }
.mstep { margin-left: auto; font-size: 11.5px; font-weight: 600; color: var(--orange); }
.done .mstep { color: var(--ok); }
.mhint { margin: 0; padding: 6px 12px 8px; font-size: 12px; color: var(--text-2); line-height: 1.5; }

.mblock { border-top: 1px solid var(--blue-border); }
.done .mblock { border-top-color: var(--border); }
.mblock-head {
  display: flex; align-items: center; gap: 6px;
  width: 100%; padding: 8px 12px; text-align: left;
  font-size: 12.5px; font-weight: 600; color: var(--text);
}
.mblock-head .chev { color: var(--text-3); transition: transform .16s; }
.mblock-head .chev.open { transform: rotate(90deg); }
.copy-btn {
  margin-left: auto; padding: 4px 10px;
  border-radius: var(--r-pill);
  background: var(--blue); color: #fff;
  font-size: 12px; font-weight: 600;
}
.copy-btn.ok { background: var(--ok); }
.mbody {
  margin: 0 12px 10px; padding: 10px;
  max-height: 240px; overflow: auto;
  background: var(--bg); border-radius: var(--r-sm);
  font-family: var(--font-mono); font-size: 11.5px; line-height: 1.55;
  color: var(--code-text); white-space: pre-wrap; word-break: break-word;
}
.mbody.resp { color: var(--text); font-family: var(--font-ui); }

.mpaste {
  display: block; width: calc(100% - 24px); margin: 0 12px 10px;
  padding: 9px 10px; border: 1px solid var(--border); border-radius: var(--r-sm);
  background: var(--bg-input); color: var(--text);
  font-size: 13px; line-height: 1.55; resize: vertical;
  min-height: 72px;
}
.mpaste:focus { outline: none; border-color: var(--blue); }
.mactions { display: flex; justify-content: flex-end; padding: 0 12px 10px; }
.msubmit {
  min-height: 38px; padding: 0 16px; font-size: 13px;
  display: inline-flex; align-items: center; gap: 5px;
}
</style>
