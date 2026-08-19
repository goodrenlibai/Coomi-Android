//! 人工模式（Manual Mode）：面向无 API Key 用户的「人工发送」半自动 Agent 循环。
//!
//! 设计思路借鉴开源项目 JsxposedX 的「人工发送」模式，并适配 Coomi 的引擎架构：
//!
//!   - 引擎照常运行完整的 Agent 工具循环（工具执行、权限审批、安全检查、会话持久化、
//!     上下文管理），唯一区别是「模型调用」被替换成一次「人工交互」；
//!   - 每轮「模型调用」时，引擎把拼装好的提示词（系统提示 + 工具清单 + 对话上下文 +
//!     输出格式约定）通过事件推给前端，用户复制后粘贴到任意免费外部 AI
//!     （ChatGPT / Claude / 文心一言 / 通义等）；
//!   - 用户把外部 AI 的回答粘贴回 Coomi，引擎用容错解析器识别其中的工具调用并执行，
//!     再把工具结果注入上下文，进入下一轮，循环往复直到外部 AI 给出最终结论。
//!
//! 与原自动模式的边界：
//!   - 复用同一套工具（CoreTools）、同一套安全策略（SecurityPolicy）、同一套会话存储
//!     （SessionStore）与同一条事件通道，分析/执行能力与自动模式完全一致；
//!   - 原有 HTTP 模型链路（HttpModelProvider）不受任何影响。

use anyhow::Result;
use async_trait::async_trait;
use coomi_engine::AutoCompactScope;
use coomi_engine::ChatMessage;
use coomi_engine::CompactionRequest;
use coomi_engine::CompactionResponse;
use coomi_engine::InvalidToolCall;
use coomi_engine::ModelCapabilities;
use coomi_engine::ModelProvider;
use coomi_engine::ModelRequest;
use coomi_engine::ModelResponse;
use coomi_engine::Role;
use coomi_engine::TokenUsage;
use coomi_engine::ToolCall;
use coomi_engine::ToolSpec;
use coomi_engine::normalize_history;
use regex::Regex;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::oneshot;

/// 提示词最大字符数：超出后截断，避免复制到剪贴板的文本大到无法处理。
const MANUAL_PROMPT_MAX_CHARS: usize = 200_000;
/// 贪婪扫描的兜底：工具名之后多少字符内必须出现 JSON 参数，否则视为提及而非调用。
const SCAN_ARG_WINDOW: usize = 200;

/// 人工交互信箱：同一会话同时只会有一个「等待粘贴」的请求（会话轮次串行执行）。
/// 与 SessionTask 生命周期绑定：断线重连后未完成的请求仍可继续响应。
///
/// 序号（seq）跨请求单调递增：即使上一轮请求已响应/取消，下一轮请求仍得到
/// 更大的序号，前端据此为每一轮提示词生成独立的卡片（`manual-<seq>`），
/// 避免多轮人工循环互相覆盖同一张卡片。
#[derive(Clone, Default)]
pub struct ManualChannel {
    inner: Arc<StdMutex<ManualState>>,
}

#[derive(Default)]
struct ManualState {
    pending: Option<ManualPending>,
    /// 已分配的下一个序号（从 1 开始）。
    next_seq: u64,
}

struct ManualPending {
    responder: Option<oneshot::Sender<String>>,
}

impl ManualChannel {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个等待人工回复的请求；返回（序号, 接收端）。
    /// 重复注册会覆盖旧的等待（旧的接收端随即得到「取消」错误）。
    pub fn request(&self) -> (u64, oneshot::Receiver<String>) {
        let (sender, receiver) = oneshot::channel();
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let seq = guard.next_seq + 1;
        guard.next_seq = seq;
        guard.pending = Some(ManualPending {
            responder: Some(sender),
        });
        (seq, receiver)
    }

    /// 提交人工回复；没有等待中的请求时返回 false（调用方应回错误）。
    pub fn respond(&self, text: String) -> bool {
        let sender = {
            let mut guard = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match guard.pending.take() {
                Some(pending) => pending.responder,
                None => return false,
            }
        };
        if let Some(sender) = sender {
            let _ = sender.send(text);
        }
        true
    }

    /// 取消当前等待中的请求（任务被停止时调用，避免悬挂的应答）。
    pub fn cancel(&self) {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.pending = None;
    }

    /// 是否存在等待人工回复的请求（任务列表据此显示「等待输入」）。
    pub fn has_pending(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
            .is_some()
    }
}

/// 人工模式的模型能力：声明一个极大的本地上下文窗口，避免自动压缩触发
/// （人工模式无法调用远端压缩，也不应该用「请你写摘要」的方式打断用户循环）。
fn manual_capabilities() -> ModelCapabilities {
    ModelCapabilities {
        context_window: 16_000_000,
        effective_context_window_percent: 100,
        auto_compact_token_limit: None,
        auto_compact_scope: AutoCompactScope::Total,
        comp_hash: None,
        max_output_tokens: 8_192,
        supports_remote_compaction: false,
        supports_vision: false,
        supports_native_tools: true,
        supports_web_search: false,
        supports_parallel_tool_calls: false,
    }
}

/// 把「模型调用」替换为「人工交互」的 ModelProvider。
///
/// `notify` 回调用于把 `manual_request` / `manual_warning` 事件推入会话事件通道
/// （即 SessionTask::push_event），前端据此渲染「复制提示词 + 粘贴回答」卡片。
pub struct ManualModelProvider {
    channel: ManualChannel,
    notify: Option<Arc<dyn Fn(Value) + Send + Sync>>,
}

impl ManualModelProvider {
    pub fn new(channel: ManualChannel, notify: Option<Arc<dyn Fn(Value) + Send + Sync>>) -> Self {
        Self { channel, notify }
    }
}

#[async_trait]
impl ModelProvider for ManualModelProvider {
    fn provider_id(&self) -> &str {
        "manual"
    }

    fn model(&self) -> &str {
        "manual"
    }

    fn capabilities(&self) -> ModelCapabilities {
        manual_capabilities()
    }

    async fn complete(&self, request: ModelRequest) -> Result<ModelResponse> {
        let prompt = build_manual_prompt(&request);
        let (seq, receiver) = self.channel.request();
        if let Some(notify) = &self.notify {
            notify(json!({
                "event_type": "manual_request",
                "seq": seq,
                "prompt": prompt,
            }));
        }
        let text = receiver
            .await
            .map_err(|_| anyhow::anyhow!("人工回复被取消或超时"))?;
        let parsed = parse_manual_response(text.trim(), &request.tools);
        if let Some(notify) = &self.notify {
            for warning in &parsed.warnings {
                notify(json!({
                    "event_type": "manual_warning",
                    "message": warning,
                }));
            }
        }
        Ok(ModelResponse {
            content: parsed.content,
            tool_calls: parsed.tool_calls,
            invalid_tool_calls: parsed.invalid,
            usage: TokenUsage::default(),
            streamed: false,
        })
    }

    /// 人工模式不做远端/摘要压缩：直接返回原历史（no-op），
    /// 避免触发一次「请你为上下文写摘要」的人工请求。
    async fn compact(&self, request: CompactionRequest) -> Result<Option<CompactionResponse>> {
        Ok(Some(CompactionResponse {
            messages: normalize_history(&request.messages),
            usage: TokenUsage::default(),
        }))
    }
}

/// 把一次模型请求（系统提示 + 工具清单 + 对话历史）拼装成可复制的提示词。
///
/// 外部 AI 会话本身有记忆（用户在同一会话里持续粘贴），因此提示词只需「增量同步」，
/// 避免每轮重复声明规则、浪费外部 AI 的上下文窗口（免费额度尤其紧张）：
///   - **首轮**（新任务，尚无工具调用历史）发送完整规则：系统提示、工具清单（含完整
///     参数 schema）、输出格式约定与用户需求；
///   - **续轮**（工具循环中）只发送**最新进展**：最近一次工具执行结果（以及 loop /
///     恢复等极短上下文），外加一句继续指令。不重复身份定位、不重复工具清单、
///     不回放历史对话。
pub fn build_manual_prompt(request: &ModelRequest) -> String {
    let prompt = if is_continuation(&request.messages) {
        build_continuation_prompt(request)
    } else {
        build_initial_prompt(request)
    };
    truncate_prompt(prompt)
}

/// 续轮判定：历史中已存在工具结果或助手工具调用，说明任务已在工具循环中。
fn is_continuation(messages: &[ChatMessage]) -> bool {
    messages.iter().any(|message| {
        message.role == Role::Tool
            || (message.role == Role::Assistant && !message.tool_calls.is_empty())
    })
}

/// 首轮：完整规则 + 对话上下文。
fn build_initial_prompt(request: &ModelRequest) -> String {
    let mut out = String::new();
    out.push_str(
        "你是 Coomi 的远程分析器。下面为你提供了：系统提示、可用工具清单与完整的对话上下文。\n\
        请基于这些信息继续完成任务。\n\n",
    );

    let system = request
        .messages
        .iter()
        .filter(|message| message.role == Role::System)
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    if !system.trim().is_empty() {
        out.push_str(system.trim());
        out.push_str("\n\n");
    }

    out.push_str("## 可用工具（Available tools）\n");
    for tool in &request.tools {
        out.push_str(&format!("### {}\n{}\n", tool.name, tool.description));
        let params =
            serde_json::to_string_pretty(&tool.parameters).unwrap_or_else(|_| "{}".to_string());
        out.push_str(&format!(
            "参数（parameters）:\n```json\n{}\n```\n\n",
            params
        ));
    }

    out.push_str("## 输出格式（重要）\n");
    out.push_str("你只能输出以下两种内容之一：\n");
    out.push_str(
        "1) 需要调用工具时：输出严格的 JSON 工具调用数组（可以包在 ```json 代码块里）：\n",
    );
    out.push_str("```json\n[{\"name\":\"工具名\",\"arguments\":{...}}]\n```\n");
    out.push_str("2) 任务已经完成时：直接输出给用户的最终结论（纯文字，不要再输出工具调用）。\n\n");

    append_context(&mut out, request);
    out.push_str("## 请继续\n");
    out.push_str("根据以上上下文继续：还需要更多信息就输出工具调用 JSON，任务已完成就输出最终结论。\n");
    out
}

/// 续轮：只发送「最新进展」——最近一次工具执行结果（及 loop / 恢复等极短上下文），
/// 外加一句继续指令。不重复身份、不重复工具清单、不回放历史对话。
fn build_continuation_prompt(request: &ModelRequest) -> String {
    let mut out = String::new();
    out.push_str("## 请继续\n");

    let increment = latest_increment(&request.messages);
    if increment.is_empty() {
        out.push_str("（无新的工具执行结果）\n\n");
    } else {
        let mut had_images = false;
        for message in increment {
            if !message.images.is_empty() {
                had_images = true;
            }
            match message.role {
                Role::Tool => {
                    out.push_str(&format!("【最新工具结果】{}\n\n", message.content));
                }
                Role::User if message.internal => {
                    // loop / 断线恢复等极短上下文：直接附上，作为继续执行的锚点。
                    out.push_str(&format!("【执行上下文】{}\n\n", message.content));
                }
                _ => {}
            }
        }
        if had_images {
            out.push_str("【注】最新结果中包含图片，图片数据已省略（仅保留文字）。\n\n");
        }
    }

    out.push_str(
        "请根据以上最新进展继续：需要继续操作就输出 JSON 工具调用（格式与之前一致），\
         任务已完成就输出最终结论。\n",
    );
    out
}

/// 提取「最新增量」：最后一条助手消息之后的所有消息。
///
/// 外部 AI 会话已有完整记忆（身份、工具、历史都在首轮声明过），续轮只需
/// 同步「自上次粘贴以来发生的新事」，因此以「最后一条助手消息」为界：
///   - 普通工具循环：最后一条助手消息是「含工具调用的调用」，其后是本次
///     执行的最新一批工具结果（及工具结果的附加内部上下文）；
///   - loop 续跑 / 断线恢复：最后一条助手消息是上一轮的最终回答，其后只有
///     注入的 loop 目标 / 恢复指令等极短内部上下文——旧工具结果不回放。
fn latest_increment(messages: &[ChatMessage]) -> Vec<&ChatMessage> {
    match messages
        .iter()
        .rposition(|message| message.role == Role::Assistant)
    {
        Some(pos) => messages[pos + 1..].iter().collect(),
        None => Vec::new(),
    }
}

/// 渲染对话上下文（用户 / 助手 / 工具消息），首轮与续轮共用。
fn append_context(out: &mut String, request: &ModelRequest) {
    out.push_str("## 对话上下文\n");
    let mut had_images = false;
    for message in &request.messages {
        if !message.images.is_empty() {
            had_images = true;
        }
        match message.role {
            Role::System => {}
            Role::User if message.internal => {}
            Role::User => {
                out.push_str(&format!("【用户】{}\n\n", message.content));
            }
            Role::Assistant => {
                if !message.content.is_empty() {
                    out.push_str(&format!("【助手】{}\n", message.content));
                }
                for call in &message.tool_calls {
                    let args = serde_json::to_string(&call.arguments).unwrap_or_default();
                    out.push_str(&format!("【助手调用工具】{} {}\n", call.name, args));
                }
                out.push('\n');
            }
            Role::Tool => {
                out.push_str(&format!("【工具结果】{}\n\n", message.content));
            }
        }
    }
    if had_images {
        out.push_str("【注】上下文中包含图片，图片二进制数据已省略（仅保留文字）。\n\n");
    }
}

/// 超长截断保护：避免复制到剪贴板的提示词大到无法处理。
fn truncate_prompt(out: String) -> String {
    if out.chars().count() > MANUAL_PROMPT_MAX_CHARS {
        let mut truncated: String = out.chars().take(MANUAL_PROMPT_MAX_CHARS).collect();
        truncated.push_str("\n\n[提示词过长已截断，较早的历史已省略]");
        return truncated;
    }
    out
}

/// 人工回答的解析结果。
pub struct ParsedManualResponse {
    /// 展示给用户 / 回填上下文的内容：无工具调用时为完整回答，有工具调用时为叙述性文字。
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    /// 参数无法归一化为 JSON 对象的调用（与真实模型路径一致，交由 Agent 层处理）。
    pub invalid: Vec<InvalidToolCall>,
    pub warnings: Vec<String>,
}

/// 容错解析外部 AI 的回答。
///
/// 不同免费 AI（ChatGPT、Claude、文心一言等）对函数调用的格式化千差万别，解析器按序尝试：
///   1. OpenAI / Anthropic 函数调用 JSON（`tool_calls` 数组 / `output` 数组）。
///   2. 原始 JSON 数组 `[{name, arguments}]`。
///   3. 含 `tool` / `tool_call` / `function` 键的 JSON 对象。
///   4. Markdown ```json 围栏代码块。
///   5. XML 风格 `<tool_call><name>…</name><arguments>…</arguments></tool_call>`。
///   6. 行式 `TOOL: name ARGS: {...}` / `[tool]name[/tool]`。
///   7. 全文扫描已知工具名 + 紧随其后的 JSON 参数。
///
/// 未知工具名会被丢弃并给出警告（与 JsxposedX 一致），不会让一份「部分可用」的
/// 回答整体失败；参数无法解析的调用进入 `invalid`，由 Agent 层决定是否纠正。
pub fn parse_manual_response(raw: &str, tools: &[ToolSpec]) -> ParsedManualResponse {
    let known: Vec<String> = tools.iter().map(|tool| tool.name.clone()).collect();
    let mut warnings: Vec<String> = Vec::new();

    if raw.trim().is_empty() {
        return ParsedManualResponse {
            content: String::new(),
            tool_calls: Vec::new(),
            invalid: Vec::new(),
            warnings: vec!["人工回复为空，未执行任何操作".to_string()],
        };
    }

    let mut extracted = try_structured(raw);
    if extracted.is_empty() {
        extracted = try_xml(raw);
    }
    if extracted.is_empty() {
        extracted = try_lines(raw);
    }
    if extracted.is_empty() {
        extracted = greedy_scan(raw, &known);
    }

    let (tool_calls, invalid) = filter_known(extracted, &known, &mut warnings);
    // 没有任何可执行的工具调用时，把整段回答当作最终结论展示。
    let content = if tool_calls.is_empty() {
        raw.trim().to_string()
    } else {
        extract_narrative(raw)
    };

    ParsedManualResponse {
        content,
        tool_calls,
        invalid,
        warnings,
    }
}

// ── 策略 1：结构化 JSON ──

fn try_structured(raw: &str) -> Vec<(String, Value)> {
    let mut candidates: Vec<String> = vec![raw.trim().to_string()];
    let fence_re = Regex::new(r"(?s)```(?:json)?\s*(.*?)```").expect("valid fence regex");
    for capture in fence_re.captures_iter(raw) {
        if let Some(body) = capture.get(1) {
            candidates.push(body.as_str().trim().to_string());
        }
    }
    if let (Some(start), Some(end)) = (raw.find('{'), raw.rfind('}'))
        && end > start
    {
        candidates.push(raw[start..=end].trim().to_string());
    }
    if let (Some(start), Some(end)) = (raw.find('['), raw.rfind(']'))
        && end > start
    {
        candidates.push(raw[start..=end].trim().to_string());
    }
    for candidate in candidates {
        if let Ok(value) = serde_json::from_str::<Value>(&candidate) {
            let calls = parse_decoded(&value);
            if !calls.is_empty() {
                return calls;
            }
        }
    }
    Vec::new()
}

fn parse_decoded(value: &Value) -> Vec<(String, Value)> {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_object)
            .filter_map(from_map)
            .collect::<Vec<(String, Value)>>(),
        Value::Object(map) => {
            for key in ["tool_calls", "toolCalls", "function_calls", "calls"] {
                if let Some(list) = map.get(key).and_then(Value::as_array) {
                    let calls = list
                        .iter()
                        .filter_map(Value::as_object)
                        .filter_map(from_map)
                        .collect::<Vec<(String, Value)>>();
                    if !calls.is_empty() {
                        return calls;
                    }
                }
            }
            // Anthropic Responses 风格的 output 数组（function_call 条目）。
            if let Some(list) = map.get("output").and_then(Value::as_array) {
                let mut calls = Vec::new();
                for item in list {
                    if item.get("type").and_then(Value::as_str) == Some("function_call")
                        && let Some(name) = item.get("name").and_then(Value::as_str)
                    {
                        let args = item
                            .get("arguments")
                            .cloned()
                            .unwrap_or_else(|| Value::Object(Map::new()));
                        calls.push((name.to_string(), coerce_args(args)));
                    }
                }
                if !calls.is_empty() {
                    return calls;
                }
            }
            if let Some(call) = from_map(map) {
                return vec![call];
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn from_map(map: &Map<String, Value>) -> Option<(String, Value)> {
    // 嵌套 function（OpenAI 风格）。
    if let Some(function) = map.get("function").and_then(Value::as_object) {
        let name = function
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let args = function
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new()));
        return if name.is_empty() {
            None
        } else {
            Some((name, coerce_args(args)))
        };
    }

    let name = ["name", "tool_name", "tool"]
        .iter()
        .find_map(|key| map.get(*key).and_then(Value::as_str))
        .or_else(|| {
            map.get("tool_call")
                .and_then(Value::as_object)
                .and_then(|object| object.get("name"))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return None;
    }
    let args = ["arguments", "args", "parameters", "input", "params"]
        .iter()
        .find_map(|key| map.get(*key))
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    Some((name, coerce_args(args)))
}

/// 把参数归一化为 JSON 对象；字符串形式（JSON 字符串）先解一层。
fn coerce_args(args: Value) -> Value {
    match args {
        Value::Object(_) => args,
        Value::String(text) => serde_json::from_str::<Value>(text.trim())
            .unwrap_or_else(|_| Value::Object(Map::new())),
        _ => Value::Object(Map::new()),
    }
}

// ── 策略 2：XML 风格标签 ──

fn try_xml(raw: &str) -> Vec<(String, Value)> {
    let re = Regex::new(
        r"(?is)<tool_call>\s*<name>\s*([^<]+?)\s*</name>\s*(?:<arguments>\s*(.*?)\s*</arguments>)?\s*</tool_call>",
    )
    .expect("valid xml regex");
    let mut calls = Vec::new();
    for capture in re.captures_iter(raw) {
        let name = capture
            .get(1)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        let args = capture
            .get(2)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        if !name.is_empty() {
            calls.push((name, coerce_args(Value::String(args))));
        }
    }
    calls
}

// ── 策略 3：行式语法 ──

fn try_lines(raw: &str) -> Vec<(String, Value)> {
    let line_re = Regex::new(
        r"(?im)^(?:TOOL|CALL|工具)\s*[:：]\s*([\w.-]+)\s*(?:ARGS|args|参数)\s*[:：]\s*(\{.*\})$",
    )
    .expect("valid line regex");
    let mut calls = Vec::new();
    for capture in line_re.captures_iter(raw) {
        let name = capture
            .get(1)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        let args = capture
            .get(2)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        if !name.is_empty() {
            calls.push((name, coerce_args(Value::String(args))));
        }
    }
    if calls.is_empty() {
        let bracket_re = Regex::new(
            r"(?i)\[tool\]\s*([\w.-]+?)\s*\[/tool\](?:\s*\[args\]\s*(\{.*?\})\s*\[/args\])?",
        )
        .expect("valid bracket regex");
        for capture in bracket_re.captures_iter(raw) {
            let name = capture
                .get(1)
                .map(|m| m.as_str().trim().to_string())
                .unwrap_or_default();
            let args = capture
                .get(2)
                .map(|m| m.as_str().trim().to_string())
                .unwrap_or_default();
            if !name.is_empty() {
                calls.push((name, coerce_args(Value::String(args))));
            }
        }
    }
    calls
}

// ── 策略 4：全文贪婪扫描（最后兜底）──

fn greedy_scan(raw: &str, known: &[String]) -> Vec<(String, Value)> {
    let mut calls = Vec::new();
    for name in known {
        let Some(mut index) = raw.find(name.as_str()) else {
            continue;
        };
        // 避免「my_read_file」这类包含已知名的标识符被误命中：名字边界必须干净。
        let mut matched = false;
        while index < raw.len() {
            let before = index == 0
                || !raw[..index]
                    .chars()
                    .next_back()
                    .is_some_and(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.');
            let after_end = index + name.len();
            let after = after_end >= raw.len()
                || !raw[after_end..]
                    .chars()
                    .next()
                    .is_some_and(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.');
            if before && after {
                matched = true;
                break;
            }
            match raw[index + name.len()..].find(name.as_str()) {
                Some(offset) => index += name.len() + offset,
                None => break,
            }
        }
        if !matched {
            continue;
        }
        let tail = &raw[index + name.len()..];
        if let (Some(bstart), Some(bend)) = (tail.find('{'), tail.rfind('}'))
            && bend > bstart
            && bstart <= SCAN_ARG_WINDOW
        {
            let args = tail[bstart..=bend].trim().to_string();
            calls.push((name.clone(), coerce_args(Value::String(args))));
            continue;
        }
        // 找不到 JSON 参数时不当作调用（仅提及工具名）。
    }
    calls
}

// ── 结果整理 ──

fn filter_known(
    calls: Vec<(String, Value)>,
    known: &[String],
    warnings: &mut Vec<String>,
) -> (Vec<ToolCall>, Vec<InvalidToolCall>) {
    let mut tool_calls = Vec::new();
    let mut invalid = Vec::new();
    for (index, (name, args)) in calls.into_iter().enumerate() {
        let name = name.trim().to_string();
        if name.is_empty() {
            continue;
        }
        let id = format!("manual-call-{}", index + 1);
        if !known.iter().any(|known| known == &name) {
            // 未知工具：丢弃并警告（与 JsxposedX 一致，不让部分可用回答整体失败）。
            warnings.push(format!("忽略未知工具: {}", name));
            continue;
        }
        match coerce_args(args) {
            Value::Object(object) => {
                tool_calls.push(ToolCall {
                    id,
                    name,
                    arguments: Value::Object(object),
                });
            }
            _ => invalid.push(InvalidToolCall {
                id,
                name,
                reason: "tool arguments are not a JSON object".to_string(),
            }),
        }
    }
    (tool_calls, invalid)
}

/// 提取回答中的叙述性文字（去掉围栏代码块 / XML 标签 / 行式工具调用）。
fn extract_narrative(raw: &str) -> String {
    let fence_re = Regex::new(r"(?s)```(?:json)?\s*.*?```").expect("valid fence regex");
    let xml_re = Regex::new(r"(?is)<tool_call>.*?</tool_call>").expect("valid xml regex");
    let line_re =
        Regex::new(r"(?im)^(?:TOOL|CALL|工具)\s*[:：].*$").expect("valid line regex");
    let text = fence_re.replace_all(raw, " ");
    let text = xml_re.replace_all(&text, " ");
    let text = line_re.replace_all(&text, " ");
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.to_string(),
            description: name.to_string(),
            parameters: json!({"type": "object"}),
        }
    }

    fn specs(names: &[&str]) -> Vec<ToolSpec> {
        names.iter().map(|name| spec(name)).collect()
    }

    #[test]
    fn parses_openai_tool_calls_json() {
        let raw = r#"{"tool_calls":[{"id":"1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"a.txt\"}"}}]}"#;
        let parsed = parse_manual_response(raw, &specs(&["read_file"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].name, "read_file");
        assert_eq!(parsed.tool_calls[0].arguments["path"], "a.txt");
    }

    #[test]
    fn parses_fenced_json_array() {
        let raw = "我会先读文件：\n```json\n[{\"name\":\"list_dir\",\"arguments\":{\"path\":\".\"}}]\n```\n然后继续。";
        let parsed = parse_manual_response(raw, &specs(&["list_dir"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].name, "list_dir");
        assert!(!parsed.content.is_empty());
    }

    #[test]
    fn parses_raw_json_array() {
        let raw = r#"[{"name":"shell","arguments":{"command":"pwd"}}]"#;
        let parsed = parse_manual_response(raw, &specs(&["shell"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].arguments["command"], "pwd");
    }

    #[test]
    fn parses_xml_style() {
        let raw = "<tool_call><name>read_file</name><arguments>{\"path\":\"b\"}</arguments></tool_call>";
        let parsed = parse_manual_response(raw, &specs(&["read_file"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].name, "read_file");
    }

    #[test]
    fn parses_line_syntax() {
        let raw = "TOOL: write_file ARGS: {\"path\":\"c\",\"content\":\"hi\"}";
        let parsed = parse_manual_response(raw, &specs(&["write_file"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].arguments["path"], "c");
    }

    #[test]
    fn drops_unknown_tools_with_warning() {
        let raw = r#"[{"name":"not_a_tool","arguments":{}},{"name":"list_dir","arguments":{}}]"#;
        let parsed = parse_manual_response(raw, &specs(&["list_dir"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert!(parsed.warnings.iter().any(|w| w.contains("not_a_tool")));
    }

    #[test]
    fn plain_text_answer_has_no_tools() {
        let raw = "任务已经完成：文件已生成。";
        let parsed = parse_manual_response(raw, &specs(&["read_file"]));
        assert!(parsed.tool_calls.is_empty());
        assert_eq!(parsed.content, raw);
    }

    #[test]
    fn greedy_scan_finds_known_tool_with_args() {
        let raw = "接下来我调用 read_file，参数是 {\"path\":\"x.txt\"}";
        let parsed = parse_manual_response(raw, &specs(&["read_file"]));
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].arguments["path"], "x.txt");
    }

    #[test]
    fn mention_without_args_is_not_a_call() {
        let raw = "可以使用 read_file 或 write_file 来完成这个任务，没有更多需要做的了。";
        let parsed = parse_manual_response(raw, &specs(&["read_file", "write_file"]));
        assert!(parsed.tool_calls.is_empty());
    }

    #[test]
    fn channel_roundtrip() {
        let channel = ManualChannel::new();
        let (_seq, mut receiver) = channel.request();
        assert!(channel.has_pending());
        assert!(channel.respond("answer".to_string()));
        assert!(!channel.has_pending());
        let text = receiver.try_recv();
        // oneshot 在异步上下文中才有意义，这里只验证发送成功即可。
        assert!(text.is_ok());
        assert!(!channel.respond("late".to_string()));
    }

    #[test]
    fn channel_cancel_clears_pending() {
        let channel = ManualChannel::new();
        let (_seq, mut receiver) = channel.request();
        assert!(channel.has_pending());
        channel.cancel();
        assert!(!channel.has_pending());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn channel_seq_increments_per_request() {
        let channel = ManualChannel::new();
        let (seq1, _rx1) = channel.request();
        channel.respond("a".to_string());
        let (seq2, _rx2) = channel.request();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
    }

    #[test]
    fn prompt_includes_system_tools_format_and_history() {
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system("SYSTEM_PROMPT_正文"),
                coomi_engine::ChatMessage::user("帮我写文件"),
            ],
            tools: vec![ToolSpec {
                name: "write_file".into(),
                description: "写入文件".into(),
                parameters: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
            }],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        assert!(prompt.contains("SYSTEM_PROMPT_正文"));
        assert!(prompt.contains("write_file"));
        assert!(prompt.contains("输出格式"));
        assert!(prompt.contains("帮我写文件"));
        // 首轮应包含完整工具参数 schema。
        assert!(prompt.contains("\"properties\""));
    }

    #[test]
    fn continuation_sends_only_latest_increment() {
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system("SYSTEM_PROMPT_正文（首轮已声明，续轮不应重复）"),
                coomi_engine::ChatMessage::user("帮我写文件"),
                coomi_engine::ChatMessage::assistant(
                    "",
                    vec![ToolCall {
                        id: "c1".into(),
                        name: "write_file".into(),
                        arguments: json!({"path": "a.txt"}),
                    }],
                ),
                coomi_engine::ChatMessage::tool("c1", "success: wrote 2 bytes"),
            ],
            tools: vec![ToolSpec {
                name: "write_file".into(),
                description: "写入文件".into(),
                parameters: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
            }],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        // 续轮只携带最新工具结果 + 继续指令。
        assert!(prompt.contains("success: wrote 2 bytes"));
        assert!(prompt.contains("## 请继续"));
        // 不重复系统提示。
        assert!(!prompt.contains("SYSTEM_PROMPT_正文（首轮已声明，续轮不应重复）"));
        // 不重复工具清单 / schema。
        assert!(!prompt.contains("write_file"));
        assert!(!prompt.contains("\"properties\""));
        // 不回放历史对话。
        assert!(!prompt.contains("帮我写文件"));
    }

    #[test]
    fn continuation_omits_identity_tools_and_history() {
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system(
                    "## Custom Identity (身份定位)\n我是一个严谨的工程师\n\nYou are Coomi, a pragmatic coding agent. 后续基础规则……",
                ),
                coomi_engine::ChatMessage::user("任务目标"),
                coomi_engine::ChatMessage::assistant(
                    "",
                    vec![ToolCall {
                        id: "c1".into(),
                        name: "read_file".into(),
                        arguments: json!({"path": "a.txt"}),
                    }],
                ),
                coomi_engine::ChatMessage::tool("c1", "success: 文件内容"),
            ],
            tools: vec![ToolSpec {
                name: "read_file".into(),
                description: "读取文件".into(),
                parameters: json!({"type": "object"}),
            }],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        // 不重复声明身份。
        assert!(!prompt.contains("Custom Identity"));
        assert!(!prompt.contains("我是一个严谨的工程师"));
        assert!(!prompt.contains("pragmatic coding agent"));
        // 不重复声明工具。
        assert!(!prompt.contains("read_file"));
        // 不回放历史对话（任务目标也不回放）。
        assert!(!prompt.contains("任务目标"));
        // 只带最新工具结果 + 继续指令。
        assert!(prompt.contains("文件内容"));
        assert!(prompt.contains("## 请继续"));
    }

    #[test]
    fn continuation_only_carries_the_latest_tool_batch() {
        // 多批次工具结果：续轮只发最后一批，更早的批次不回放。
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system("sys"),
                coomi_engine::ChatMessage::user("任务"),
                coomi_engine::ChatMessage::assistant(
                    "",
                    vec![ToolCall {
                        id: "c1".into(),
                        name: "list_dir".into(),
                        arguments: json!({}),
                    }],
                ),
                coomi_engine::ChatMessage::tool("c1", "第一批工具结果"),
                coomi_engine::ChatMessage::assistant(
                    "",
                    vec![ToolCall {
                        id: "c2".into(),
                        name: "read_file".into(),
                        arguments: json!({"path": "a.txt"}),
                    }],
                ),
                coomi_engine::ChatMessage::tool("c2", "第二批工具结果（最新）"),
            ],
            tools: vec![],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        assert!(prompt.contains("第二批工具结果（最新）"));
        assert!(!prompt.contains("第一批工具结果"));
    }

    #[test]
    fn continuation_carries_loop_context_without_replaying_history() {
        // loop 续跑：上一轮以最终回答结束，本轮只有注入的 loop 目标——
        // 只带 loop 内部上下文作为继续执行的锚点，不回放旧工具结果与旧回答。
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system("sys"),
                coomi_engine::ChatMessage::user("任务目标"),
                coomi_engine::ChatMessage::assistant(
                    "",
                    vec![ToolCall {
                        id: "c1".into(),
                        name: "list_dir".into(),
                        arguments: json!({}),
                    }],
                ),
                coomi_engine::ChatMessage::tool("c1", "旧的工具结果"),
                coomi_engine::ChatMessage::assistant("上一步完成", Vec::new()),
                coomi_engine::ChatMessage::internal_user(
                    "<loop_context>\nContinue working autonomously toward the active Loop objective: 持续构建项目\n</loop_context>",
                ),
            ],
            tools: vec![],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        assert!(prompt.contains("持续构建项目"));
        assert!(prompt.contains("## 请继续"));
        // 不回放历史：任务目标、旧工具结果、旧回答都不带。
        assert!(!prompt.contains("任务目标"));
        assert!(!prompt.contains("旧的工具结果"));
        assert!(!prompt.contains("上一步完成"));
    }

    #[test]
    fn new_turn_without_tool_history_is_initial() {
        // 会话里已有纯文字问答（无工具历史），新一轮仍是「首轮」→ 发送完整规则。
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system("SYSTEM_PROMPT_正文"),
                coomi_engine::ChatMessage::user("第一个问题"),
                coomi_engine::ChatMessage::assistant("第一个回答", Vec::new()),
                coomi_engine::ChatMessage::user("第二个问题"),
            ],
            tools: vec![ToolSpec {
                name: "read_file".into(),
                description: "读取文件".into(),
                parameters: json!({"type": "object"}),
            }],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        assert!(prompt.contains("SYSTEM_PROMPT_正文"));
        assert!(prompt.contains("第一个问题"));
        assert!(prompt.contains("第二个问题"));
    }

    #[test]
    fn prompt_skips_internal_user_messages() {
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![coomi_engine::ChatMessage::internal_user("内部指令不应出现")],
            tools: vec![],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        assert!(!prompt.contains("内部指令不应出现"));
    }

    #[test]
    fn prompt_truncates_overlong_context() {
        // 注意：不能使用连续 ASCII 字母（会被 sanitize_long_encoded_data 当作
        // base64 编码段替换掉），用非 ASCII 字符构造超长上下文。
        let big = "长".repeat(300_000);
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![coomi_engine::ChatMessage::user(big)],
            tools: vec![],
            reasoning_effort: None,
        };
        let prompt = build_manual_prompt(&request);
        assert!(prompt.chars().count() <= MANUAL_PROMPT_MAX_CHARS + 64);
        assert!(prompt.contains("提示词过长已截断"));
    }

    #[tokio::test]
    async fn provider_parses_tool_calls_and_notifies() {
        let channel = ManualChannel::new();
        let events: Arc<StdMutex<Vec<Value>>> = Arc::new(StdMutex::new(Vec::new()));
        let events_for_cb = Arc::clone(&events);
        let provider = ManualModelProvider::new(
            channel.clone(),
            Some(Arc::new(move |payload: Value| {
                events_for_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(payload);
            })),
        );
        let responder_channel = channel.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                if responder_channel.has_pending() {
                    let _ = responder_channel
                        .respond(r#"[{"name":"read_file","arguments":{"path":"a.txt"}}]"#.to_string());
                    break;
                }
            }
        });
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![
                coomi_engine::ChatMessage::system("sys"),
                coomi_engine::ChatMessage::user("hi"),
            ],
            tools: vec![ToolSpec {
                name: "read_file".into(),
                description: "read".into(),
                parameters: json!({"type": "object"}),
            }],
            reasoning_effort: None,
        };
        let response = provider.complete(request).await.expect("complete");
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].name, "read_file");
        assert_eq!(response.tool_calls[0].arguments["path"], "a.txt");
        let events = events.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_type"], "manual_request");
        assert!(events[0]["prompt"].as_str().unwrap_or("").contains("read_file"));
    }

    #[tokio::test]
    async fn provider_cancel_produces_error() {
        let channel = ManualChannel::new();
        let provider = ManualModelProvider::new(channel.clone(), None);
        let request = ModelRequest {
            model: "manual".into(),
            messages: vec![coomi_engine::ChatMessage::user("hi")],
            tools: vec![],
            reasoning_effort: None,
        };
        // 先注册请求再取消，模拟任务被用户停止：complete 应以错误收场而非悬挂。
        let task = tokio::spawn(async move { provider.complete(request).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        channel.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("complete did not finish after cancel")
            .expect("join failed");
        assert!(result.is_err());
    }
}
