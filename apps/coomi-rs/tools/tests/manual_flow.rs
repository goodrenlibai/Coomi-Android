//! 人工模式端到端流程测试：完整 Agent 循环 + 真实 CoreTools 执行。
//!
//! 模拟「无 API 用户」的完整交互：
//!   用户下达任务 → 引擎推提示词 → 用户粘贴「工具调用」→ 引擎执行工具 →
//!   再次推提示词（含工具结果）→ 用户粘贴「最终结论」→ 本轮结束。

use async_trait::async_trait;
use coomi_engine::Agent;
use coomi_engine::ApprovalHandler;
use coomi_engine::NoopObserver;
use coomi_engine::Role;
use coomi_engine::Session;
use coomi_engine::ToolCall;
use coomi_security::AccessMode;
use coomi_security::SecurityPolicy;
use coomi_services::ManualChannel;
use coomi_services::ManualModelProvider;
use coomi_tools::CoreTools;
use std::time::Duration;

struct ApproveAll;

#[async_trait]
impl ApprovalHandler for ApproveAll {
    async fn approve(&self, _call: &ToolCall, _reason: &str) -> bool {
        true
    }
}

/// 脚本化人工应答器：按顺序消费等待中的请求，回放预设回答。
/// 与真实用户「复制提示词 → 粘贴回答」的节奏一致。
fn spawn_responder(channel: ManualChannel, script: Vec<String>) {
    tokio::spawn(async move {
        let mut index = 0usize;
        while index < script.len() {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if channel.has_pending() {
                let answer = script[index].clone();
                channel.respond(answer);
                index += 1;
            }
        }
    });
}

#[tokio::test]
async fn manual_agent_executes_tools_then_concludes() {
    let workspace = tempfile::tempdir().expect("workspace");
    let cwd = workspace.path().to_path_buf();
    let channel = ManualChannel::new();
    let provider = ManualModelProvider::new(channel.clone(), None);

    // 第一轮：写文件；第二轮：读文件；第三轮：最终结论。
    spawn_responder(
        channel,
        vec![
            r#"[{"name":"write_file","arguments":{"path":"hello.txt","content":"hi"}}]"#.to_string(),
            r#"[{"name":"read_file","arguments":{"path":"hello.txt"}}]"#.to_string(),
            "文件已写好并验证。".to_string(),
        ],
    );

    let policy = SecurityPolicy::new(&cwd, AccessMode::WorkspaceWrite).expect("policy");
    let tools = CoreTools::new(cwd.clone(), policy);
    let agent = Agent::new("你是一个测试用的本地编码代理。");
    let mut session = Session::new("manual", "manual", cwd.clone());

    let output = tokio::time::timeout(
        Duration::from_secs(30),
        agent.run_turn(
            &mut session,
            "帮我写一个 hello.txt 再读回来",
            &provider,
            &tools,
            &ApproveAll,
            &NoopObserver,
        ),
    )
    .await
    .expect("turn timed out")
    .expect("turn failed");

    assert_eq!(output, "文件已写好并验证。");
    // 工具真实执行：文件落盘且内容正确。
    let content = std::fs::read_to_string(workspace.path().join("hello.txt")).expect("read file");
    assert_eq!(content, "hi");
    // 会话历史完整保留工具调用与结果（写入 + 读取两条 tool 消息）。
    let tool_messages = session
        .messages
        .iter()
        .filter(|message| message.role == Role::Tool)
        .count();
    assert!(tool_messages >= 2, "expected tool results, got {tool_messages}");
    assert!(session
        .messages
        .iter()
        .any(|message| message.role == Role::Tool && message.content.contains("success")));
}

#[tokio::test]
async fn manual_plain_answer_concludes_without_tools() {
    let workspace = tempfile::tempdir().expect("workspace");
    let cwd = workspace.path().to_path_buf();
    let channel = ManualChannel::new();
    let provider = ManualModelProvider::new(channel.clone(), None);
    spawn_responder(
        channel,
        vec!["没有需要执行的操作，任务已完成。".to_string()],
    );

    let policy = SecurityPolicy::new(&cwd, AccessMode::WorkspaceWrite).expect("policy");
    let tools = CoreTools::new(cwd.clone(), policy);
    let agent = Agent::new("测试系统提示。");
    let mut session = Session::new("manual", "manual", cwd.clone());

    let output = agent
        .run_turn(
            &mut session,
            "简单回答即可",
            &provider,
            &tools,
            &ApproveAll,
            &NoopObserver,
        )
        .await
        .expect("turn failed");

    assert_eq!(output, "没有需要执行的操作，任务已完成。");
    assert!(session
        .messages
        .iter()
        .all(|message| message.role != Role::Tool));
}

#[tokio::test]
async fn manual_greedy_scan_still_executes_known_tools() {
    // 兜底路径：外部 AI 只「提及」工具名 + JSON 参数（非严格 JSON 数组），也应执行。
    let workspace = tempfile::tempdir().expect("workspace");
    let cwd = workspace.path().to_path_buf();
    let channel = ManualChannel::new();
    let provider = ManualModelProvider::new(channel.clone(), None);
    spawn_responder(
        channel,
        vec![
            "我会先写文件：write_file 参数 {\"path\":\"note.txt\",\"content\":\"ok\"}".to_string(),
            "完成。".to_string(),
        ],
    );

    let policy = SecurityPolicy::new(&cwd, AccessMode::WorkspaceWrite).expect("policy");
    let tools = CoreTools::new(cwd.clone(), policy);
    let agent = Agent::new("测试系统提示。");
    let mut session = Session::new("manual", "manual", cwd.clone());

    let output = agent
        .run_turn(
            &mut session,
            "写一个 note.txt",
            &provider,
            &tools,
            &ApproveAll,
            &NoopObserver,
        )
        .await
        .expect("turn failed");

    assert_eq!(output, "完成。");
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("note.txt")).expect("read"),
        "ok"
    );
}
