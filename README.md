# deepseek-harness 多租户后端示例

Rust 项目只提供多租户认证和 `/api/remote.mux` WebSocket JSON-RPC 后端，不托管
HTML/JS。每个会话由服务启动 `dsh --profile headless --json` 子进程；子进程退出后，
同一会话后续请求通过 dsh 原生 Session ID 恢复。服务侧 `session_id` 关联租户，
`process_id` 是最近一次 dsh 子进程的 OS PID。

## 配置模型

在 `tenants.json` 中分别配置服务认证 key 和底层 LLM 信息：

```json
[
  {
    "api_key": "sk-tenant-a",
    "tenant_id": "tenant-a",
    "name": "Tenant A",
    "llm": {
      "base_url": "https://api.vivgrid.com/v1",
      "api_key": "YOUR_VIVGRID_API_KEY",
      "model": "hy4-flash"
    }
  }
]
```

`llm.base_url` 必须是 OpenAI Chat Completions 兼容的 endpoint。启动 dsh 时，服务
使用 `llm.api_key` 设置 `DSH_TENANT_MODEL_API_KEY`，并通过临时 `--patch` 将
`llm.base_url`、`llm.model` 配置到 `openai-completions` provider 和默认模型。
请将示例 key 替换为真实密钥，不要提交真实密钥到版本库。

## 启动后端

```sh
cargo run
```

默认监听 `127.0.0.1:8080`，dsh 默认从 PATH 查找。可配置：

```sh
DSH_COMMAND=/path/to/dsh \
DSH_PROFILE=headless \
BIND_ADDR=127.0.0.1:8080 \
TENANTS_FILE=tenants.json \
cargo run
```

## 独立前端测试页

前端和 Rust 后端完全分离，位于 `web-app/`。可用任意静态文件服务器运行，例如：

```sh
python -m http.server 5173 --directory web-app
```

打开 `http://127.0.0.1:5173`，设置 WebSocket 地址
`ws://127.0.0.1:8080/api/remote.mux` 和租户 API Key。页面使用 MIT 开源库
[`json-rpc-2.0`](https://github.com/shogowada/json-rpc-2.0) 处理 JSON-RPC 请求、响应和
通知；该无依赖库由 esm.sh 加载，因此浏览器需要可访问 esm.sh。

页面将 `session.event` 中来自 dsh `--json` 投影的 `thinking`、`text`、`tool_call`、
`tool_result` 和 `status` 事件显示为回复、折叠思考块和工具卡片。事件定义来自
[`packages/bundle/headless/src/json-stream.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/bundle/headless/src/json-stream.ts)。
思考内容是否出现取决于模型/provider；dsh 的文本和思考事件在步骤提交后发出，并非逐 token 流式输出。

## Python 客户端

安装 WebSocket 依赖后，可启动交互式终端客户端。API Key 是 `tenants.json` 顶层的租户
`api_key`；也可以通过 `DSH_API_KEY` 环境变量提供：

```sh
uv pip install websockets
python client.py --api-key sk-tenant-a "帮我查一下明天北京的天气"
```

客户端会在界面标题中显示服务端分配的 `session_id`，可用它继续已有会话；这里的
`session_id` 是桥接服务 ID，不是 dsh 原生 session ID。客户端使用分区终端界面：上方显示
agent 回复、思考和工具调用，下方专用于输入。WebSocket 会保持连接，可随时输入补充信息、
确认或审批内容；agent 处理期间输入的内容也会排队发送。客户端始终为交互模式，需要 TTY。

```sh
python client.py --api-key sk-tenant-a --session_id <session-id> "继续刚才的话题"
# 不发送 prompt，回放已有事件并持续监听 session
python client.py --api-key sk-tenant-a --session_id <session-id>
```

在交互提示符输入 `/quit` 会通知服务端销毁当前 session、停止其 dsh 后端并退出客户端。
销毁后再次使用该 `session_id` 会创建一个新的服务端 session。

`--session_id` 且不传 prompt 时会回放历史事件并持续监听，也可直接输入新的 prompt。输入
`/quit` 会销毁服务端 session 并退出；Ctrl+C 仅退出客户端。输出区用 PageUp/PageDown 滚动；
客户端不接管鼠标，保留终端原生拖选/复制。默认服务地址为 `ws://127.0.0.1:8080/api/remote.mux`，
可用 `--url` 覆盖；
`--after-event-id` 可指定历史回放的起点。
