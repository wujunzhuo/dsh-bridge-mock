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
python3 -m http.server 5173 --directory web-app
```

打开 `http://127.0.0.1:5173`，设置 WebSocket 地址
`ws://127.0.0.1:8080/api/remote.mux` 和租户 API Key。页面使用 MIT 开源库
[`json-rpc-2.0`](https://github.com/shogowada/json-rpc-2.0) 处理 JSON-RPC 请求、响应和
通知；该无依赖库由 esm.sh 加载，因此浏览器需要可访问 esm.sh。

页面将 `session.event` 中来自 dsh `--json` 投影的 `thinking`、`text`、`tool_call`、
`tool_result` 和 `status` 事件显示为回复、折叠思考块和工具卡片。事件定义来自
[`packages/bundle/headless/src/json-stream.ts`](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/bundle/headless/src/json-stream.ts)。
思考内容是否出现取决于模型/provider；dsh 的文本和思考事件在步骤提交后发出，并非逐 token 流式输出。
