# deepseek-harness 多应用后端示例

## 设计

- `app_id` 标识应用；应用 API Key 用于 WebSocket 认证。
- 一个 session 可包含多个 turn。每个 turn 由 backend 独立运行；应用层维护 user/assistant 历史并作为后续上下文。
- session 的事件历史和消息由应用层接口管理；示例使用内存实现。每个 session 共享独立 workspace，示例使用临时目录，服务退出时清理。
- backend、session 存储和 workspace 都有接口，便于替换为 sandbox、数据库或 blob 存储实现。

## 使用

复制示例配置并填写应用 API Key 和模型 API Key：

```sh
cp default_config.json config.json
```

启动服务（默认读取 `config.json`，默认监听 `127.0.0.1:8080`）：

```sh
cargo run
```

可通过 `CONFIG_FILE`、`BIND_ADDR`、`DSH_COMMAND` 和 `DSH_PROFILE` 覆盖配置文件、监听地址及 dsh 命令/配置。
`config.json` 已加入 `.gitignore`，不要提交密钥。

独立网页客户端：

```sh
python -m http.server 5173 --directory web-app
```

打开 `http://127.0.0.1:5173`，填写 `ws://127.0.0.1:8080/api/remote.mux` 和配置中的应用 `api_key`。

也可使用 Python 客户端：

```sh
uv pip install websockets
python client.py --api-key <app-api-key>
```

客户端启动后在界面中输入问题。

使用返回的 `session_id` 可继续会话：

```sh
python client.py --api-key <app-api-key> --session_id <session-id>
```

命令行中的 `/quit` 只退出客户端；`/clear` 和网页的“新对话”会销毁当前 session/workspace，再开始新会话。
