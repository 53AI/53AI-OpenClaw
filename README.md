# 53AIHub OpenClaw 插件

53AIHub (AgentHub) 智能机器人接入 OpenClaw 的通道插件。

## 功能

- 将 53AIHub/AgentHub 智能体接入 OpenClaw
- 支持直接消息 (DM) 模式
- 支持 WebSocket 实时通信
- **支持多模态消息**（图片、文件）
- 支持 AI 生成的图片/文件发送给用户
- **支持"思考中"消息** - 用户发送消息后立即收到反馈

## 安装方式

### 方式一：本地开发调试安装

适用于插件开发阶段，支持热更新。

```bash
# 在插件目录下构建
cd 53aihub-openclaw-plugin
npm install
npm run build

# 方式 A: 链式安装 (推荐开发时使用，修改后只需重启 gateway)
openclaw plugins install -l /path/to/53aihub-openclaw-plugin

# 方式 B: 复制到扩展目录
mkdir -p ~/.openclaw/extensions/53aihub
cp -r dist openclaw.plugin.json package.json ~/.openclaw/extensions/53aihub/
cd ~/.openclaw/extensions/53aihub && npm install --production
```

### 方式二：npm pack 打包安装

适用于分发 tarball 文件，无需 npm 发布。

**打包：**
```bash
cd 53aihub-openclaw-plugin
npm run clean && npm install && npm run build
npm pack
# 生成: 53aihub-1.0.0.tgz
```

**安装：**
```bash
# 从本地 tarball 安装
openclaw plugins install ./53aihub-1.0.0.tgz

# 或从远程 URL 安装
openclaw plugins install https://your-server.com/53aihub-1.0.0.tgz
```

### 方式三：从 npm 安装（待实现）

```bash
openclaw plugins install 53aihub-openclaw-plugin

# 或指定版本
openclaw plugins install 53aihub-openclaw-plugin@1.0.0
```

### 方式四：从 Git 仓库安装

```bash
openclaw plugins install git@gitcode.com:qunyy/53aihub-openclaw-plugin.git
```

## 配置

安装插件后，重启 Gateway 并配置通道：

```bash
# 重启 Gateway
openclaw gateway restart
# 或 systemd 服务
sudo systemctl restart openclaw-gateway

# 配置必要参数
openclaw config set channels.53aihub.botId "智能体详情的botId"
openclaw config set channels.53aihub.secret "智能体详情的botId对应的secret"
openclaw config set channels.53aihub.websocketUrl "ws://域名/api/v1/openclaw/ws/connect"

# 启用通道
openclaw config set channels.53aihub.enabled true
```

### 配置参数说明

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `botId` | 是 | - | AgentHub 智能体 ID (HashID) |
| `secret` | 是 | - | AgentHub App Secret / Token |
| `websocketUrl` | 是 | - | AgentHub WebSocket 连接地址 |
| `token` | 否 | - | secret 的别名，与 secret 二选一 |
| `enabled` | 否 | false | 是否启用通道 |
| `accessPolicy` | 否 | `open` | 访问策略: `open`=开放所有用户, `allowlist`=仅白名单用户, `pairing`=首次使用需审批, `disabled`=禁用 |
| `allowFrom` | 否 | - | 访问控制白名单，配合 accessPolicy=allowlist 或 pairing 使用 |
| `sendThinkingMessage` | 否 | true | 是否发送"思考中"提示消息 |

### 访问控制配置示例

**白名单模式** - 仅允许指定用户访问:
```bash
openclaw config set channels.53aihub.accessPolicy "allowlist"
openclaw config set channels.53aihub.allowFrom '["user-123", "user-456"]'
```

**配对审批模式** - 首次使用需管理员审批:
```bash
openclaw config set channels.53aihub.accessPolicy "pairing"
openclaw config set channels.53aihub.allowFrom '["user-789"]'
```

**开放模式** - 允许所有用户访问（默认）:
```bash
openclaw config set channels.53aihub.accessPolicy "open"
```

### 查看配置状态

```bash
# 查看通道状态
openclaw channels status

# 查看插件详情
openclaw plugins info 53aihub
```

## 验证安装

```bash
# 检查插件是否加载
openclaw plugins list | grep 53aihub

# 检查通道配置
openclaw config get channels.53aihub

# 检查 Gateway 日志 (用户级服务)
journalctl --user -u openclaw-gateway.service -f | grep -i 53aihub
```

## 多模态消息支持

插件支持接收和发送多模态消息（图片、文件）。

### 接收多模态消息

当用户发送包含图片或文件的消息时，插件会自动解析并处理：

**用户发送图片** → 插件接收 `AgentHubIncomingMessage`:
```typescript
{
  type: "message",
  msgId: "msg-xxx",
  chatId: "user-123",
  text: "这张图片是什么？",
  imageUrls: ["https://example.com/image.png"],
  contentItems: [
    { type: "text", text: "这张图片是什么？" },
    { type: "image", image: { url: "https://example.com/image.png" } }
  ]
}
```

**用户发送文件**:
```typescript
{
  type: "message",
  msgId: "msg-xxx",
  text: "请分析这个文档",
  fileUrls: ["https://example.com/document.pdf"],
  contentItems: [
    { type: "text", text: "请分析这个文档" },
    { type: "file", file: { url: "https://example.com/document.pdf", filename: "document.pdf" } }
  ]
}
```

### 发送媒体消息给用户

AI 可以主动发送图片或文件给用户：

**发送图片**:
```typescript
import { sendMediaMessage } from "./message-sender.js";

await sendMediaMessage(wsClient, "user-123", {
  type: "image",
  url: "https://example.com/generated-image.png",
  mimeType: "image/png"
}, "这是生成的图片");
```

**发送文件**:
```typescript
await sendMediaMessage(wsClient, "user-123", {
  type: "file",
  url: "https://example.com/report.pdf",
  filename: "report.pdf",
  mimeType: "application/pdf"
}, "分析报告已生成");
```

**发送 Base64 图片**:
```typescript
await sendMediaMessage(wsClient, "user-123", {
  type: "image",
  base64: "iVBORw0KGgoAAAANS...",
  mimeType: "image/png"
}, "图片已生成");
```

### 媒体消息格式

**发送格式** (`action: "message"`):
```json
{
  "req_id": "msg-xxx",
  "action": "message",
  "status": "final",
  "data": {
    "toChatId": "user-123",
    "text": "这是图片描述",
    "media": {
      "type": "image",
      "url": "https://example.com/image.png",
      "mimeType": "image/png"
    }
  }
}
```

**Media 字段说明**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | `"image"` 或 `"file"` |
| `url` | string | 媒体文件 URL（与 base64 二选一） |
| `base64` | string | Base64 编码数据（与 url 二选一） |
| `mimeType` | string | MIME 类型，如 `image/png`、`application/pdf` |
| `filename` | string | 文件名（仅 file 类型需要） |

## 卸载

```bash
# 清理配置
openclaw config delete channels.53aihub

# 移除插件
openclaw plugins uninstall 53aihub-openclaw-plugin --force
```

## 开发

```bash
# 安装依赖
npm install

# 构建
npm run build

# 监听模式 (开发时)
npm run dev

# 清理构建产物
npm run clean
```

## 故障排查

### 插件未加载

1. 确认 `openclaw.plugin.json` 存在于插件根目录
2. 确认 `dist/` 目录包含编译后的文件
3. 检查 Gateway 日志: `journalctl --user -u openclaw-gateway.service -f`

### 通道未生效

1. 确认 `channels.53aihub.enabled` 为 `true`
2. 确认必填配置项已设置
3. 重启 Gateway: `openclaw gateway restart`

### WebSocket 连接失败

1. 检查 `websocketUrl` 格式是否正确
2. 确认网络可达性
3. 检查 `botId` 和 `secret` 是否正确

## License

MIT
