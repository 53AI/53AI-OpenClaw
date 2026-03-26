# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.9] - 2026-03-26

### Changed
- 更新 `homepage` 字段为 `https://www.53ai.com`。

## [1.0.8] - 2026-03-26

### Added
- 新增压缩前后钩子（`handleBeforeCompaction` / `handleAfterCompaction`），支持在会话消息压缩前后向用户发送状态通知。
- 新增 `compaction-hooks.ts` 模块，实现从 `sessionKey` 解析 `chatId` 的逻辑，并对非本渠道会话自动跳过处理。
- 消息发送逻辑新增思考状态（thinking）支持，压缩过程中可向用户推送"正在整理对话记忆"等提示。

### Changed
- `message-sender.ts`：优化消息发送调用，支持传递思考状态标志。
- `monitor.ts`：增强监控逻辑以适配压缩钩子的事件上报。

## [1.0.7] - 2026-03-19

### Changed
- 规范配置项命名：将 `websocketUrl` 统一更名为 `WSUrl`。
- 文档更新：优化了 README.md 中的参数说明。

## [1.0.6] - 2026-03-19

### Changed
- 文档更新：修正了 `botId` 和 `secret` 的说明。

## [1.0.5] - 2026-03-19

### Changed
- 重构了接口名称，将 `Hub53AIMessageData` 简化为 `MessageData`。
- 代码格式优化。

## [1.0.4] - 2026-03-19

### Fixed
- 修复了 TypeScript 语法错误（重命名了以数字开头的接口名）。
- 修复了项目构建依赖问题。

### Changed
- 将内部接口重命名为 `Hub53AI` 前缀，以符合 TypeScript 标识符规范。

## [1.0.1] - 2026-03-18

### Changed
- 更新插件名称和 ID 为 `53ai-openclaw`。
- 文档更新和规范化定义。

## [1.0.0] - 2025-03-17

### Added
- Initial release
- WebSocket real-time communication support
- Direct message (DM) mode support
- Multi-modal message support (images, files)
- "Thinking" message support for user feedback
- Access policy control (open, allowlist, pairing)
- Persistent request ID storage

### Changed
- Security fix: Removed credentials from URL query parameters
- Fixed empty catch blocks with proper logging
- Fixed potential WebSocket memory leak with cleanup on all error paths
- Fixed message race condition with sequential queue processing
- Reduced max reconnect attempts from 100 to 10

### Fixed
- Type safety improvements: replaced `any` types with proper interfaces
