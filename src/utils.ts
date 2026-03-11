import type { OpenClawConfig } from "openclaw/plugin-sdk";
import { DEFAULT_ACCOUNT_ID } from "openclaw/plugin-sdk";
import { CHANNEL_ID, DEFAULT_WS_URL } from "./const.js";

// ============================================================================
// 配置类型定义
// ============================================================================

/**
 * 53AIHub 渠道配置
 */
export interface AIHubConfig {
  /** 是否启用 */
  enabled?: boolean;
  /** 账户名称 */
  name?: string;
  /** 机器人 ID */
  botId?: string;
  /** 用户 ID */
  userId?: string;
  /** 密钥 */
  secret?: string;
  /** 访问令牌 */
  token?: string;
  /** WebSocket URL */
  websocketUrl?: string;
  
  /** 访问控制白名单 */
  allowFrom?: Array<string | number>;
  /** 访问策略: open=开放, allowlist=白名单, pairing=配对审批 */
  accessPolicy?: "open" | "allowlist" | "pairing";
  
  /** 是否发送"思考中"消息 */
  sendThinkingMessage?: boolean;
}

/**
 * 解析后的账户配置
 */
export interface ResolvedAccount {
  accountId: string;
  name: string;
  enabled: boolean;
  websocketUrl: string;
  botId: string;
  secret: string;
  token: string;
  sendThinkingMessage: boolean;
  config: AIHubConfig;
}

// ============================================================================
// 配置解析函数
// ============================================================================

/**
 * 解析账户配置
 */
export function resolveAccount(cfg: OpenClawConfig, accountId = DEFAULT_ACCOUNT_ID): ResolvedAccount {
  const config = (cfg.channels?.[CHANNEL_ID] ?? {}) as AIHubConfig;
  
  return {
    accountId,
    name: config.name ?? "53AIHub",
    enabled: config.enabled !== false,
    websocketUrl: config.websocketUrl || DEFAULT_WS_URL,
    botId: config.botId ?? config.userId ?? "",
    secret: config.secret ?? config.token ?? "",
    token: config.token ?? config.secret ?? "",
    sendThinkingMessage: config.sendThinkingMessage ?? true,
    config,
  };
}

/**
 * 设置账户配置
 */
export function setAccount(
  cfg: OpenClawConfig,
  account: Partial<AIHubConfig>
): OpenClawConfig {
  const existing = (cfg.channels?.[CHANNEL_ID] ?? {}) as AIHubConfig;
  const merged: AIHubConfig = {
    enabled: account.enabled ?? existing.enabled ?? true,
    botId: account.botId ?? existing.botId ?? "",
    secret: account.secret ?? existing.secret ?? "",
    token: account.token ?? existing.token ?? "",
    allowFrom: account.allowFrom ?? existing.allowFrom,
    accessPolicy: account.accessPolicy ?? existing.accessPolicy,
    sendThinkingMessage: account.sendThinkingMessage ?? existing.sendThinkingMessage,
    ...(account.websocketUrl || existing.websocketUrl
      ? { websocketUrl: account.websocketUrl ?? existing.websocketUrl }
      : {}),
    ...(account.name || existing.name
      ? { name: account.name ?? existing.name }
      : {}),
  };

  return {
    ...cfg,
    channels: {
      ...cfg.channels,
      [CHANNEL_ID]: merged,
    },
  };
}