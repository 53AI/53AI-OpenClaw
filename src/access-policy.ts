import type { RuntimeEnv } from "openclaw/plugin-sdk";
import type { WebSocket } from "ws";
import { getRuntime } from "./runtime.js";
import type { ResolvedAccount } from "./utils.js";
import { CHANNEL_ID } from "./const.js";

interface AccessPolicyOptions {
  userId: string;
  account: ResolvedAccount;
  wsClient: WebSocket;
  runtime: RuntimeEnv;
}

interface AccessPolicyResult {
  allowed: boolean;
  reason?: string;
}

/**
 * 访问策略检查
 * 
 * 策略模式：
 * - "open": 允许所有用户（默认）
 * - "allowlist": 仅允许 allowFrom 中的用户
 * - "pairing": 首次使用需要管理员审批
 */
export async function checkAccessPolicy(options: AccessPolicyOptions): Promise<AccessPolicyResult> {
  const { userId, account, runtime } = options;
  const core = getRuntime();

  const accessPolicy = account.config.accessPolicy ?? "open";

  // 开放模式 - 所有用户都能用
  if (accessPolicy === "open") {
    return { allowed: true };
  }

  // 白名单模式
  if (accessPolicy === "allowlist") {
    const allowFrom = account.config.allowFrom ?? [];
    if (allowFrom.includes(userId)) {
      return { allowed: true };
    }
    runtime.log?.(`[53aihub] User ${userId} not in allowlist`);
    return { allowed: false, reason: `User ${userId} not in allowlist` };
  }

  // 配对模式 - 首次使用需要审批
  if (accessPolicy === "pairing") {
    // 检查配置中的白名单
    const allowFrom = account.config.allowFrom ?? [];
    if (allowFrom.includes(userId)) {
      return { allowed: true };
    }

    // 检查持久化的配对记录
    const persistentAllowFrom = await core.channel.pairing.readAllowFromStore({
      channel: CHANNEL_ID,
      accountId: account.accountId,
    });

    if (persistentAllowFrom.includes(userId)) {
      return { allowed: true };
    }

    // 记录配对请求
    runtime.log?.(`[53aihub] User ${userId} requires pairing. Recording request.`);
    await core.channel.pairing.upsertPairingRequest({
      channel: CHANNEL_ID,
      accountId: account.accountId,
      id: userId,
      meta: {
        displayName: `53AIHub 用户 (${userId})`,
      },
    });

    return { allowed: false, reason: "Pairing required" };
  }

  return { allowed: false };
}