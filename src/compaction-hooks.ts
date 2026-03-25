import { getRuntime } from "./runtime.js";
import { CHANNEL_ID } from "./const.js";

interface BeforeCompactionEvent {
  messageCount: number;
  compactingCount?: number;
  tokenCount?: number;
  messages?: unknown[];
  sessionFile?: string;
}

interface AfterCompactionEvent {
  messageCount: number;
  tokenCount?: number;
  compactedCount: number;
  sessionFile?: string;
}

interface AgentContext {
  agentId?: string;
  sessionKey?: string;
  sessionId?: string;
  workspaceDir?: string;
  messageProvider?: string;
  trigger?: string;
  channelId?: string;
}

const compactionSessions = new Map<string, {
  startTime: number;
  messageCount: number;
}>();

/**
 * 从 sessionKey 中解析 chatId
 * sessionKey 格式: agent:{agentId}:{channel}:direct:{chatId}
 * 例如: agent:main:53aihub:direct:user123 -> user123
 */
function parseChatIdFromSessionKey(sessionKey: string): string | null {
  if (!sessionKey) return null;
  const parts = sessionKey.split(":");
  // 格式: agent:{agentId}:{channel}:direct:{chatId}
  // parts: [0]agent, [1]agentId, [2]channel, [3]direct, [4]chatId
  if (parts.length >= 5 && parts[2] === CHANNEL_ID && parts[3] === "direct") {
    return parts[4];
  }
  return null;
}

export async function handleBeforeCompaction(
  event: BeforeCompactionEvent,
  ctx: AgentContext
): Promise<void> {
  const runtime = getRuntime();
  const logger = runtime.logging.getChildLogger({ component: "53aihub-compaction" });
  const log = (msg: string) => logger.info(msg);

  log(`before_compaction: sessionKey=${ctx.sessionKey}, channelId=${ctx.channelId}, messageCount=${event.messageCount}`);

  const sessionKey = ctx.sessionKey || "";
  if (!sessionKey.startsWith(`${CHANNEL_ID}:`) && !sessionKey.includes(`:${CHANNEL_ID}:`)) {
    log(`Skipping compaction hook for non-53aihub session: ${sessionKey}`);
    return;
  }

  const chatId = parseChatIdFromSessionKey(sessionKey);
  if (!chatId) {
    logger.error(`Cannot parse chatId from sessionKey: ${sessionKey}`);
    return;
  }

  compactionSessions.set(sessionKey, {
    startTime: Date.now(),
    messageCount: event.messageCount,
  });

  log(`Compaction started for chatId=${chatId}`);
}

export async function handleAfterCompaction(
  event: AfterCompactionEvent,
  ctx: AgentContext
): Promise<void> {
  const runtime = getRuntime();
  const logger = runtime.logging.getChildLogger({ component: "53aihub-compaction" });
  const log = (msg: string) => logger.info(msg);

  log(`after_compaction: sessionKey=${ctx.sessionKey}, channelId=${ctx.channelId}, messageCount=${event.messageCount}, compactedCount=${event.compactedCount}`);

  const sessionKey = ctx.sessionKey || "";
  if (!sessionKey.startsWith(`${CHANNEL_ID}:`) && !sessionKey.includes(`:${CHANNEL_ID}:`)) {
    log(`Skipping compaction hook for non-53aihub session: ${sessionKey}`);
    return;
  }

  const chatId = parseChatIdFromSessionKey(sessionKey);
  if (!chatId) {
    logger.error(`Cannot parse chatId from sessionKey: ${sessionKey}`);
    return;
  }

  const sessionInfo = compactionSessions.get(sessionKey);
  compactionSessions.delete(sessionKey);

  if (sessionInfo) {
    const duration = Date.now() - sessionInfo.startTime;
    log(`Compaction completed: chatId=${chatId}, duration=${duration}ms, messagesBefore=${sessionInfo.messageCount}, messagesAfter=${event.messageCount}, compacted=${event.compactedCount}`);
  }
}