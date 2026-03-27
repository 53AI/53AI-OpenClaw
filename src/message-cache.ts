import type { RuntimeEnv } from "openclaw/plugin-sdk";
import type { WebSocket } from "ws";
import { MESSAGE_CACHE_TTL_MS } from "./const.js";

/**
 * 缓存的消息条目
 */
interface CachedMessage {
  /** 缓存唯一 key */
  cacheKey: string;
  /** 消息 ID */
  reqId: string;
  /** 目标聊天 ID */
  toChatId: string;
  /** 消息内容 */
  text: string;
  /** 是否为最终消息 */
  finish: boolean;
  /** 是否为错误消息 */
  isError?: boolean;
  /** 错误码 */
  errorCode?: string;
  /** 错误详情 */
  errorDetails?: string;
  /** 是否为思考消息 */
  isThinking?: boolean;
  /** 创建时间戳 */
  createdAt: number;
  /** 重试次数 */
  retryCount: number;
}

/**
 * 消息缓存配置
 */
interface MessageCacheConfig {
  maxEntries: number;
  ttlMs: number;
  maxRetries: number;
}

const DEFAULT_CONFIG: MessageCacheConfig = {
  maxEntries: 100,
  ttlMs: MESSAGE_CACHE_TTL_MS,
  maxRetries: 3,
};

/**
 * 按 accountId 隔离的消息缓存管理器
 */
class MessageCache {
  private cache: Map<string, CachedMessage> = new Map();
  private config: MessageCacheConfig;
  private accountId: string;
  private runtime: RuntimeEnv | undefined;

  constructor(accountId: string, config: Partial<MessageCacheConfig> = {}, runtime?: RuntimeEnv) {
    this.accountId = accountId;
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.runtime = runtime;
  }

  /**
   * 生成唯一缓存 key
   */
  private generateCacheKey(reqId: string): string {
    return `${this.accountId}:${reqId}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
  }

  /**
   * 添加消息到缓存，返回缓存 key
   */
  add(params: {
    reqId: string;
    toChatId: string;
    text: string;
    finish: boolean;
    isError?: boolean;
    errorCode?: string;
    errorDetails?: string;
    isThinking?: boolean;
  }): string {
    this.prune();

    if (this.cache.size >= this.config.maxEntries) {
      // 优先淘汰非终态消息（finish=false 且 isError=false），保留终态消息
      const entries = [...this.cache.entries()];
      
      // 先找最老的非终态消息
      const oldestNonFinal = entries
        .filter(([, entry]) => !entry.finish && !entry.isError)
        .sort((a, b) => a[1].createdAt - b[1].createdAt)[0];
      
      if (oldestNonFinal) {
        this.cache.delete(oldestNonFinal[0]);
        this.runtime?.log?.(`[53aihub] MessageCache: evicted oldest non-final entry ${oldestNonFinal[0]}`);
      } else {
        // 所有消息都是终态，才淘汰最老的
        const oldest = entries.sort((a, b) => a[1].createdAt - b[1].createdAt)[0];
        if (oldest) {
          this.cache.delete(oldest[0]);
          this.runtime?.log?.(`[53aihub] MessageCache: evicted oldest final entry ${oldest[0]} (all entries are final)`);
        }
      }
    }

    const cacheKey = this.generateCacheKey(params.reqId);
    this.cache.set(cacheKey, {
      cacheKey,
      ...params,
      createdAt: Date.now(),
      retryCount: 0,
    });

    this.runtime?.log?.(`[53aihub] MessageCache: cached message reqId=${params.reqId}, cacheKey=${cacheKey}, cacheSize=${this.cache.size}`);
    return cacheKey;
  }

  /**
   * 获取所有待重试的消息，返回 cacheKey + entry
   */
  getPendingMessages(): CachedMessage[] {
    const now = Date.now();
    const pending: CachedMessage[] = [];

    for (const [key, entry] of this.cache) {
      if (now - entry.createdAt > this.config.ttlMs) {
        this.cache.delete(key);
        continue;
      }

      if (entry.retryCount >= this.config.maxRetries) {
        this.cache.delete(key);
        this.runtime?.log?.(`[53aihub] MessageCache: dropped message after max retries cacheKey=${key}`);
        continue;
      }

      pending.push(entry);
    }

    return pending;
  }

  /**
   * 按 cacheKey 标记消息为已发送
   */
  markSent(cacheKey: string): void {
    if (this.cache.has(cacheKey)) {
      this.cache.delete(cacheKey);
      this.runtime?.log?.(`[53aihub] MessageCache: marked as sent cacheKey=${cacheKey}`);
    }
  }

  /**
   * 按 cacheKey 增加重试计数
   */
  incrementRetry(cacheKey: string): void {
    const entry = this.cache.get(cacheKey);
    if (entry) {
      entry.retryCount++;
    }
  }

  /**
   * 清理过期条目
   */
  private prune(): void {
    const now = Date.now();
    let pruned = 0;

    for (const [key, entry] of this.cache) {
      if (now - entry.createdAt > this.config.ttlMs) {
        this.cache.delete(key);
        pruned++;
      }
    }

    if (pruned > 0) {
      this.runtime?.log?.(`[53aihub] MessageCache: pruned ${pruned} expired entries`);
    }
  }

  size(): number {
    return this.cache.size;
  }

  clear(): void {
    this.cache.clear();
  }
}

// 按 accountId 隔离的缓存实例
const cacheInstances: Map<string, MessageCache> = new Map();

/**
 * 获取或创建按账号隔离的缓存实例
 */
export function getMessageCache(accountId: string, runtime?: RuntimeEnv): MessageCache {
  let cache = cacheInstances.get(accountId);
  if (!cache) {
    cache = new MessageCache(accountId, {}, runtime);
    cacheInstances.set(accountId, cache);
  }
  return cache;
}

/**
 * 清理指定账号的缓存
 */
export function clearAccountCache(accountId: string): void {
  const cache = cacheInstances.get(accountId);
  if (cache) {
    cache.clear();
    cacheInstances.delete(accountId);
  }
}

/**
 * 发送消息参数（与 sendReply 兼容）
 */
export interface CachedSendParams {
  wsClient: WebSocket;
  text: string;
  toChatId: string;
  replyToMsgId?: string;
  runtime: RuntimeEnv;
  finish: boolean;
  streamId: string;
  isError?: boolean;
  errorCode?: string;
  errorDetails?: string;
  isThinking?: boolean;
}

/**
 * 缓存感知的消息发送器
 * 返回 [success, cacheKey?] - 成功时 cacheKey 为 undefined，失败时返回缓存 key
 */
export async function sendWithCache(
  accountId: string,
  params: CachedSendParams,
  sendFn: (params: CachedSendParams) => Promise<void>
): Promise<{ success: boolean; cacheKey?: string }> {
  const { wsClient, replyToMsgId, streamId, text, toChatId, finish, isError, errorCode, errorDetails, isThinking, runtime } = params;
  const reqId = replyToMsgId || streamId;

  if (wsClient.readyState !== 1) {
    const cache = getMessageCache(accountId, runtime);
    const cacheKey = cache.add({
      reqId,
      toChatId,
      text,
      finish,
      isError,
      errorCode,
      errorDetails,
      isThinking,
    });
    runtime.error?.(`[53aihub] sendWithCache: WebSocket not ready, cached message cacheKey=${cacheKey}`);
    return { success: false, cacheKey };
  }

  try {
    await sendFn(params);
    return { success: true };
  } catch (err) {
    const cache = getMessageCache(accountId, runtime);
    const cacheKey = cache.add({
      reqId,
      toChatId,
      text,
      finish,
      isError,
      errorCode,
      errorDetails,
      isThinking,
    });
    runtime.error?.(`[53aihub] sendWithCache: send failed, cached message cacheKey=${cacheKey}, error=${String(err)}`);
    return { success: false, cacheKey };
  }
}

/**
 * 重放缓存的消息
 */
export async function replayCachedMessages(
  accountId: string,
  wsClient: WebSocket,
  runtime: RuntimeEnv,
  sendFn: (params: CachedSendParams) => Promise<void>
): Promise<number> {
  const cache = getMessageCache(accountId, runtime);
  const pending = cache.getPendingMessages();

  if (pending.length === 0) {
    return 0;
  }

  runtime.log?.(`[53aihub] replayCachedMessages: replaying ${pending.length} cached messages for account=${accountId}`);
  let successCount = 0;

  for (const entry of pending) {
    if (wsClient.readyState !== 1) {
      runtime.error?.(`[53aihub] replayCachedMessages: WebSocket not ready, stopping replay`);
      break;
    }

    try {
      await sendFn({
        wsClient,
        text: entry.text,
        toChatId: entry.toChatId,
        replyToMsgId: entry.reqId,
        runtime,
        finish: entry.finish,
        streamId: entry.reqId,
        isError: entry.isError,
        errorCode: entry.errorCode,
        errorDetails: entry.errorDetails,
        isThinking: entry.isThinking,
      });
      cache.markSent(entry.cacheKey);
      successCount++;

      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (err) {
      const newRetryCount = entry.retryCount + 1;
      cache.incrementRetry(entry.cacheKey);
      runtime.error?.(`[53aihub] replayCachedMessages: failed to send cacheKey=${entry.cacheKey}, retryCount=${newRetryCount}`);
    }
  }

  runtime.log?.(`[53aihub] replayCachedMessages: completed, ${successCount}/${pending.length} sent`);
  return successCount;
}
