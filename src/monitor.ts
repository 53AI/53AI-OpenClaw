import type { OpenClawConfig, RuntimeEnv } from "openclaw/plugin-sdk";
import WebSocket from "ws";
import { getRuntime } from "./runtime.js";
import type { ResolvedAccount } from "./utils.js";
import {
  CHANNEL_ID,
  MESSAGE_PROCESS_TIMEOUT_MS,
  WS_HEARTBEAT_INTERVAL_MS,
  WS_MAX_RECONNECT_ATTEMPTS,
  WS_RECONNECT_BASE_DELAY_MS,
  MEDIA_IMAGE_PLACEHOLDER,
  MEDIA_DOCUMENT_PLACEHOLDER,
  THINKING_MESSAGE,
} from "./const.js";
import type { MonitorOptions, MessageState, Hub53AIIncomingMessage, Hub53AIWsMessage } from "./interface.js";
import { ErrorCode, inferErrorCode } from "./interface.js";
import { parseIncomingMessage, parseMessageContent } from "./message-parser.js";
import { sendReply, sendThinkingMessage } from "./message-sender.js";
import { checkAccessPolicy } from "./access-policy.js";
import {
  setWebSocket,
  setMessageState,
  deleteMessageState,
  startMessageStateCleanup,
  cleanupAccount,
  setLastMsgIdForChat,
  warmupReqIdStore,
} from "./state-manager.js";
import { withTimeout } from "./timeout.js";
import { downloadAndSaveImages, downloadAndSaveFiles } from "./media-handler.js";
import { sendWithCache, replayCachedMessages } from "./message-cache.js";
import type { CachedSendParams } from "./message-cache.js";

// 按 chatId 分队列的消息处理器 - 同一会话串行，不同会话并行
class MessageQueue {
  private queues: Map<string, Array<() => Promise<void>>> = new Map();
  private processing: Map<string, boolean> = new Map();
  private runtime: RuntimeEnv;

  constructor(runtime: RuntimeEnv) {
    this.runtime = runtime;
  }

  enqueue(chatId: string, task: () => Promise<void>): void {
    if (!this.queues.has(chatId)) {
      this.queues.set(chatId, []);
      this.processing.set(chatId, false);
    }
    this.queues.get(chatId)!.push(task);
    this.process(chatId);
  }

  private async process(chatId: string): Promise<void> {
    const queue = this.queues.get(chatId);
    const isProcessing = this.processing.get(chatId);
    
    if (!queue || isProcessing || queue.length === 0) return;

    this.processing.set(chatId, true);
    try {
      const task = queue.shift();
      if (task) {
        await task();
      }
    } catch (err) {
      this.runtime.error?.(`[53aihub] MessageQueue error (chatId=${chatId}): ${String(err)}`);
    } finally {
      this.processing.set(chatId, false);
      if (queue.length > 0) {
        this.process(chatId);
      } else {
        this.queues.delete(chatId);
        this.processing.delete(chatId);
      }
    }
  }

  clear(): void {
    this.queues.clear();
    this.processing.clear();
  }
}

function buildMessageContext(
  body: Hub53AIIncomingMessage,
  account: ResolvedAccount,
  config: OpenClawConfig,
  mediaList: Array<{ path: string; contentType?: string }>,
) {
  const core = getRuntime();
  const chatId = body.chatId || body.userId;

  const route = core.channel.routing.resolveAgentRoute({
    cfg: config,
    channel: CHANNEL_ID,
    accountId: account.accountId,
    peer: {
      kind: "direct",
      id: chatId,
    },
  });

  const hasImages = mediaList.some((m) => m.contentType?.startsWith("image/"));
  const messageBody = body.text || (mediaList.length > 0 ? (hasImages ? MEDIA_IMAGE_PLACEHOLDER : MEDIA_DOCUMENT_PLACEHOLDER) : "");

  const mediaPaths = mediaList.length > 0 ? mediaList.map((m) => m.path) : undefined;
  const mediaTypes = mediaList.length > 0
    ? (mediaList.map((m) => m.contentType).filter(Boolean) as string[])
    : undefined;

  return core.channel.reply.finalizeInboundContext({
    Body: messageBody,
    RawBody: messageBody,
    CommandBody: messageBody,
    MessageSid: body.msgId,
    From: `${CHANNEL_ID}:${body.userId}`,
    To: `${CHANNEL_ID}:${chatId}`,
    SenderId: body.userId,
    SessionKey: route.sessionKey,
    AccountId: account.accountId,
    ChatType: "direct",
    ConversationLabel: `user:${body.userId}`,
    Timestamp: Date.now(),
    Provider: CHANNEL_ID,
    Surface: CHANNEL_ID,
    OriginatingChannel: CHANNEL_ID,
    OriginatingTo: `${CHANNEL_ID}:${chatId}`,
    CommandAuthorized: true,
    ReplyToBody: body.quoteContent,
    MediaPath: mediaList[0]?.path,
    MediaType: mediaList[0]?.contentType,
    MediaPaths: mediaPaths,
    MediaTypes: mediaTypes,
    MediaUrls: mediaPaths,
  });
}

async function processMessage(params: {
  rawPayload: string;
  account: ResolvedAccount;
  config: OpenClawConfig;
  runtime: RuntimeEnv;
  wsClient: WebSocket;
}) {
  const { rawPayload, account, config, runtime, wsClient } = params;

  runtime.log?.(`[53aihub] processMessage: rawPayload length=${rawPayload.length}`);

  const body = parseIncomingMessage(rawPayload);
  if (!body) {
    runtime.log?.(`[53aihub] processMessage: parseIncomingMessage returned null`);
    return;
  }

  const parsed = parseMessageContent(body);
  const hasMedia = parsed.imageUrls.length > 0 || parsed.fileUrls.length > 0;

  if (!parsed.textParts.join("\n").trim() && !hasMedia) {
    runtime.log?.(`[53aihub] processMessage: empty message, body=${JSON.stringify(body)}`);
    return;
  }

  const chatId = body.chatId || body.userId;
  runtime.log?.(`[53aihub] processMessage: chatId=${chatId}, msgId=${body.msgId}, text=${parsed.textParts.join(" ").substring(0, 50)}... images=${parsed.imageUrls.length} files=${parsed.fileUrls.length}`);

  const core = getRuntime();
  const streamId = `stream-${Date.now()}`;

  // 缓存感知的消息发送器 - 必须在所有 sendReply 调用前定义
  const cachedSend = async (sendParams: Omit<CachedSendParams, 'wsClient' | 'runtime'>) => {
    await sendWithCache(account.accountId, {
      ...sendParams,
      wsClient,
      runtime,
    }, sendReply);
  };

  const accessResult = await checkAccessPolicy({
    userId: body.userId,
    account,
    wsClient,
    runtime,
  });

  if (!accessResult.allowed) {
    await cachedSend({
      text: accessResult.reason === "Pairing required"
        ? "您尚未获得授权使用此机器人，请联系管理员进行审核。"
        : `⚠️ 访问被拒绝: ${accessResult.reason || "未知原因"}`,
      toChatId: chatId,
      replyToMsgId: body.msgId,
      finish: true,
      streamId,
      isError: true,
      errorCode: ErrorCode.ACCESS_DENIED,
      errorDetails: accessResult.reason,
    });
    return;
  }

  setLastMsgIdForChat(chatId, body.msgId, account.accountId);

  const state: MessageState = { accumulatedText: "", lastSentText: "", streamId };
  setMessageState(body.msgId, state);

  if (account.sendThinkingMessage) {
    runtime.log?.(`[53aihub] processMessage: sendThinkingMessage=${account.sendThinkingMessage}, about to send thinking message`);
    try {
      await sendThinkingMessage(wsClient, THINKING_MESSAGE, body.msgId, state.streamId, runtime);
      runtime.log?.(`[53aihub] processMessage: thinking message sent successfully`);
    } catch (err) {
      runtime.error?.(`[53aihub] Failed to send thinking message: ${String(err)}`);
    }
  } else {
    runtime.log?.(`[53aihub] processMessage: sendThinkingMessage=${account.sendThinkingMessage}, SKIPPING thinking message`);
  }

  const cleanupState = () => {
    deleteMessageState(body.msgId);
  };

  const [imageMediaList, fileMediaList] = await Promise.all([
    downloadAndSaveImages({
      imageUrls: parsed.imageUrls,
      account,
      config,
      runtime,
      wsClient,
    }),
    downloadAndSaveFiles({
      fileUrls: parsed.fileUrls,
      account,
      config,
      runtime,
      wsClient,
    }),
  ]);
  const mediaList = [...imageMediaList, ...fileMediaList];

  const ctxPayload = buildMessageContext(body, account, config, mediaList);

  let cleanedUp = false;
  const safeCleanup = () => {
    if (!cleanedUp) {
      cleanedUp = true;
      cleanupState();
    }
  };

  runtime.log?.(`[53aihub] processMessage: Starting dispatchReplyWithBufferedBlockDispatcher for msgId=${body.msgId}`);

  try {
    await withTimeout(
      core.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
        ctx: ctxPayload,
        cfg: config,
        dispatcherOptions: {
          deliver: async (payload, info) => {
            runtime.log?.(`[53aihub] deliver: kind=${info.kind}, textLen=${payload.text?.length || 0}, accumulatedLen=${state.accumulatedText.length}, isError=${payload.isError}`);

            if (payload.isError) {
              const errorMsg = payload.text || "Unknown error";
              const errorCode = inferErrorCode(errorMsg);
              runtime.error?.(`[53aihub] deliver ERROR: ${errorMsg}`);
              await cachedSend({
                text: `⚠️ ${errorMsg}`,
                toChatId: chatId,
                replyToMsgId: body.msgId,
                finish: true,
                streamId: state.streamId,
                isError: true,
                errorCode,
                errorDetails: errorMsg,
              });
              return;
            }

            const isCompaction = payload.text?.startsWith("🧹 Compacting context") || 
                                 state.accumulatedText.startsWith("🧹 Compacting context");
            
            if (isCompaction && info.kind !== "final") {
              runtime.log?.(`[53aihub] deliver COMPACTION: text preview=${payload.text?.substring(0, 50)}...`);
              await cachedSend({
                text: payload.text || "",
                toChatId: chatId,
                replyToMsgId: body.msgId,
                finish: false,
                streamId: state.streamId,
                isThinking: true,
              });
              return;
            }

            state.accumulatedText += payload.text;

            if (info.kind !== "final") {
              runtime.log?.(`[53aihub] deliver STREAMING: accumulatedText preview=${state.accumulatedText.substring(0, 50)}...`);
              await cachedSend({
                text: state.accumulatedText,
                toChatId: chatId,
                replyToMsgId: body.msgId,
                finish: false,
                streamId: state.streamId,
              });
            }
          },
          onError: async (err, info) => {
            runtime.error?.(`[53aihub] onError: kind=${info.kind}, error=${String(err)}`);
            const errorText = String(err);
            const errorCode = inferErrorCode(errorText);
            await cachedSend({
              text: `⚠️ 系统错误: ${errorText}`,
              toChatId: chatId,
              replyToMsgId: body.msgId,
              finish: true,
              streamId: state.streamId,
              isError: true,
              errorCode,
              errorDetails: `kind=${info.kind}, error=${errorText}`,
            });
          },
        },
      }),
      MESSAGE_PROCESS_TIMEOUT_MS,
      `Message processing timed out (msgId=${body.msgId})`
    );

    runtime.log?.(`[53aihub] processMessage: dispatchReply completed, accumulatedTextLen=${state.accumulatedText.length}`);

    if (state.accumulatedText) {
      runtime.log?.(`[53aihub] processMessage: Sending final reply with accumulatedText`);
      await cachedSend({
        text: state.accumulatedText,
        toChatId: chatId,
        replyToMsgId: body.msgId,
        finish: true,
        streamId: state.streamId,
      });
    } else {
      runtime.log?.(`[53aihub] processMessage: No accumulatedText, sending empty final reply`);
      await cachedSend({
        text: "",
        toChatId: chatId,
        replyToMsgId: body.msgId,
        finish: true,
        streamId: state.streamId,
      });
    }

    safeCleanup();
  } catch (err) {
    runtime.error?.(`[53aihub] processMessage FAILED: ${String(err)}`);
    const errorText = String(err);
    const errorCode = inferErrorCode(errorText);

    if (!cleanedUp) {
      try {
        await cachedSend({
          text: `⚠️ 处理请求时发生异常: ${errorText}`,
          toChatId: chatId,
          replyToMsgId: body.msgId,
          finish: true,
          streamId: state.streamId,
          isError: true,
          errorCode,
          errorDetails: errorText,
        });
      } catch (sendErr) {
        runtime.error?.(`[53aihub] Failed to send final error notification: ${String(sendErr)}`);
      }
    }

    safeCleanup();
  }
}

export async function monitorProvider(options: MonitorOptions): Promise<void> {
  const { account, config, runtime, abortSignal } = options;

  runtime.log?.(`[${account.accountId}] Initializing WS connection to 53AIHub...`);
  startMessageStateCleanup();

  return new Promise((resolve, reject) => {
    let wsClient: WebSocket | null = null;
    let reconnectAttempts = 0;
    let pingInterval: NodeJS.Timeout | null = null;
    let isAborted = false;
    let messageQueue: MessageQueue | null = null;

    const connect = () => {
      if (isAborted) return;

      // 安全: 不在 URL 中传递敏感信息，仅通过 headers 传递认证
      const wsUrl = account.WSUrl;
      const botId = account.botId || account.config.botId;
      const secret = account.secret || account.token || account.config.secret || account.config.token;

      // 日志输出时隐藏敏感信息
      const safeUrl = new URL(wsUrl);
      const logUrl = `${safeUrl.origin}${safeUrl.pathname}`;

      const authBase64 = Buffer.from(`${botId}:${secret}`).toString('base64');

      const wsOptions = {
        headers: {
          "Authorization": `Bearer ${secret}`,
          "Proxy-Authorization": `Basic ${authBase64}`,
          "X-Bot-Id": botId || "",
          "X-Api-Key": secret || "",
        } as Record<string, string>
      };

      runtime.log?.(`[${account.accountId}] Connecting to ${logUrl} ...`);
      wsClient = new WebSocket(wsUrl, wsOptions);
      setWebSocket(account.accountId, wsClient);

      // 初始化消息队列
      messageQueue = new MessageQueue(runtime);

      wsClient.on("open", async () => {
        runtime.log?.(`[${account.accountId}] WebSocket connected successfully`);
        reconnectAttempts = 0;

        try {
          const replayedCount = await replayCachedMessages(account.accountId, wsClient!, runtime, sendReply);
          if (replayedCount > 0) {
            runtime.log?.(`[${account.accountId}] Replayed ${replayedCount} cached messages`);
          }
        } catch (err) {
          runtime.error?.(`[${account.accountId}] Failed to replay cached messages: ${String(err)}`);
        }

        pingInterval = setInterval(() => {
          if (wsClient?.readyState === WebSocket.OPEN) {
            wsClient.ping();
          }
        }, WS_HEARTBEAT_INTERVAL_MS);
      });

      wsClient.on("message", (data: Buffer | string) => {
        const rawPayload = data.toString();
        runtime.log?.(`[${account.accountId}] Received WS message: ${rawPayload.substring(0, 200)}...`);

        let chatId = "unknown";
        try {
          const msg = JSON.parse(rawPayload) as { action?: string; data?: { conversation_id?: string; user?: string; chatId?: string; userId?: string } };
          if (msg.action === "chat") {
            chatId = msg.data?.conversation_id || msg.data?.user || "unknown";
          } else {
            chatId = msg.data?.chatId || msg.data?.userId || "unknown";
          }
        } catch {
          chatId = "unknown";
        }

        messageQueue?.enqueue(chatId, async () => {
          await processMessage({
            rawPayload,
            account,
            config,
            runtime,
            wsClient: wsClient!,
          });
        });
      });

      wsClient.on("error", (err) => {
        runtime.error?.(`[${account.accountId}] WebSocket Error: ${String(err)}`);
      });

      wsClient.on("close", async (code, reason) => {
        runtime.log?.(`[${account.accountId}] WebSocket closed. Code: ${code}, Reason: ${reason}`);

        // 清理资源
        if (pingInterval) {
          clearInterval(pingInterval);
          pingInterval = null;
        }
        if (messageQueue) {
          messageQueue.clear();
          messageQueue = null;
        }

        if (!isAborted && reconnectAttempts < WS_MAX_RECONNECT_ATTEMPTS) {
          reconnectAttempts++;
          const backoff = Math.min(WS_RECONNECT_BASE_DELAY_MS * Math.pow(2, reconnectAttempts), 30000);
          runtime.log?.(`[${account.accountId}] Reconnecting in ${backoff}ms... (attempt ${reconnectAttempts}/${WS_MAX_RECONNECT_ATTEMPTS})`);
          setTimeout(connect, backoff);
        } else if (!isAborted) {
          runtime.error?.(`[${account.accountId}] Max reconnect attempts (${WS_MAX_RECONNECT_ATTEMPTS}) reached, preserving cache for external recovery`);
          await cleanupAccount(account.accountId, false);
          reject(new Error(`Max reconnect attempts (${WS_MAX_RECONNECT_ATTEMPTS}) reached`));
        }
      });
    };

    if (abortSignal) {
      abortSignal.addEventListener("abort", async () => {
        isAborted = true;
        if (pingInterval) {
          clearInterval(pingInterval);
          pingInterval = null;
        }
        if (messageQueue) {
          messageQueue.clear();
          messageQueue = null;
        }
        await cleanupAccount(account.accountId, true);
        resolve();
      });
    }

    warmupReqIdStore(account.accountId, (msg) => runtime.log?.(msg))
      .then(() => connect())
      .catch(async (err) => {
        runtime.error?.(`[${account.accountId}] Failed to warmup ReqId store: ${String(err)}, preserving cache for retry`);
        await cleanupAccount(account.accountId, false);
        reject(err);
      });
  });
}