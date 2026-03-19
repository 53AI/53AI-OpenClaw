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
import type { MonitorOptions, MessageState, 53AIHubIncomingMessage, 53AIHubWsMessage } from "./interface.js";
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

// 消息队列处理器 - 确保消息按顺序处理，避免竞态条件
class MessageQueue {
  private queue: Array<() => Promise<void>> = [];
  private processing = false;
  private runtime: RuntimeEnv;

  constructor(runtime: RuntimeEnv) {
    this.runtime = runtime;
  }

  enqueue(task: () => Promise<void>): void {
    this.queue.push(task);
    this.process();
  }

  private async process(): Promise<void> {
    if (this.processing || this.queue.length === 0) return;
    
    this.processing = true;
    try {
      const task = this.queue.shift();
      if (task) {
        await task();
      }
    } catch (err) {
      this.runtime.error?.(`[53aihub] MessageQueue error: ${String(err)}`);
    } finally {
      this.processing = false;
      // 继续处理队列中的下一条消息
      if (this.queue.length > 0) {
        this.process();
      }
    }
  }

  clear(): void {
    this.queue = [];
  }
}

function buildMessageContext(
  body: 53AIHubIncomingMessage,
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

  const accessResult = await checkAccessPolicy({
    userId: body.userId,
    account,
    wsClient,
    runtime,
  });

  if (!accessResult.allowed) {
    await sendReply({
      wsClient,
      text: accessResult.reason === "Pairing required" 
        ? "您尚未获得授权使用此机器人，请联系管理员进行审核。"
        : `⚠️ 访问被拒绝: ${accessResult.reason || "未知原因"}`,
      toChatId: chatId,
      replyToMsgId: body.msgId,
      runtime,
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
            state.accumulatedText += payload.text;
            runtime.log?.(`[53aihub] deliver: kind=${info.kind}, textLen=${payload.text?.length || 0}, accumulatedLen=${state.accumulatedText.length}, isError=${payload.isError}`);

            if (payload.isError) {
              const errorMsg = payload.text || "Unknown error";
              const errorCode = inferErrorCode(errorMsg);
              runtime.error?.(`[53aihub] deliver ERROR: ${errorMsg}`);
              await sendReply({
                wsClient,
                text: `⚠️ ${errorMsg}`,
                toChatId: chatId,
                replyToMsgId: body.msgId,
                runtime,
                finish: true,
                streamId: state.streamId,
                isError: true,
                errorCode,
                errorDetails: errorMsg,
              });
              return;
            }

            if (info.kind !== "final") {
              runtime.log?.(`[53aihub] deliver STREAMING: accumulatedText preview=${state.accumulatedText.substring(0, 50)}...`);
              await sendReply({
                wsClient,
                text: state.accumulatedText,
                toChatId: chatId,
                replyToMsgId: body.msgId,
                runtime,
                finish: false,
                streamId: state.streamId,
              });
            }
          },
          onError: async (err, info) => {
            runtime.error?.(`[53aihub] onError: kind=${info.kind}, error=${String(err)}`);
            const errorText = String(err);
            const errorCode = inferErrorCode(errorText);
            try {
              await sendReply({
                wsClient,
                text: `⚠️ 系统错误: ${errorText}`,
                toChatId: chatId,
                replyToMsgId: body.msgId,
                runtime,
                finish: true,
                streamId: state.streamId,
                isError: true,
                errorCode,
                errorDetails: `kind=${info.kind}, error=${errorText}`,
              });
            } catch (sendErr) {
              runtime.error?.(`[53aihub] Failed to send error notification: ${String(sendErr)}`);
            }
          },
        },
      }),
      MESSAGE_PROCESS_TIMEOUT_MS,
      `Message processing timed out (msgId=${body.msgId})`
    );

    runtime.log?.(`[53aihub] processMessage: dispatchReply completed, accumulatedTextLen=${state.accumulatedText.length}`);

    if (state.accumulatedText) {
      runtime.log?.(`[53aihub] processMessage: Sending final reply with accumulatedText`);
      await sendReply({
        wsClient,
        text: state.accumulatedText,
        toChatId: chatId,
        replyToMsgId: body.msgId,
        runtime,
        finish: true,
        streamId: state.streamId,
      });
    } else {
      runtime.log?.(`[53aihub] processMessage: No accumulatedText, sending empty final reply`);
      await sendReply({
        wsClient,
        text: "",
        toChatId: chatId,
        replyToMsgId: body.msgId,
        runtime,
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
        await sendReply({
          wsClient,
          text: `⚠️ 处理请求时发生异常: ${errorText}`,
          toChatId: chatId,
          replyToMsgId: body.msgId,
          runtime,
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

    const cleanup = async () => {
      if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
      }
      if (messageQueue) {
        messageQueue.clear();
        messageQueue = null;
      }
      await cleanupAccount(account.accountId);
    };

    const connect = () => {
      if (isAborted) return;
      
      // 安全: 不在 URL 中传递敏感信息，仅通过 headers 传递认证
      const wsUrl = account.websocketUrl;
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

      wsClient.on("open", () => {
        runtime.log?.(`[${account.accountId}] WebSocket connected successfully`);
        reconnectAttempts = 0;
        pingInterval = setInterval(() => {
          if (wsClient?.readyState === WebSocket.OPEN) {
            wsClient.ping();
          }
        }, WS_HEARTBEAT_INTERVAL_MS);
      });

      wsClient.on("message", (data: Buffer | string) => {
        const rawPayload = data.toString();
        runtime.log?.(`[${account.accountId}] Received WS message: ${rawPayload.substring(0, 200)}...`);
        
        // 使用消息队列确保顺序处理，避免竞态条件
        messageQueue?.enqueue(async () => {
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
          runtime.error?.(`[${account.accountId}] Max reconnect attempts (${WS_MAX_RECONNECT_ATTEMPTS}) reached`);
          await cleanup();
          reject(new Error(`Max reconnect attempts (${WS_MAX_RECONNECT_ATTEMPTS}) reached`));
        }
      });
    };

    if (abortSignal) {
      abortSignal.addEventListener("abort", async () => {
        isAborted = true;
        await cleanup();
        resolve();
      });
    }

    warmupReqIdStore(account.accountId, (msg) => runtime.log?.(msg))
      .then(() => connect())
      .catch(async (err) => {
        runtime.error?.(`[${account.accountId}] Failed to warmup ReqId store: ${String(err)}`);
        await cleanup();
        reject(err);
      });
  });
}