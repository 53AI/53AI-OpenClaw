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
} from "./const.js";
import type { MonitorOptions, MessageState, AgentHubIncomingMessage, AgentHubWsMessage } from "./interface.js";
import { ErrorCode, inferErrorCode } from "./interface.js";
import { parseIncomingMessage, parseMessageContent } from "./message-parser.js";
import { sendReply } from "./message-sender.js";
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

function buildMessageContext(
  body: AgentHubIncomingMessage,
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

    const connect = () => {
      if (isAborted) return;
      
      const wsUrlObj = new URL(account.websocketUrl);
      const botId = account.botId || account.config.botId;
      const secret = account.secret || account.token || account.config.secret || account.config.token;
      
      if (botId) wsUrlObj.searchParams.append("botId", botId);
      if (secret) wsUrlObj.searchParams.append("secret", secret);
      
      const wsUrl = wsUrlObj.toString();

      const authBase64 = Buffer.from(`${botId}:${secret}`).toString('base64');

      const wsOptions = {
        headers: {
          "Authorization": `Bearer ${secret}`,
          "Proxy-Authorization": `Basic ${authBase64}`,
          "X-Bot-Id": botId || "",
          "X-Api-Key": secret || "",
        } as Record<string, string>
      };

      runtime.log?.(`[${account.accountId}] Connecting to ${wsUrl} ...`);
      wsClient = new WebSocket(wsUrl, wsOptions);
      setWebSocket(account.accountId, wsClient);

      wsClient.on("open", () => {
        runtime.log?.(`[${account.accountId}] WebSocket connected successfully`);
        reconnectAttempts = 0;
        pingInterval = setInterval(() => {
          if (wsClient?.readyState === WebSocket.OPEN) {
            wsClient.ping();
          }
        }, WS_HEARTBEAT_INTERVAL_MS);
      });

      wsClient.on("message", async (data: Buffer | string) => {
        try {
          const rawPayload = data.toString();
          runtime.log?.(`[${account.accountId}] Received WS message: ${rawPayload.substring(0, 200)}...`);
          await processMessage({
            rawPayload,
            account,
            config,
            runtime,
            wsClient: wsClient!,
          });
        } catch (err) {
          runtime.error?.(`[${account.accountId}] Message processing error: ${String(err)}`);
        }
      });

      wsClient.on("error", (err) => {
        runtime.error?.(`[${account.accountId}] WebSocket Error: ${String(err)}`);
      });

      wsClient.on("close", (code, reason) => {
        runtime.log?.(`[${account.accountId}] WebSocket closed. Code: ${code}, Reason: ${reason}`);
        if (pingInterval) clearInterval(pingInterval);
        
        if (!isAborted && reconnectAttempts < WS_MAX_RECONNECT_ATTEMPTS) {
          reconnectAttempts++;
          const backoff = Math.min(WS_RECONNECT_BASE_DELAY_MS * Math.pow(2, reconnectAttempts), 30000);
          runtime.log?.(`[${account.accountId}] Reconnecting in ${backoff}ms... (attempt ${reconnectAttempts}/${WS_MAX_RECONNECT_ATTEMPTS})`);
          setTimeout(connect, backoff);
        } else if (!isAborted) {
          reject(new Error("Max reconnect attempts reached"));
        }
      });
    };

    if (abortSignal) {
      abortSignal.addEventListener("abort", async () => {
        isAborted = true;
        if (pingInterval) clearInterval(pingInterval);
        await cleanupAccount(account.accountId);
        resolve();
      });
    }

    warmupReqIdStore(account.accountId, (msg) => runtime.log?.(msg))
      .then(() => connect())
      .catch(reject);
  });
}