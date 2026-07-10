import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { basename, dirname, extname, join } from "node:path";

import WebSocket from "ws";

import { ensureHubSkillInstalled, type EnsureHubSkillRequest, type EnsureHubSkillResult } from "./skill-installer";
import type {
  GatewayClient,
  GatewayEvent,
  GatewayMessageAttachment,
  GatewayRuntimeInfo,
  GatewaySessionPage,
  GatewaySession
} from "./gateway-client";
import {
  collectConversationManifestLocalOutputFiles,
  collectManifestLocalOutputFiles,
  collectReferencedLocalOutputFiles,
  collectCreatedLocalOutputFiles,
  collectRecentReferencedLocalOutputFiles,
  extractReferencedLocalOutputPaths,
  resolveLocalOutputManifestPath,
  resolveLocalOutputWorkspaceDirs,
  snapshotLocalOutputFiles,
  type LocalOutputManifestFile,
  type LocalOutputFileSnapshot
} from "./local-output-files";
import type { SessionMessage, SessionStatus, TimelineEvent } from "./models";
import {
  applyHubUserMessagePatch,
  isHubUserMessagePatch,
  findHubUserMessagePatch,
  mergeHubLocalAssistantMessages,
  mergeHubUserMessageMetadata
} from "./hub-message-utils";

export type Hub53AIConfig = {
  enabled: boolean;
  botId: string;
  secret: string;
  wsUrl: string;
  accessPolicy: "open" | "allowlist";
  allowFrom: string[];
  sendThinkingMessage: boolean;
  reconnectBaseMs: number;
  maxReconnectAttempts: number;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  detectCreatedFiles?: boolean;
  fileWorkspaceDirs?: string[];
  createdFilesMaxFileBytes?: number;
  createdFilesMaxCount?: number;
  createdFilesExclude?: string[];
  artifactUploadTimeoutMs?: number;
  diagnosticLogs?: boolean;
  ledgerDebug?: boolean;
  duplicateTrace?: boolean;
  debug?: {
    all?: boolean;
    ledger?: boolean;
    duplicates?: boolean;
  };
};

export type Hub53AIStatusSnapshot = {
  enabled: boolean;
  configured: boolean;
  connectionStatus: "disabled" | "connecting" | "connected" | "disconnected" | "error";
  botId?: string;
  wsUrl?: string;
  lastHeartbeatAt?: string;
  lastConnectedAt?: string;
  lastError?: string;
  receivedMessageCount: number;
  sentMessageCount: number;
  pendingOutboundCount: number;
};

export type Hub53AIIncomingMessage = {
  type: string;
  msgId: string;
  reqId: string;
  chatId: string;
  userId: string;
  userName?: string;
  text: string;
  imageUrls?: string[];
  fileUrls?: string[];
  files?: Hub53AIInputFile[];
  skill?: Hub53AISkillSelection;
  quoteContent?: string;
  conversationTitle?: string;
  clientMessageId?: string;
};

export type Hub53AIOutgoingChunk = {
  req_id: string;
  action: "chat";
  status: "streaming" | "thinking" | "done" | "error";
  data: {
    id: string;
    object: "chat.completion.chunk";
    created: number;
    model: "openclaw-agent";
    status?: "streaming" | "thinking" | "done" | "error";
    mode?: string;
    replace?: boolean;
    event_kind?: string;
    payload?: Record<string, unknown>;
    session_id?: string;
    conversation_id?: string;
    choices: Array<{
      index: number;
      delta: {
        content?: string;
        reasoning_content?: string;
        role: "assistant";
      };
      finish_reason: "stop" | "error" | null;
    }>;
    error?: {
      code: string;
      message: string;
      details?: string;
    };
  };
};

export type Hub53AIOutputFile = {
  id: string;
  artifact_id?: string;
  upload_file_id?: string;
  file_name: string;
  path?: string;
  url?: string;
  preview_url?: string;
  download_url?: string;
  signed_download_url?: string;
  preview_key?: string;
  mime_type?: string;
  size?: number;
  sha256?: string;
  base64?: string;
  content?: string;
  message_id?: string;
  source_kind?: string;
};

export type Hub53AIInputFile = {
  id?: string;
  file_id?: string;
  name?: string;
  file_name?: string;
  filename?: string;
  url?: string;
  preview_url?: string;
  download_url?: string;
  signed_download_url?: string;
  preview_key?: string;
  mime_type?: string;
  size?: number;
  local_path?: string;
};

export type Hub53AISkillSelection = {
  skill_id?: string;
  skill_name?: string;
  display_name?: string;
  ensure?: boolean;
};

type OpenClawTimelineV2SegmentType =
  | "answer"
  | "thinking"
  | "tool_call"
  | "tool_result"
  | "run"
  | "output_files";

type OpenClawTimelineV2Operation = "append" | "replace" | "close";
type OpenClawTimelineV2Visibility = "hidden" | "stream" | "final";

type OpenClawTimelineV2Meta = {
  protocol_version: "openclaw.timeline.v2";
  turn_id: string;
  segment_id: string;
  segment_type: OpenClawTimelineV2SegmentType;
  segment_index: number;
  delta_index: number;
  operation: OpenClawTimelineV2Operation;
  visibility: OpenClawTimelineV2Visibility;
  final: boolean;
};

type OpenClawLedgerPartType = "answer" | "thinking" | "tool" | "output_file" | "status";
type OpenClawLedgerEventType =
  | "turn.started"
  | "part.delta"
  | "part.replace"
  | "part.done"
  | "turn.completed"
  | "turn.interrupted"
  | "turn.failed";
type OpenClawLedgerOperation = "append" | "replace" | "close" | "noop";
type OpenClawLedgerTerminalStatus = "running" | "completed" | "interrupted" | "failed" | "cancelled";

type OpenClawLedgerEvent = {
  protocol_version: "openclaw.ledger.v1";
  seq: number;
  session_id: string;
  conversation_id: string;
  turn_id: string;
  run_id?: string;
  active_request_id: string;
  part_id: string;
  part_type: OpenClawLedgerPartType;
  event_type: OpenClawLedgerEventType;
  operation: OpenClawLedgerOperation;
  visibility: OpenClawTimelineV2Visibility;
  text?: string;
  payload?: Record<string, unknown>;
  terminal_status?: OpenClawLedgerTerminalStatus;
  created_at: string;
  raw_event_ref?: string;
};

type OpenClawLedgerTurnSnapshot = {
  turn_id: string;
  run_id?: string;
  active_request_id: string;
  status: OpenClawLedgerTerminalStatus;
  terminal_seq?: number;
  last_seq: number;
  part_ids: string[];
};

type OpenClawLedgerTurnStats = {
  last_created_at_ms: number;
  has_visible_part: boolean;
};

type TrackedActiveOpenClawTurns = {
  turnIds: Set<string>;
  runIds: Set<string>;
  requestIdByTurnId: Map<string, string>;
  requestIdByRunId: Map<string, string>;
};

type OpenClawSessionSnapshot = {
  session_id: string;
  conversation_id: string;
  last_seq: number;
  active_turns: OpenClawLedgerTurnSnapshot[];
  recent_events: OpenClawLedgerEvent[];
  ledger_events?: OpenClawLedgerEvent[];
  ledgerEvents?: OpenClawLedgerEvent[];
};

type OpenClawRunFailureClassification = {
  code: string;
  reason: string;
  userMessage: string;
  rawMessage?: string;
  provider?: string;
  model?: string;
  runtimeMs?: number;
  authRelated: boolean;
  confidence: "high" | "medium" | "low";
};

type OpenClawTypedFinalMatchStrategy =
  | "run_id"
  | "response_id"
  | "request_window"
  | "latest_after_user";

type OpenClawTypedFinalTranscript = {
  text: string;
  segmentCount: number;
  matchStrategy: OpenClawTypedFinalMatchStrategy;
  messageIds: string[];
  messageSeqs: number[];
};

type OpenClawTypedLiveTranscript = OpenClawTypedFinalTranscript;

type OpenClawTypedToolCall = {
  id: string;
  name: string;
  args: Record<string, unknown>;
  command?: string;
  meta?: string;
  sourceEventId?: string;
  sourceSeq?: number;
};

type ManifestOutputBackfillGroup = {
  turnId: string;
  activeRequestId: string;
  files: LocalOutputManifestFile[];
};

type VerifiedHistoryManifestScope = {
  turnId: string;
  activeRequestId: string;
};

export type Hub53AIMediaAttachment = Hub53AIOutputFile & {
  kind: "image" | "audio" | "video" | "text" | "file";
};

export type Hub53AIOutgoingProcessStep = {
  req_id: string;
  action: "chat";
  status: "streaming";
  data: {
    id: string;
    object: "process.step";
    created: number;
    model: "openclaw-agent";
    status: "streaming";
    session_id?: string;
    conversation_id?: string;
    process_step: {
      step_code: "output_files";
      name: string;
      status: "completed";
      message: string;
      data: {
        files: Hub53AIOutputFile[];
        contract_version: "v1";
        openclaw_timeline?: OpenClawTimelineV2Meta;
        media_attachments: Hub53AIMediaAttachment[];
        media_contract_version: "v1";
      };
      timestamp: number;
    };
  };
};

export type Hub53AIOutgoingRPCFrame = {
  req_id: string;
  action: string;
  status: "done" | "error";
  data: unknown;
};

export type Hub53AIQueuedFrame = Hub53AIOutgoingChunk | Hub53AIOutgoingProcessStep;
export type Hub53AIOutgoingFrame = Hub53AIQueuedFrame | Hub53AIOutgoingRPCFrame;

type StoredHubState = {
  mappings: Record<string, string>;
  outbox: Hub53AIQueuedFrame[];
  canonicalEventsBySession?: Record<string, TimelineEvent[]>;
  syntheticEventsBySession?: Record<string, TimelineEvent[]>;
};

type HubBridgeCallbacks = {
  onSessionUpsert(session: GatewaySession): Promise<void>;
  onUserMessage(message: SessionMessage): Promise<void>;
  onSessionStatus(sessionId: string, status: SessionStatus): Promise<void>;
  onBridgeThinkingEvent?(event: TimelineEvent): Promise<void>;
  listSessionMessages?(sessionId: string): SessionMessage[] | Promise<SessionMessage[]>;
  listSessionEvents?(sessionId: string): TimelineEvent[] | Promise<TimelineEvent[]>;
  listKnownSessions?(): GatewaySession[] | Promise<GatewaySession[]>;
  onEnsureSessionStream(sessionId: string): Promise<void>;
  getLastEventSeq(sessionId: string): number;
  onStatusChange(): void;
};

type HubBridgeInput = {
  stateDir: string;
  configPath?: string;
  config: Hub53AIConfig;
  gateway: GatewayClient;
  rpcContext?: {
    getStatusSnapshot?: () => unknown | Promise<unknown>;
    getConfigSnapshot?: () => unknown | Promise<unknown>;
  };
  callbacks: HubBridgeCallbacks;
  logger?: {
    info?(message: string): void;
    warn?(message: string): void;
    error?(message: string): void;
  };
};

const HEARTBEAT_INTERVAL_MS = 30_000;
const HEARTBEAT_TIMEOUT_MS = 90_000;
const MAX_OUTBOX_FRAMES = 200;
const MAX_SYNTHETIC_EVENTS_PER_SESSION = 200;
const MAX_CANONICAL_EVENTS_PER_SESSION = 500;
const MAX_CANONICAL_EVENTS_PER_MESSAGE_PAGE = 160;
const MAX_CANONICAL_EVENTS_PER_SNAPSHOT = 240;
const MAX_SESSION_MESSAGE_TURN_BOUNDARY_OVERSCAN = 8;
const CANONICAL_MESSAGE_PAGE_SEQ_WINDOW_BEFORE = 80;
const CANONICAL_MESSAGE_PAGE_SEQ_WINDOW_AFTER = 160;
const PERSIST_STATE_DEBOUNCE_MS = 300;
const OPENCLAW_ORPHAN_RUNNING_TURN_TERMINAL_MS = 30_000;
const RUN_WAIT_TIMEOUT_MS = 30 * 60 * 1000;
const HUB_SESSION_VALIDATION_PAGE_LIMIT = 50;
const HUB_SESSION_VALIDATION_MAX_PAGES = 100;
const HUB_SESSION_TITLE_PREFIX = "53AI Hub-";
const CONTROL_CENTER_SESSION_TITLE = "Claw Control Center";
const HUB_TITLE_SUMMARY_LENGTH = 40;
const OPENCLAW_RUNTIME_CONTEXT_START = "<53aihub-openclaw-runtime-context>";
const OPENCLAW_RUNTIME_CONTEXT_END = "</53aihub-openclaw-runtime-context>";
const HUB_RPC_ACTIONS = new Set([
  "sessions.list",
  "sessions.current",
  "sessions.messages",
  "sessions.events",
  "sessions.snapshot",
  "sessions.control",
  "runtime.get",
  "runtime.skills.ensure",
  "cron.tasks"
]);

type Hub53AIRPCRequest = {
  reqId: string;
  action: string;
  data: unknown;
};

type RPCPagination = {
  limit: number;
  offset: number;
};

type RuntimeSkillDisplayItem = string | Record<string, unknown>;

class HubRPCError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly details?: string
  ) {
    super(message);
    this.name = "HubRPCError";
  }
}

export function createHub53AIBridge(input: HubBridgeInput) {
  const statePath = join(input.stateDir, "claw-control-center-53aihub.json");
  let state: StoredHubState = { mappings: {}, outbox: [] };
  let socket: WebSocket | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let reconnectTimer: NodeJS.Timeout | null = null;
  let stopped = false;
  let reconnectAttempts = 0;
  let connectionStatus: Hub53AIStatusSnapshot["connectionStatus"] = input.config.enabled ? "disconnected" : "disabled";
  let lastHeartbeatAt: string | undefined;
  let lastHeartbeatProbeAtMs = 0;
  let lastHeartbeatAckAtMs = 0;
  let lastConnectedAt: string | undefined;
  let lastError: string | undefined;
  let receivedMessageCount = 0;
  let sentMessageCount = 0;
  const chatQueues = new Map<string, Promise<void>>();
  const lastReplyByReq = new Map<string, string>();
  const activeReqIdsBySession = new Map<string, Set<string>>();
  const activeRequestDetailsBySession = new Map<string, Map<string, ActiveSessionRequest>>();
  const syntheticEventsBySession = new Map<string, TimelineEvent[]>();
  const canonicalEventsBySession = new Map<string, TimelineEvent[]>();
  const ledgerSeqBySession = new Map<string, number>();
  const backfillLockBySession = new Map<string, Promise<void>>();
  const lastBackfillGatewaySeqBySession = new Map<string, number>();
  const ensuredRuntimeSkillsByKey = new Map<string, RuntimeSkillDisplayItem>();
  let persistStateQueue: Promise<void> = Promise.resolve();
  let persistStateTimer: NodeJS.Timeout | null = null;

  async function start() {
    await loadState();
    if (!input.config.enabled) {
      connectionStatus = "disabled";
      notifyStatus();
      return;
    }
    validateConfig(input.config);
    stopped = false;
    connect();
  }

  async function stop() {
    stopped = true;
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (socket) {
      socket.close();
      socket = null;
    }
    await persistState({ force: true });
  }

  function getStatus(): Hub53AIStatusSnapshot {
    return {
      enabled: input.config.enabled,
      configured: Boolean(input.config.botId && input.config.secret && input.config.wsUrl),
      connectionStatus,
      botId: maskBotId(input.config.botId),
      wsUrl: sanitizeWsUrl(input.config.wsUrl),
      lastHeartbeatAt,
      lastConnectedAt,
      lastError,
      receivedMessageCount,
      sentMessageCount,
      pendingOutboundCount: state.outbox.length
    };
  }

  function connect() {
    if (stopped) {
      return;
    }
    connectionStatus = "connecting";
    lastError = undefined;
    notifyStatus();

    const authBase64 = Buffer.from(`${input.config.botId}:${input.config.secret}`).toString("base64");
    const ws = new WebSocket(input.config.wsUrl, {
      headers: {
        Authorization: `Bearer ${input.config.secret}`,
        "Proxy-Authorization": `Basic ${authBase64}`,
        "X-Bot-Id": input.config.botId,
        "X-Api-Key": input.config.secret
      }
    });
    socket = ws;

    ws.on("open", () => {
      reconnectAttempts = 0;
      connectionStatus = "connected";
      lastConnectedAt = new Date().toISOString();
      lastHeartbeatProbeAtMs = 0;
      lastHeartbeatAckAtMs = Date.now();
      input.logger?.info?.(`[53aihub] connected to ${sanitizeWsUrl(input.config.wsUrl)}`);
      sendAppPing();
      void replayOutbox();
      heartbeatTimer = setInterval(() => {
        runHeartbeatCheck(ws);
      }, getHeartbeatIntervalMs());
      notifyStatus();
    });

    ws.on("message", (data) => {
      void handleRawMessage(data.toString()).catch((error) => {
        lastError = error instanceof Error ? error.message : String(error);
        input.logger?.error?.(`[53aihub] failed to process message: ${lastError}`);
        notifyStatus();
      });
    });

    ws.on("error", (error) => {
      lastError = error instanceof Error ? error.message : String(error);
      connectionStatus = "error";
      input.logger?.error?.(`[53aihub] websocket error: ${lastError}`);
      notifyStatus();
    });

    ws.on("close", (code, reason) => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      if (socket === ws) {
        socket = null;
      }
      if (stopped) {
        connectionStatus = "disabled";
        notifyStatus();
        return;
      }
      connectionStatus = "disconnected";
      lastError = `WebSocket closed: ${code}${reason.length ? ` ${reason.toString()}` : ""}`;
      notifyStatus();
      scheduleReconnect();
    });
  }

  function getHeartbeatIntervalMs(): number {
    const value = input.config.heartbeatIntervalMs;
    return Number.isFinite(value) && value! > 0 ? Math.max(1_000, Math.floor(value!)) : HEARTBEAT_INTERVAL_MS;
  }

  function getHeartbeatTimeoutMs(): number {
    const value = input.config.heartbeatTimeoutMs;
    return Number.isFinite(value) && value! > 0 ? Math.max(2_000, Math.floor(value!)) : HEARTBEAT_TIMEOUT_MS;
  }

  function markHeartbeatAcked() {
    lastHeartbeatAckAtMs = Date.now();
    lastHeartbeatAt = new Date(lastHeartbeatAckAtMs).toISOString();
    notifyStatus();
  }

  function runHeartbeatCheck(ws: WebSocket) {
    if (stopped || socket !== ws) {
      return;
    }
    if (ws.readyState !== WebSocket.OPEN) {
      forceReconnect(ws, `WebSocket unhealthy readyState=${ws.readyState}`);
      return;
    }

    const heartbeatTimeoutMs = getHeartbeatTimeoutMs();
    const now = Date.now();
    if (
      lastHeartbeatProbeAtMs > 0 &&
      lastHeartbeatAckAtMs < lastHeartbeatProbeAtMs &&
      now - lastHeartbeatProbeAtMs >= heartbeatTimeoutMs
    ) {
      forceReconnect(ws, `WebSocket heartbeat timed out after ${heartbeatTimeoutMs}ms`);
      return;
    }

    try {
      (ws as WebSocket & { ping?: () => void }).ping?.();
    } catch (error) {
      forceReconnect(ws, `WebSocket ping failed: ${error instanceof Error ? error.message : String(error)}`);
      return;
    }
    sendAppPing();
  }

  function forceReconnect(ws: WebSocket, reason: string) {
    if (stopped || socket !== ws) {
      return;
    }
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
    socket = null;
    connectionStatus = "disconnected";
    lastError = reason;
    input.logger?.warn?.(`[53aihub] ${reason}; reconnecting`);
    notifyStatus();
    try {
      if (typeof (ws as WebSocket & { terminate?: () => void }).terminate === "function") {
        (ws as WebSocket & { terminate: () => void }).terminate();
      } else {
        ws.close();
      }
    } catch {
      // The reconnect schedule below is the recovery path.
    }
    scheduleReconnect();
  }

  function scheduleReconnect() {
    if (stopped) {
      return;
    }
    if (reconnectTimer) {
      return;
    }

    const maxReconnectAttempts = Math.max(1, input.config.maxReconnectAttempts);
    const exceededConfiguredAttempts = reconnectAttempts >= maxReconnectAttempts;
    if (exceededConfiguredAttempts) {
      connectionStatus = "error";
      lastError = `Max reconnect attempts (${input.config.maxReconnectAttempts}) reached; continuing background reconnects`;
      notifyStatus();
    }

    const backoffExponent = Math.min(reconnectAttempts, maxReconnectAttempts - 1);
    const backoff = Math.min(input.config.reconnectBaseMs * 2 ** backoffExponent, 30_000);
    reconnectAttempts += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, backoff);
  }

  async function handleRawMessage(rawPayload: string) {
    const heartbeat = parseHeartbeat(rawPayload);
    if (heartbeat === "pong") {
      markHeartbeatAcked();
      return;
    }
    if (heartbeat === "ping") {
      sendRaw(JSON.stringify({ action: "pong", data: { botId: input.config.botId } }));
      markHeartbeatAcked();
      return;
    }

    const rpcRequest = parseRPCRequest(rawPayload);
    if (rpcRequest) {
      await processRPCRequest(rpcRequest);
      return;
    }

    const message = parseIncomingMessage(rawPayload);
    if (!message) {
      return;
    }
    receivedMessageCount += 1;
    notifyStatus();

    const previous = chatQueues.get(message.chatId) ?? Promise.resolve();
    const next = previous
      .catch(() => undefined)
      .then(() => processIncomingMessage(message));
    chatQueues.set(message.chatId, next);
    await next.finally(() => {
      if (chatQueues.get(message.chatId) === next) {
        chatQueues.delete(message.chatId);
      }
    });
  }

  async function processRPCRequest(request: Hub53AIRPCRequest) {
    try {
      const data = await resolveRPCRequest(request);
      sendRPCFrame({
        req_id: request.reqId,
        action: request.action,
        status: "done",
        data
      });
    } catch (error) {
      const rpcError = normalizeRPCError(error);
      sendRPCFrame({
        req_id: request.reqId,
        action: request.action,
        status: "error",
        data: rpcError
      });
    }
  }

  async function resolveRPCRequest(request: Hub53AIRPCRequest): Promise<unknown> {
    if (request.action === "sessions.list") {
      const pagination = readRPCPagination(request.data, 50);
      const page = await input.gateway.listSessionPage({
        limit: pagination.limit,
        offset: pagination.offset
      });
      return mergeKnownHubSessionTitles(page);
    }

    if (request.action === "sessions.current") {
      return resolveCurrentSessionRPC(request.data);
    }

    if (request.action === "sessions.messages") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      await ensureKnownGatewaySession(sessionId);
      const pagination = readRPCPagination(payload, 100);
      const requestedFetchLimit = pagination.offset + pagination.limit;
      const fetchLimit = requestedFetchLimit + MAX_SESSION_MESSAGE_TURN_BOUNDARY_OVERSCAN;
      const messages = await input.gateway.getSessionMessages(sessionId, fetchLimit);
      const localMessages = input.callbacks.listSessionMessages
        ? await Promise.resolve(input.callbacks.listSessionMessages(sessionId)).catch(() => [])
        : [];
      const rawPageMessages = sliceLatestWindowPage(messages, pagination.limit, pagination.offset);
      const boundaryPage = sliceLatestWindowPageWithTurnBoundary(messages, pagination.limit, pagination.offset);
      const pageMessages = mergeHubLocalAssistantMessages(
        mergeHubUserMessageMetadata(
          boundaryPage.items,
          Array.isArray(localMessages) ? localMessages : []
        ),
        Array.isArray(localMessages) ? localMessages : []
      );
      const nextOffset = Math.max(
        pagination.offset + rawPageMessages.length,
        messages.length - boundaryPage.start
      );
      const total = messages.length >= fetchLimit
        ? Math.max(requestedFetchLimit + 1, nextOffset + 1)
        : messages.length;
      await ensureCanonicalLedgerBackfill(sessionId);
      const ledgerEvents = listCanonicalLedgerEventsForMessagePage(sessionId, pageMessages, 0, pagination);
      const events = listCanonicalTimelineEventsForLedgerEvents(sessionId, ledgerEvents);
      return {
        messages: pageMessages,
        events,
        ledger_events: ledgerEvents,
        ledgerEvents,
        pagination: buildPaginationWithNextOffset(pagination.limit, pagination.offset, total, nextOffset)
      };
    }

    if (request.action === "sessions.events") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      await ensureKnownGatewaySession(sessionId);
      const pagination = readRPCPagination(payload, 100);
      const afterSeq = readRPCAfterSeq(payload);
      const events = (await listSessionEvents(sessionId)).filter((event) => event.seq > afterSeq);
      const pageEvents = events.slice(pagination.offset, pagination.offset + pagination.limit);
      const ledgerEvents = listCanonicalLedgerEvents(sessionId, afterSeq);
      return {
        events: pageEvents,
        ledger_events: ledgerEvents,
        ledgerEvents,
        pagination: buildPagination(pagination.limit, pagination.offset, events.length, pageEvents.length)
      };
    }

    if (request.action === "sessions.snapshot") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      await ensureKnownGatewaySession(sessionId);
      const afterSeq = readRPCAfterSeq(payload);
      await ensureCanonicalLedgerBackfill(sessionId);
      const result = buildOpenClawSessionSnapshot(sessionId, listCanonicalLedgerEvents(sessionId), afterSeq);
      return result;
    }

    if (request.action === "sessions.control") {
      const payload = toRecord(request.data);
      const sessionId = readRPCSessionId(payload);
      const action = stringOr(payload.action);
      if (action !== "stop") {
        throw new HubRPCError("PARAM_ERROR", "unsupported sessions.control action");
      }
      void input.gateway.controlSession(sessionId, "stop").catch((error) => {
        input.logger?.warn?.(
          `[53aihub] failed to stop session ${sessionId}: ${error instanceof Error ? error.message : String(error)}`
        );
      });
      await resolveActiveSessionRequests(sessionId, "control.stop");
      return {
        ok: true,
        action,
        session_id: sessionId,
        conversation_id: sessionId
      };
    }

    if (request.action === "runtime.get") {
      return resolveRuntimeRPC(request.data);
    }

    if (request.action === "runtime.skills.ensure") {
      const ensureRequest = toRecord(request.data) as EnsureHubSkillRequest;
      const result = await ensureHubSkillInstalled({
        request: ensureRequest,
        configPath: input.configPath,
        stateDir: input.stateDir,
        hub: {
          botId: input.config.botId,
          secret: input.config.secret,
          wsUrl: input.config.wsUrl
        },
        logger: input.logger
      });
      rememberEnsuredRuntimeSkill(ensureRequest, result);
      return result;
    }

    if (request.action === "cron.tasks") {
      const pagination = readRPCPagination(request.data, 100);
      const runtimeInfo = await input.gateway.getRuntimeInfo();
      const tasks = runtimeInfo.cronTasks ?? [];
      const pageTasks = tasks.slice(pagination.offset, pagination.offset + pagination.limit);
      return {
        tasks: pageTasks,
        cronTasks: pageTasks,
        ...(runtimeInfo.cronScheduler ? { scheduler: runtimeInfo.cronScheduler, cronScheduler: runtimeInfo.cronScheduler } : {}),
        pagination: buildPagination(pagination.limit, pagination.offset, tasks.length, pageTasks.length)
      };
    }

    throw new HubRPCError("FEATURE_NOT_AVAILABLE", `Unsupported RPC action: ${request.action}`);
  }

  async function ensureKnownGatewaySession(sessionId: string): Promise<void> {
    let offset = 0;
    for (let pageIndex = 0; pageIndex < HUB_SESSION_VALIDATION_MAX_PAGES; pageIndex += 1) {
      const page: GatewaySessionPage = await input.gateway.listSessionPage({
        limit: HUB_SESSION_VALIDATION_PAGE_LIMIT,
        offset
      });
      if (page.sessions.some((session) => session.id === sessionId)) {
        return;
      }
      if (!page.pagination?.hasMore) {
        throw new HubRPCError("NOT_FOUND", `OpenClaw session not found: ${sessionId}`);
      }
      const nextOffset = page.pagination.nextOffset;
      if (typeof nextOffset === "number" && nextOffset > offset) {
        offset = nextOffset;
        continue;
      }
      if (page.sessions.length > 0) {
        offset += page.sessions.length;
        continue;
      }
      break;
    }
    throw new HubRPCError("NOT_FOUND", `OpenClaw session not found: ${sessionId}`);
  }

  async function listSessionEvents(sessionId: string): Promise<TimelineEvent[]> {
    const lastSeq = getLastBackfillGatewaySeq(sessionId);
    const t0 = Date.now();
    const [gatewayEvents, storedEvents] = await Promise.all([
      input.gateway.listEvents(sessionId, lastSeq),
      input.callbacks.listSessionEvents?.(sessionId) ?? []
    ]);
    const t1 = Date.now();
    const rawEvents = filterSyntheticToolPlaceholderThinkingEvents(
      filterSupersededHistoryThinkingEvents(
        dedupeTimelineEvents([...gatewayEvents, ...storedEvents])
          .map(normalizeTimelineEventSegmentType)
          .map(normalizeTimelineEventMessageSeq)
      )
    );
    await ensureCanonicalLedgerBackfillFromEvents(sessionId, rawEvents);
    const canonicalEvents = listCanonicalSessionEvents(sessionId);
    const exposedEvents = dedupeTimelineEvents(canonicalEvents);
    const rawOnlyHiddenCount = rawEvents.filter(
      (event) => !normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event))
    ).length;
    traceOpenClawDuplicate(input.logger, "hub.events.list", {
      sessionId,
      gatewayCount: gatewayEvents.length,
      rawInputCount: rawEvents.length,
      canonicalCount: canonicalEvents.length,
      rawOnlyHiddenCount,
      elapsed: t1 - t0
    }, input.config);
    return exposedEvents;
  }

  function getLastBackfillGatewaySeq(sessionId: string): number {
    return lastBackfillGatewaySeqBySession.get(sessionId) ?? 0;
  }

  function setLastBackfillGatewaySeq(sessionId: string, seq: number): void {
    if (seq > 0) {
      lastBackfillGatewaySeqBySession.set(sessionId, seq);
    }
  }

  async function ensureCanonicalLedgerBackfill(sessionId: string) {
    if (!sessionId) {
      return;
    }
    const existingLock = backfillLockBySession.get(sessionId);
    if (existingLock) {
      return existingLock;
    }
    const lockPromise = (async () => {
      const lastSeq = getLastBackfillGatewaySeq(sessionId);
      const [gatewayEvents, storedEvents] = await Promise.all([
        input.gateway.listEvents(sessionId, lastSeq),
        input.callbacks.listSessionEvents?.(sessionId) ?? []
      ]);
      const maxGatewaySeq = gatewayEvents.reduce((max, e) => Math.max(max, (e as TimelineEvent & { seq?: number }).seq ?? 0), 0);
      const rawEvents = filterSyntheticToolPlaceholderThinkingEvents(
        filterSupersededHistoryThinkingEvents(
          dedupeTimelineEvents([...gatewayEvents, ...storedEvents])
            .map(normalizeTimelineEventSegmentType)
            .map(normalizeTimelineEventMessageSeq)
        )
      );
      await ensureCanonicalLedgerBackfillFromEvents(sessionId, rawEvents);
      if (maxGatewaySeq > lastSeq) {
        setLastBackfillGatewaySeq(sessionId, maxGatewaySeq);
      }
    })();
    backfillLockBySession.set(sessionId, lockPromise);
    try {
      return lockPromise;
    } finally {
      if (backfillLockBySession.get(sessionId) === lockPromise) {
        backfillLockBySession.delete(sessionId);
      }
    }
  }

  async function ensureCanonicalLedgerBackfillFromEvents(sessionId: string, events: TimelineEvent[]) {
    if (!sessionId) {
      return;
    }
    if (!events.length) {
      await ensureCanonicalManifestOutputBackfill(sessionId);
      return;
    }

    const initialCanonicalEvents = listCanonicalSessionEvents(sessionId);
    const liveCompletedRunIds = collectOpenClawLiveCompletedRunIds(initialCanonicalEvents);
    const turnGroups = collectCompletedHistoryLedgerBackfillGroups(sessionId, events).filter(
      (group) => !group.runId || !liveCompletedRunIds.has(group.runId)
    );
    const manifestFiles = await collectConversationManifestFilesForBackfill(sessionId);
    const verifiedManifestScopes = buildVerifiedHistoryManifestScopes(sessionId, turnGroups, manifestFiles);
    const expectedBackfillRefs = buildExpectedHistoryBackfillRefs(sessionId, turnGroups, verifiedManifestScopes);
    const rewrittenExisting = pruneCanonicalHistoryBackfillEvents(sessionId, expectedBackfillRefs, liveCompletedRunIds);
    const existingCanonicalEvents = listCanonicalSessionEvents(sessionId);
    const existingEventIds = new Set(existingCanonicalEvents.map((event) => event.id).filter(Boolean));
    const existingRawRefs = new Set(
      extractOpenClawLedgerEvents(existingCanonicalEvents)
        .map((event) => event.raw_event_ref)
        .filter((ref): ref is string => Boolean(ref))
    );

    let appended = 0;
    let skippedExisting = 0;
    let skippedIncomplete = 0;

    for (const group of turnGroups) {
      if (!group.terminalSeen) {
        skippedIncomplete += group.events.length;
        continue;
      }
      const identity = group.runId || `history:${group.firstSeq || group.events[0]?.id || "turn"}`;
      const eventScope = createHistoryLedgerBackfillScope(sessionId, identity, verifiedManifestScopes.get(group));
      if (group.runId) {
        eventScope.currentRunId = group.runId;
      }

      const orderedEvents = [
        ...group.events.filter((event) => !isTerminalRunEvent(event)),
        ...group.events.filter((event) => isTerminalRunEvent(event))
      ];
      let typedFinalChecked = false;
      for (const event of orderedEvents) {
        if (!typedFinalChecked && isTerminalRunEvent(event)) {
          typedFinalChecked = true;
          const typedFinalAppended = await appendTypedFinalReplaceForHistoryGroup(sessionId, group, eventScope, event);
          if (typedFinalAppended) {
            appended += 1;
          }
        }
        if (!shouldAttachOpenClawTimeline(event)) {
          continue;
        }
        const rawEventRef = buildOpenClawRawEventRef(event);
        if ((event.id && existingEventIds.has(event.id)) || existingRawRefs.has(rawEventRef)) {
          skippedExisting += 1;
          continue;
        }
        applyOpenClawEventScopeActivity(event, eventScope);
        augmentPayloadWithEventMeta(event, eventScope);
        appendWriteToolOutputFilesForHistoryEvent(sessionId, event, eventScope);
        appended += 1;
        if (event.id) {
          existingEventIds.add(event.id);
        }
        existingRawRefs.add(rawEventRef);
      }
    }

    if (appended || skippedIncomplete || rewrittenExisting) {
      traceOpenClawLedger(input.logger, "history-backfill", {
        sessionId,
        groupCount: turnGroups.length,
        appended,
        rewrittenExisting,
        skippedExisting,
        skippedIncomplete,
        skippedCompletedLiveRuns: liveCompletedRunIds.size
      }, input.config);
    }
    await ensureCanonicalManifestOutputBackfill(sessionId);
  }

  async function ensureCanonicalManifestOutputBackfill(sessionId: string): Promise<number> {
    if (!sessionId) {
      return 0;
    }
    const manifestPath = resolveLocalOutputManifestPath({
      stateDir: input.stateDir,
      conversationId: sessionId
    });
    const manifestFiles = await collectConversationManifestFilesForBackfill(sessionId);
    if (manifestFiles.length === 0) {
      return 0;
    }

    const existingKeys = new Set<string>();
    for (const event of extractOpenClawLedgerEvents(listCanonicalSessionEvents(sessionId))) {
      if (event.part_type !== "output_file") {
        continue;
      }
      for (const file of extractOutputFilesFromPayload(event.payload)) {
        for (const key of buildManifestOutputFileBackfillKeys(event.active_request_id, file)) {
          existingKeys.add(key);
        }
      }
    }

    const groups = new Map<string, ManifestOutputBackfillGroup>();
    let skippedExisting = 0;
    for (const file of manifestFiles) {
      const keys = buildManifestOutputFileBackfillKeys(file.active_request_id, file);
      if (keys.some((key) => existingKeys.has(key))) {
        skippedExisting += 1;
        continue;
      }
      const groupKey = `${file.turn_id}\0${file.active_request_id}`;
      const group = groups.get(groupKey) ?? {
        turnId: file.turn_id,
        activeRequestId: file.active_request_id,
        files: []
      };
      group.files.push(file);
      groups.set(groupKey, group);
      for (const key of keys) {
        existingKeys.add(key);
      }
    }

    let appended = 0;
    for (const group of groups.values()) {
      const eventScope = createManifestOutputBackfillScope(sessionId, group.turnId, group.activeRequestId);
      const outputTimeline = buildOpenClawOutputFilesTimelineMeta(eventScope, group.files);
      const hubFiles = await uploadOutputFilesToHub(
        "manifest-output-backfill",
        sessionId,
        group.files,
        eventScope,
        outputTimeline
      );
      const ledgerSourceEvent = await buildOutputFilesLedgerSourceEvent(
        sessionId,
        hubFiles,
        eventScope,
        outputTimeline
      );
      const outputLedger = buildOpenClawLedgerEvent(ledgerSourceEvent, eventScope, outputTimeline);
      appendCanonicalSessionEvent(
        sessionId,
        buildCanonicalOutputFilesTimelineEvent(ledgerSourceEvent, outputTimeline, outputLedger)
      );
      appended += group.files.length;
    }

    if (appended || skippedExisting) {
      traceOpenClawLedger(input.logger, "manifest-output-backfill", {
        sessionId,
        manifestPath,
        manifestFiles: manifestFiles.length,
        appended,
        skippedExisting
      }, input.config);
    }
    return appended;
  }

  async function collectConversationManifestFilesForBackfill(sessionId: string): Promise<LocalOutputManifestFile[]> {
    const manifestPath = resolveLocalOutputManifestPath({
      stateDir: input.stateDir,
      conversationId: sessionId
    });
    return collectConversationManifestLocalOutputFiles({
      config: input.config,
      configPath: input.configPath,
      stateDir: input.stateDir,
      manifestPath,
      conversationId: sessionId,
      logger: input.logger
    });
  }

  function buildManifestOutputFileBackfillKeys(activeRequestId: string, file: Hub53AIOutputFile): string[] {
    const active = activeRequestId || "";
    const name = file.file_name || "";
    const size = typeof file.size === "number" && Number.isFinite(file.size) ? String(file.size) : "";
    const sha256 = typeof file.sha256 === "string" ? file.sha256.trim().toLowerCase() : "";
    return [
      active && name && sha256 ? `${active}\0${name}\0sha256:${sha256}` : "",
      active && name && size ? `${active}\0${name}\0size:${size}` : ""
    ].filter(Boolean);
  }

  function createManifestOutputBackfillScope(sessionId: string, turnId: string, activeRequestId: string): GatewayEventScope {
    return {
      eventBoundaryMs: 0,
      turnId,
      activeRequestId,
      currentTurnId: turnId,
      nextSegmentIndex: 0,
      nextAnswerSegmentIndex: 0,
      answerBoundaryAfterVisibleResponse: false,
      activityAppliedEventKeys: new Set<string>(),
      answerContentAppliedEventKeys: new Set<string>(),
      answerSegmentTextById: new Map<string, string>(),
      answerSegmentVisibilityById: new Map<string, OpenClawTimelineV2Visibility>(),
      answerSegmentTrustedById: new Map<string, boolean>(),
      nextDeltaIndexBySegment: new Map<string, number>(),
      segmentIndexById: new Map<string, number>(),
      timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
      typedToolCallEnrichedKeys: new Set<string>(),
      currentActivitySeen: false,
      visibleResponseSeen: false,
      lastSeqSeen: computeMaxOpenClawLedgerSeqForSession(sessionId),
      emittedOutputFileKeys: new Set<string>(),
      referencedLocalOutputPaths: new Set<string>(),
      writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>()
    };
  }

  type HistoryBackfillExpectedRef = {
    turnId: string;
    activeRequestId: string;
    runId?: string;
  };

  function buildExpectedHistoryBackfillRefs(
    sessionId: string,
    groups: HistoryLedgerBackfillGroup[],
    verifiedManifestScopes: Map<HistoryLedgerBackfillGroup, VerifiedHistoryManifestScope> = new Map()
  ): Map<string, HistoryBackfillExpectedRef> {
    const refs = new Map<string, HistoryBackfillExpectedRef>();
    for (const group of groups) {
      if (!group.terminalSeen) {
        continue;
      }
      const identity = group.runId || `history:${group.firstSeq || group.events[0]?.id || "turn"}`;
      const verifiedScope = verifiedManifestScopes.get(group);
      const activeRequestId = verifiedScope?.activeRequestId || `history:${identity}`;
      const expected: HistoryBackfillExpectedRef = {
        turnId: verifiedScope?.turnId || buildOpenClawTimelineTurnId(sessionId, activeRequestId),
        activeRequestId,
        ...(group.runId ? { runId: group.runId } : {})
      };
      for (const event of group.events) {
        if (shouldAttachOpenClawTimeline(event)) {
          refs.set(buildOpenClawRawEventRef(event), expected);
        }
      }
    }
    return refs;
  }

  function buildVerifiedHistoryManifestScopes(
    sessionId: string,
    groups: HistoryLedgerBackfillGroup[],
    manifestFiles: LocalOutputManifestFile[]
  ): Map<HistoryLedgerBackfillGroup, VerifiedHistoryManifestScope> {
    const manifestGroups = buildManifestOutputBackfillGroups(manifestFiles);
    if (!groups.length || !manifestGroups.length) {
      return new Map();
    }

    const candidateByGroup = new Map<HistoryLedgerBackfillGroup, VerifiedHistoryManifestScope>();
    const groupsByActiveRequestId = new Map<string, HistoryLedgerBackfillGroup[]>();
    for (const group of groups) {
      if (!group.terminalSeen || !group.runId) {
        continue;
      }
      const window = readHistoryBackfillRunWindowMs(group);
      if (!window) {
        continue;
      }
      const matchingManifestGroups = manifestGroups.filter((manifestGroup) =>
        isManifestActiveRequestInHistoryRunWindow(manifestGroup.activeRequestId, window)
      );
      if (matchingManifestGroups.length !== 1) {
        if (matchingManifestGroups.length > 1) {
          traceOpenClawLedger(input.logger, "history-manifest-scope-skip", {
            sessionId,
            reason: "ambiguous_manifest_active_request_window",
            runId: group.runId,
            activeRequestIds: matchingManifestGroups.map((candidate) => candidate.activeRequestId)
          }, input.config);
        }
        continue;
      }
      const manifestGroup = matchingManifestGroups[0];
      candidateByGroup.set(group, {
        turnId: manifestGroup.turnId,
        activeRequestId: manifestGroup.activeRequestId
      });
      const claimants = groupsByActiveRequestId.get(manifestGroup.activeRequestId) ?? [];
      claimants.push(group);
      groupsByActiveRequestId.set(manifestGroup.activeRequestId, claimants);
    }

    const verified = new Map<HistoryLedgerBackfillGroup, VerifiedHistoryManifestScope>();
    for (const [group, scope] of candidateByGroup) {
      const claimants = groupsByActiveRequestId.get(scope.activeRequestId) ?? [];
      if (claimants.length !== 1) {
        traceOpenClawLedger(input.logger, "history-manifest-scope-skip", {
          sessionId,
          reason: "manifest_active_request_claimed_by_multiple_history_runs",
          activeRequestId: scope.activeRequestId,
          runIds: claimants.map((candidate) => candidate.runId).filter(Boolean)
        }, input.config);
        continue;
      }
      verified.set(group, scope);
    }

    if (verified.size > 0) {
      traceOpenClawLedger(input.logger, "history-manifest-scope", {
        sessionId,
        matched: [...verified.entries()].map(([group, scope]) => ({
          runId: group.runId,
          turnId: scope.turnId,
          activeRequestId: scope.activeRequestId
        }))
      }, input.config);
    }
    return verified;
  }

  function buildManifestOutputBackfillGroups(manifestFiles: LocalOutputManifestFile[]): ManifestOutputBackfillGroup[] {
    const groups = new Map<string, ManifestOutputBackfillGroup>();
    for (const file of manifestFiles) {
      const groupKey = `${file.turn_id}\0${file.active_request_id}`;
      const group = groups.get(groupKey) ?? {
        turnId: file.turn_id,
        activeRequestId: file.active_request_id,
        files: []
      };
      group.files.push(file);
      groups.set(groupKey, group);
    }
    return [...groups.values()];
  }

  type HistoryBackfillRunWindowMs = {
    startedAtMs: number;
    endedAtMs: number;
  };

  function readHistoryBackfillRunWindowMs(group: HistoryLedgerBackfillGroup): HistoryBackfillRunWindowMs | null {
    const starts: number[] = [];
    const ends: number[] = [];
    for (const event of group.events) {
      const payload = toRecord(event.payload);
      const data = toRecord(payload.data);
      const session = toRecord(payload.session);
      const startedAt = firstTimestampMs(
        payload.startedAt,
        payload.started_at,
        data.startedAt,
        data.started_at,
        session.startedAt,
        session.started_at,
        event.kind === "run.started" ? event.createdAt : undefined
      );
      if (startedAt) {
        starts.push(startedAt);
      }
      const endedAt = firstTimestampMs(
        payload.endedAt,
        payload.ended_at,
        data.endedAt,
        data.ended_at,
        session.endedAt,
        session.ended_at,
        isTerminalRunEvent(event) ? event.createdAt : undefined
      );
      if (endedAt) {
        ends.push(endedAt);
      }
    }
    if (!starts.length && !ends.length) {
      return null;
    }
    const startedAtMs = starts.length ? Math.min(...starts) : Math.min(...ends);
    const endedAtMs = ends.length ? Math.max(...ends) : Math.max(...starts);
    if (!Number.isFinite(startedAtMs) || !Number.isFinite(endedAtMs) || endedAtMs <= 0) {
      return null;
    }
    return {
      startedAtMs: Math.min(startedAtMs, endedAtMs),
      endedAtMs: Math.max(startedAtMs, endedAtMs)
    };
  }

  function isManifestActiveRequestInHistoryRunWindow(
    activeRequestId: string,
    window: HistoryBackfillRunWindowMs
  ): boolean {
    const activeRequestMs = parseManifestActiveRequestTimestampMs(activeRequestId);
    if (!activeRequestMs) {
      return false;
    }
    const slackMs = 5_000;
    return activeRequestMs >= window.startedAtMs - slackMs && activeRequestMs <= window.endedAtMs + slackMs;
  }

  function parseManifestActiveRequestTimestampMs(activeRequestId: string): number {
    const parsed = Number(activeRequestId);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function firstTimestampMs(...values: unknown[]): number {
    for (const value of values) {
      const parsed = timestampMs(value);
      if (parsed > 0) {
        return parsed;
      }
    }
    return 0;
  }

  function timestampMs(value: unknown): number {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value > 0 && value < 10_000_000_000 ? value * 1000 : value;
    }
    if (typeof value === "string" && value.trim()) {
      const numeric = Number(value);
      if (Number.isFinite(numeric)) {
        return numeric > 0 && numeric < 10_000_000_000 ? numeric * 1000 : numeric;
      }
      const parsed = Date.parse(value);
      return Number.isFinite(parsed) ? parsed : 0;
    }
    return 0;
  }

  function pruneCanonicalHistoryBackfillEvents(
    sessionId: string,
    expectedRefs: Map<string, HistoryBackfillExpectedRef>,
    skipRunIds: Set<string> = new Set()
  ): number {
    if (!sessionId || (expectedRefs.size === 0 && skipRunIds.size === 0)) {
      return 0;
    }

    const events = canonicalEventsBySession.get(sessionId) ?? [];
    if (events.length === 0) {
      return 0;
    }

    const nextEvents = events.filter((event) => {
      const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
      if (!ledger || !ledger.active_request_id.startsWith("history:")) {
        return true;
      }
      if (ledger.run_id && skipRunIds.has(ledger.run_id)) {
        return false;
      }
      const expected = ledger.raw_event_ref ? expectedRefs.get(ledger.raw_event_ref) : undefined;
      if (!expected) {
        return false;
      }
      return (
        ledger.turn_id === expected.turnId &&
        ledger.active_request_id === expected.activeRequestId &&
        (!expected.runId || ledger.run_id === expected.runId)
      );
    });
    const removed = events.length - nextEvents.length;
    if (removed <= 0) {
      return 0;
    }

    canonicalEventsBySession.set(sessionId, nextEvents);
    setPersistedSessionEvents("canonicalEventsBySession", sessionId, nextEvents, MAX_CANONICAL_EVENTS_PER_SESSION);
    return removed;
  }

  type HistoryLedgerBackfillGroup = {
    runId: string;
    firstSeq: number;
    lastSeq: number;
    terminalSeen: boolean;
    messageSeqs: Set<number>;
    events: TimelineEvent[];
  };

  function collectCompletedHistoryLedgerBackfillGroups(
    sessionId: string,
    events: TimelineEvent[]
  ): HistoryLedgerBackfillGroup[] {
    const groups: HistoryLedgerBackfillGroup[] = [];
    const groupByMessageSeq = new Map<string, HistoryLedgerBackfillGroup>();
    const deferredHistoryEvents: TimelineEvent[] = [];
    let current: HistoryLedgerBackfillGroup | null = null;

    const flush = () => {
      if (current && current.events.length) {
        groups.push(current);
      }
      current = null;
    };

    const addMessageSeqMapping = (group: HistoryLedgerBackfillGroup, event: TimelineEvent) => {
      for (const messageSeq of readBackfillAssistantMessageSeqs(event)) {
        group.messageSeqs.add(messageSeq);
        groupByMessageSeq.set(buildHistoryBackfillMessageSeqKey(group.runId, messageSeq), group);
        if (!group.runId) {
          groupByMessageSeq.set(buildHistoryBackfillMessageSeqKey("", messageSeq), group);
        }
      }
    };

    const sortedEvents = events
      .filter((event) => event.sessionId === sessionId)
      .sort((left, right) => (left.seq || 0) - (right.seq || 0));

    for (const event of sortedEvents) {
      const runId = getGatewayEventRunIdentity(event);
      if (event.kind === "run.started") {
        flush();
        current = createHistoryLedgerBackfillGroup(event, runId);
      } else if (!current && (runId || shouldAttachOpenClawTimeline(event))) {
        current = createHistoryLedgerBackfillGroup(event, runId);
      } else if (runId && current?.runId && current.runId !== runId) {
        flush();
        current = createHistoryLedgerBackfillGroup(event, runId);
      } else if (runId && current && !current.runId) {
        current.runId = runId;
      }

      if (!current) {
        continue;
      }

      if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
        current.firstSeq = current.firstSeq > 0 ? Math.min(current.firstSeq, event.seq) : event.seq;
        current.lastSeq = Math.max(current.lastSeq, event.seq);
      }
      addMessageSeqMapping(current, event);

      if (shouldAttachOpenClawTimeline(event)) {
        if (readHistoryMessageSeq(event)) {
          deferredHistoryEvents.push(event);
        } else {
          current.events.push(event);
        }
      }

      if (isTerminalRunEvent(event)) {
        current.terminalSeen = true;
        flush();
      }
    }

    flush();
    for (const event of deferredHistoryEvents) {
      const targetGroup = findHistoryBackfillGroupForEvent(groups, groupByMessageSeq, event);
      if (!targetGroup) {
        continue;
      }
      targetGroup.events.push(event);
      if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
        targetGroup.firstSeq = targetGroup.firstSeq > 0 ? Math.min(targetGroup.firstSeq, event.seq) : event.seq;
        targetGroup.lastSeq = Math.max(targetGroup.lastSeq, event.seq);
      }
    }

    for (const group of groups) {
      group.events.sort((left, right) => (left.seq || 0) - (right.seq || 0));
    }

    return groups;
  }

  function createHistoryLedgerBackfillGroup(event: TimelineEvent, runId: string): HistoryLedgerBackfillGroup {
    return {
      runId,
      firstSeq: typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : 0,
      lastSeq: typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : 0,
      terminalSeen: false,
      messageSeqs: new Set<number>(),
      events: []
    };
  }

  function findHistoryBackfillGroupForEvent(
    groups: HistoryLedgerBackfillGroup[],
    groupByMessageSeq: Map<string, HistoryLedgerBackfillGroup>,
    event: TimelineEvent
  ): HistoryLedgerBackfillGroup | undefined {
    const historyMessageSeq = readHistoryMessageSeq(event);
    if (historyMessageSeq) {
      const runId = getGatewayEventRunIdentity(event);
      const mappedGroup = runId
        ? groupByMessageSeq.get(buildHistoryBackfillMessageSeqKey(runId, historyMessageSeq))
        : groupByMessageSeq.get(buildHistoryBackfillMessageSeqKey("", historyMessageSeq));
      if (mappedGroup) {
        return mappedGroup;
      }
      if (runId) {
        return undefined;
      }
      const matchingGroups = groups.filter((group) => group.messageSeqs.has(historyMessageSeq));
      if (matchingGroups.length === 1) {
        return matchingGroups[0];
      }
    }

    const eventSeq = typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : 0;
    if (!eventSeq) {
      return undefined;
    }

    return groups.find((group) => {
      if (!group.terminalSeen || !group.firstSeq || !group.lastSeq) {
        return false;
      }
      const runId = getGatewayEventRunIdentity(event);
      if (runId && group.runId && group.runId !== runId) {
        return false;
      }
      return eventSeq >= group.firstSeq && eventSeq <= group.lastSeq;
    });
  }

  function buildHistoryBackfillMessageSeqKey(runId: string, messageSeq: number): string {
    return `${runId || "*"}:${messageSeq}`;
  }

  function readBackfillAssistantMessageSeqs(event: TimelineEvent): number[] {
    if (event.kind !== "status.update" || readHistoryMessageSeq(event)) {
      return [];
    }

    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const session = toRecord(payload.session);
    const phase = String(payload.phase ?? payload.status ?? data.phase ?? data.status ?? session.phase ?? session.status ?? "").toLowerCase();
    if (phase && phase !== "message" && phase !== "assistant_message" && phase !== "final_message") {
      return [];
    }

    return uniquePositiveNumbers(
      payload.messageSeq,
      payload.message_seq,
      data.messageSeq,
      data.message_seq,
      session.messageSeq,
      session.message_seq
    );
  }

  function uniquePositiveNumbers(...values: unknown[]): number[] {
    const output: number[] = [];
    const seen = new Set<number>();
    for (const value of values) {
      const parsed = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(parsed) || parsed <= 0 || seen.has(parsed)) {
        continue;
      }
      seen.add(parsed);
      output.push(parsed);
    }
    return output;
  }

  function firstPositiveNumber(...values: unknown[]): number {
    for (const value of values) {
      const parsed = typeof value === "number" ? value : Number(value);
      if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
      }
    }
    return 0;
  }

  function createHistoryLedgerBackfillScope(
    sessionId: string,
    identity: string,
    verifiedManifestScope?: VerifiedHistoryManifestScope
  ): GatewayEventScope {
    const activeRequestId = verifiedManifestScope?.activeRequestId || `history:${identity}`;
    const turnId = verifiedManifestScope?.turnId || buildOpenClawTimelineTurnId(sessionId, activeRequestId);
    return {
      eventBoundaryMs: 0,
      turnId,
      activeRequestId,
      currentTurnId: turnId,
      nextSegmentIndex: 0,
      nextAnswerSegmentIndex: 0,
      answerBoundaryAfterVisibleResponse: false,
      activityAppliedEventKeys: new Set<string>(),
      answerContentAppliedEventKeys: new Set<string>(),
      answerSegmentTextById: new Map<string, string>(),
      answerSegmentVisibilityById: new Map<string, OpenClawTimelineV2Visibility>(),
      answerSegmentTrustedById: new Map<string, boolean>(),
      nextDeltaIndexBySegment: new Map<string, number>(),
      segmentIndexById: new Map<string, number>(),
      timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
      typedToolCallEnrichedKeys: new Set<string>(),
      currentActivitySeen: false,
      visibleResponseSeen: false,
      lastSeqSeen: 0,
      emittedOutputFileKeys: new Set<string>(),
      referencedLocalOutputPaths: new Set<string>(),
      writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>()
    };
  }

  async function appendTypedFinalReplaceForHistoryGroup(
    sessionId: string,
    group: HistoryLedgerBackfillGroup,
    eventScope: GatewayEventScope,
    terminalEvent: TimelineEvent
  ): Promise<boolean> {
    if (eventScope.activeRequestId.startsWith("history:")) {
      traceOpenClawLedger(input.logger, "history-typed-final-skip", {
        sessionId,
        reason: "unverified_history_turn_scope",
        activeRequestId: eventScope.activeRequestId,
        turnId: eventScope.currentTurnId || eventScope.turnId,
        terminalEvent: summarizeTimelineEventForTrace(terminalEvent)
      }, input.config);
      return false;
    }

    const typedFinal = await resolveTypedTranscriptFinal(sessionId, terminalEvent, eventScope);
    if (!typedFinal) {
      const removed = pruneWeakTypedFinalReplaceForHistoryScope(sessionId, eventScope, group.runId);
      traceOpenClawLedger(input.logger, "history-typed-final-skip", {
        sessionId,
        reason: "typed_final_not_found",
        removedStaleTypedFinalEvents: removed,
        activeRequestId: eventScope.activeRequestId,
        turnId: eventScope.currentTurnId || eventScope.turnId,
        terminalEvent: summarizeTimelineEventForTrace(terminalEvent)
      }, input.config);
      return false;
    }

    if (!isStrongTypedFinalMatchStrategy(typedFinal.matchStrategy)) {
      const removed = pruneWeakTypedFinalReplaceForHistoryScope(sessionId, eventScope, group.runId);
      traceOpenClawLedger(input.logger, "history-typed-final-skip", {
        sessionId,
        reason: "weak_typed_final_match_strategy",
        activeRequestId: eventScope.activeRequestId,
        turnId: eventScope.currentTurnId || eventScope.turnId,
        runId: group.runId,
        matchStrategy: typedFinal.matchStrategy,
        removedStaleTypedFinalEvents: removed,
        terminalEvent: summarizeTimelineEventForTrace(terminalEvent)
      }, input.config);
      return false;
    }

    const currentAnswer = readCurrentOpenClawAnswerText(eventScope, eventScope.activeRequestId);
    const event = buildTypedTranscriptFinalEvent(
      sessionId,
      terminalEvent,
      eventScope,
      eventScope.activeRequestId,
      typedFinal,
      currentAnswer
    );
    applyOpenClawEventScopeActivity(event, eventScope);
    event.payload = augmentPayloadWithEventMeta(event, eventScope);
    appendCanonicalSessionEvent(sessionId, event);

    traceOpenClawLedger(input.logger, "history-typed-final", {
      sessionId,
      result: "canonicalized_verified_manifest_scope",
      activeRequestId: eventScope.activeRequestId,
      turnId: eventScope.currentTurnId || eventScope.turnId,
      runId: group.runId,
      matchStrategy: typedFinal.matchStrategy,
      textLength: typedFinal.text.length,
      textHash: hashTraceText(typedFinal.text),
      event: summarizeTimelineEventForTrace(event)
    }, input.config);
    return true;
  }

  function isStrongTypedFinalMatchStrategy(strategy?: string): boolean {
    return strategy === "run_id" || strategy === "response_id";
  }

  function pruneWeakTypedFinalReplaceForHistoryScope(
    sessionId: string,
    eventScope: GatewayEventScope,
    runId?: string
  ): number {
    const events = canonicalEventsBySession.get(sessionId) ?? [];
    let removed = 0;
    const nextEvents = events.filter((event) => {
      const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
      if (!ledger || ledger.part_type !== "answer" || ledger.active_request_id !== eventScope.activeRequestId) {
        return true;
      }
      const payload = toRecord(event.payload);
      const ledgerPayload = toRecord(ledger.payload);
      const sourceKind = readStringMetadata(payload, "source_kind") || readStringMetadata(ledgerPayload, "source_kind");
      const isTypedFinal = sourceKind === "typed_transcript.final_replace" ||
        readBooleanMetadata(payload, "typed_final") === true ||
        readBooleanMetadata(ledgerPayload, "typed_final") === true;
      if (!isTypedFinal) {
        return true;
      }
      const candidateRunId = ledger.run_id || getGatewayEventRunIdentity(event) || stringOr(payload.runId, payload.run_id);
      if (runId && candidateRunId && candidateRunId !== runId) {
        return true;
      }
      const matchStrategy = readStringMetadata(payload, "typed_final_match_strategy") ||
        readStringMetadata(ledgerPayload, "typed_final_match_strategy");
      if (isStrongTypedFinalMatchStrategy(matchStrategy)) {
        return true;
      }
      removed += 1;
      return false;
    });
    if (removed > 0) {
      canonicalEventsBySession.set(sessionId, nextEvents);
      setPersistedSessionEvents("canonicalEventsBySession", sessionId, nextEvents, MAX_CANONICAL_EVENTS_PER_SESSION);
      persistStateSoon("weak typed final prune");
    }
    return removed;
  }

  function listCanonicalSessionEvents(sessionId: string): TimelineEvent[] {
    return filterCanonicalSessionEventsForExposure(sessionId, dedupeTimelineEvents([
      ...(canonicalEventsBySession.get(sessionId) ?? []),
      ...(syntheticEventsBySession.get(sessionId) ?? [])
    ]))
      .map(normalizeTimelineEventSegmentType)
      .map(normalizeTimelineEventMessageSeq);
  }

  function listCanonicalLedgerEvents(sessionId: string, afterSeq = 0): OpenClawLedgerEvent[] {
    const ledgerEvents = extractOpenClawLedgerEvents(listCanonicalSessionEvents(sessionId));
    const dedupedEvents = dedupeCanonicalLedgerEventsForExposure(ledgerEvents);
    if (dedupedEvents.length !== ledgerEvents.length) {
      traceOpenClawLedger(input.logger, "canonical-dedupe", {
        sessionId,
        before: ledgerEvents.length,
        after: dedupedEvents.length,
        removed: ledgerEvents.length - dedupedEvents.length
      }, input.config);
    }
    return dedupedEvents
      .filter((event) => event.seq > afterSeq)
      .sort((left, right) => left.seq - right.seq);
  }

  function listCanonicalLedgerEventsForMessagePage(
    sessionId: string,
    messages: SessionMessage[],
    afterSeq = 0,
    pagination?: RPCPagination
  ): OpenClawLedgerEvent[] {
    const events = listCanonicalLedgerEvents(sessionId, afterSeq);
    const messageSeqs = messages.map(readSessionMessageSeq).filter((seq) => seq > 0);
    if (messageSeqs.length === 0) {
      if (!pagination || pagination.offset <= 0) {
        return events.slice(-MAX_CANONICAL_EVENTS_PER_MESSAGE_PAGE);
      }
      return [];
    }

    const messageSeqSet = new Set(messageSeqs);
    const matchedLedgerEvents = events.filter((event) => {
        const payload = toRecord(event.payload);
        const rawSeq = firstPositiveNumber(payload.rawSeq, payload.messageSeq, payload.message_seq);
        if (rawSeq > 0 && messageSeqSet.has(rawSeq)) {
          return true;
        }
        return messageSeqSet.has(event.seq);
      });
    const matchedLedgerSeqs = matchedLedgerEvents.map((event) => event.seq);

    if (matchedLedgerSeqs.length === 0) {
      return pagination && pagination.offset > 0 ? [] : events.slice(-MAX_CANONICAL_EVENTS_PER_MESSAGE_PAGE);
    }

    const minSeq = Math.min(...matchedLedgerSeqs) - CANONICAL_MESSAGE_PAGE_SEQ_WINDOW_BEFORE;
    const maxSeq = Math.max(...matchedLedgerSeqs) + CANONICAL_MESSAGE_PAGE_SEQ_WINDOW_AFTER;
    const matchedTurnIds = new Set(matchedLedgerEvents.map((event) => event.turn_id).filter(Boolean));
    const matchedActiveRequestIds = new Set(matchedLedgerEvents.map((event) => event.active_request_id).filter(Boolean));
    const scoped = events.filter((event) =>
      (event.seq >= minSeq && event.seq <= maxSeq) ||
      matchedTurnIds.has(event.turn_id) ||
      matchedActiveRequestIds.has(event.active_request_id)
    );
    return scoped.length > MAX_CANONICAL_EVENTS_PER_MESSAGE_PAGE
      ? scoped.slice(-MAX_CANONICAL_EVENTS_PER_MESSAGE_PAGE)
      : scoped;
  }

  function listCanonicalTimelineEventsForLedgerEvents(sessionId: string, ledgerEvents: OpenClawLedgerEvent[]): TimelineEvent[] {
    if (ledgerEvents.length === 0) {
      return [];
    }

    const ledgerSeqs = new Set(ledgerEvents.map((event) => event.seq).filter((seq) => seq > 0));
    if (ledgerSeqs.size === 0) {
      return [];
    }

    return listCanonicalSessionEvents(sessionId).filter((event) => {
      const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
      return Boolean(ledger && ledgerSeqs.has(ledger.seq));
    });
  }

  function dedupeCanonicalLedgerEventsForExposure(events: OpenClawLedgerEvent[]): OpenClawLedgerEvent[] {
    if (events.length <= 1) {
      return events;
    }

    const liveCompletedRunIds = collectOpenClawLiveCompletedRunIdsFromLedger(events);
    const scopedEvents = events.filter((event) => {
      if (!isOpenClawLedgerRunIdentityConsistent(event)) {
        return false;
      }
      if (event.run_id && liveCompletedRunIds.has(event.run_id) && isOpenClawHistoryLedgerEvent(event)) {
        return false;
      }
      return true;
    });
    const foldedEvents = foldSplitOpenClawAnswerEventsForExposure(scopedEvents);
    const selectedAnswers = new Map<string, OpenClawLedgerEvent>();
    const selectedOutputFiles = new Map<string, OpenClawLedgerEvent>();
    const duplicateAnswerRefs = new Set<string>();
    for (const event of [...foldedEvents].sort((left, right) => left.seq - right.seq)) {
      const answerIdentity = getOpenClawLedgerAnswerSemanticIdentity(event);
      if (answerIdentity) {
        const previous = selectedAnswers.get(answerIdentity);
        if (!previous) {
          selectedAnswers.set(answerIdentity, event);
        } else {
          const preferred = shouldPreferOpenClawLedgerAnswerEvent(event, previous) ? event : previous;
          const dropped = preferred === event ? previous : event;
          duplicateAnswerRefs.add(getOpenClawLedgerEventObjectRef(dropped));
          selectedAnswers.set(answerIdentity, preferred);
        }
      }

      const outputFileIdentity = getOpenClawLedgerOutputFileSemanticIdentity(event);
      if (outputFileIdentity) {
        const previous = selectedOutputFiles.get(outputFileIdentity);
        if (!previous) {
          selectedOutputFiles.set(outputFileIdentity, event);
        } else {
          const preferred = event.seq >= previous.seq ? event : previous;
          const dropped = preferred === event ? previous : event;
          duplicateAnswerRefs.add(getOpenClawLedgerEventObjectRef(dropped));
          selectedOutputFiles.set(outputFileIdentity, preferred);
        }
      }
    }

    if (duplicateAnswerRefs.size === 0 && foldedEvents.length === events.length && foldedEvents.every((event, index) => event === events[index])) {
      return events;
    }
    return foldedEvents.filter((event) => !duplicateAnswerRefs.has(getOpenClawLedgerEventObjectRef(event)));
  }

  function getOpenClawLedgerEventObjectRef(event: OpenClawLedgerEvent): string {
    return event.raw_event_ref || `${event.session_id}:${event.seq}:${event.turn_id}:${event.part_id}:${event.event_type}`;
  }

  function foldSplitOpenClawAnswerEventsForExposure(events: OpenClawLedgerEvent[]): OpenClawLedgerEvent[] {
    const splitAnswers = events.some((event) => event.part_type === "answer" && getOpenClawLedgerAnswerPartIndex(event.part_id) > 0);
    if (!splitAnswers) {
      return events;
    }

    const answerStateByRun = new Map<string, Map<number, string>>();
    const replacements = new Map<string, OpenClawLedgerEvent>();
    for (const event of [...events].sort((left, right) => left.seq - right.seq)) {
      if (event.part_type !== "answer") {
        continue;
      }
      const runIdentity = getOpenClawLedgerRunIdentity(event);
      if (!runIdentity) {
        continue;
      }
      const answerIndex = getOpenClawLedgerAnswerPartIndex(event.part_id);
      const text = readOpenClawLedgerAnswerText(event);
      const stateKey = `${event.session_id}:run:${runIdentity}`;
      const segmentTextByIndex = answerStateByRun.get(stateKey) ?? new Map<number, string>();
      answerStateByRun.set(stateKey, segmentTextByIndex);

      const priorText = joinOpenClawAnswerSegments(segmentTextByIndex, answerIndex - 1);
      const nextSegmentText =
        answerIndex > 0 && priorText && text.startsWith(priorText)
          ? text.slice(priorText.length).replace(/^\s+/, "")
          : text;
      const existingText = segmentTextByIndex.get(answerIndex) || "";
      segmentTextByIndex.set(answerIndex, event.operation === "append" ? `${existingText}${nextSegmentText}` : nextSegmentText);

      if (answerIndex <= 0) {
        continue;
      }

      const foldedText = joinOpenClawAnswerSegments(segmentTextByIndex, answerIndex);
      replacements.set(
        getOpenClawLedgerEventObjectRef(event),
        rewriteOpenClawLedgerAnswerEventForExposure(event, `${event.turn_id}:answer:0`, foldedText)
      );
    }

    if (replacements.size === 0) {
      return events;
    }
    return events.map((event) => replacements.get(getOpenClawLedgerEventObjectRef(event)) ?? event);
  }

  function rewriteOpenClawLedgerAnswerEventForExposure(
    event: OpenClawLedgerEvent,
    partId: string,
    text: string
  ): OpenClawLedgerEvent {
    const payload = toRecord(event.payload);
    const payloadWithText =
      typeof payload.content === "string"
        ? {
            ...payload,
            content: text,
            openclaw_folded_answer_segments: true,
            openclaw_original_part_id: event.part_id
          }
        : payload;
    return {
      ...event,
      part_id: partId,
      event_type: "part.replace",
      operation: "replace",
      text,
      ...(Object.keys(payloadWithText).length > 0 ? { payload: payloadWithText } : {})
    };
  }

  function readOpenClawLedgerAnswerText(event: OpenClawLedgerEvent): string {
    if (typeof event.text === "string") {
      return event.text;
    }
    const payload = toRecord(event.payload);
    return typeof payload.content === "string" ? payload.content : "";
  }

  function joinOpenClawAnswerSegments(segmentTextByIndex: Map<number, string>, maxIndex: number): string {
    if (maxIndex < 0) {
      return "";
    }
    const texts: string[] = [];
    for (let index = 0; index <= maxIndex; index += 1) {
      const text = segmentTextByIndex.get(index);
      if (text) {
        texts.push(text);
      }
    }
    return texts.join("");
  }

  function getOpenClawLedgerAnswerSemanticIdentity(event: OpenClawLedgerEvent): string {
    if (event.part_type !== "answer") {
      return "";
    }
    if (event.operation === "append") {
      return "";
    }
    const runIdentity = getOpenClawLedgerRunIdentity(event);
    if (!runIdentity) {
      return "";
    }
    return `${event.session_id}:run:${runIdentity}:answer`;
  }

  function getOpenClawLedgerOutputFileSemanticIdentity(event: OpenClawLedgerEvent): string {
    if (event.part_type !== "output_file") {
      return "";
    }
    const runIdentity = event.run_id || extractOpenClawHistoryIdentity(event.turn_id) || extractOpenClawHistoryIdentity(event.part_id);
    if (!runIdentity) {
      return "";
    }
    const files = extractOutputFilesFromPayload(event.payload);
    const fileKey = files
      .map((file) => getOutputFilePartIdentityKey(file))
      .filter(Boolean)
      .sort()
      .join(",");
    return `${event.session_id}:run:${runIdentity}:output_file:${fileKey || getOpenClawLedgerPartSemanticIndex(event.part_id, "output_file")}`;
  }

  function isOpenClawLedgerRunIdentityConsistent(event: OpenClawLedgerEvent): boolean {
    if (!event.run_id) {
      return true;
    }
    const historyIdentity = extractOpenClawHistoryIdentity(event.active_request_id) || extractOpenClawHistoryIdentity(event.turn_id);
    return !historyIdentity || historyIdentity === event.run_id;
  }

  function collectOpenClawLiveCompletedRunIds(events: TimelineEvent[]): Set<string> {
    return collectOpenClawLiveCompletedRunIdsFromLedger(extractOpenClawLedgerEvents(events));
  }

  function collectOpenClawLiveCompletedRunIdsFromLedger(events: OpenClawLedgerEvent[]): Set<string> {
    const runIds = new Set<string>();
    for (const event of events) {
      if (!event.run_id || isOpenClawHistoryLedgerEvent(event)) {
        continue;
      }
      if (event.terminal_status && event.terminal_status !== "running") {
        runIds.add(event.run_id);
      }
    }
    return runIds;
  }

  function extractOpenClawHistoryIdentity(value?: string): string {
    if (!value) {
      return "";
    }
    const match = value.match(/(?:^|:)history:([^:]+)/);
    return match?.[1] || "";
  }

  function getOpenClawLedgerRunIdentity(event: OpenClawLedgerEvent): string {
    return event.run_id || extractOpenClawHistoryIdentity(event.turn_id) || extractOpenClawHistoryIdentity(event.part_id);
  }

  function getOpenClawLedgerAnswerPartIndex(partId: string): number {
    const parsed = Number(getOpenClawLedgerPartSemanticIndex(partId, "answer"));
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function getOpenClawLedgerPartSemanticIndex(partId: string, partType: OpenClawLedgerPartType): string {
    const marker = `:${partType}:`;
    const markerIndex = partId.lastIndexOf(marker);
    if (markerIndex < 0) {
      return "0";
    }
    const suffix = partId.slice(markerIndex + marker.length);
    const index = suffix.split(":")[0]?.trim();
    return index || "0";
  }

  function shouldPreferOpenClawLedgerAnswerEvent(
    incoming: OpenClawLedgerEvent,
    previous: OpenClawLedgerEvent
  ): boolean {
    const incomingScore = scoreOpenClawLedgerAnswerEvent(incoming);
    const previousScore = scoreOpenClawLedgerAnswerEvent(previous);
    if (incomingScore !== previousScore) {
      return incomingScore > previousScore;
    }
    return incoming.seq >= previous.seq;
  }

  function scoreOpenClawLedgerAnswerEvent(event: OpenClawLedgerEvent): number {
    let score = 0;
    if (event.visibility === "final") score += 200;
    if (event.operation === "replace") score += 80;
    if (event.event_type === "part.replace") score += 40;
    if (readStringMetadata(event.payload, "source_kind") === "typed_transcript.final_replace") score += 80;
    if (readStringMetadata(event.payload, "source_kind") === "assistant.message") score += 30;
    if (!isOpenClawHistoryLedgerEvent(event)) score += 20;
    if (String(event.text ?? "").trim()) score += 5;
    return score;
  }

  function isOpenClawHistoryLedgerEvent(event: OpenClawLedgerEvent): boolean {
    return event.active_request_id.startsWith("history:") || event.turn_id.includes(":history:");
  }

  function dedupeTimelineEvents(events: TimelineEvent[]): TimelineEvent[] {
    const byKey = new Map<string, TimelineEvent>();
    for (const event of events) {
      const key = timelineEventDedupeKey(event);
      const previous = byKey.get(key);
      byKey.set(key, previous ? mergeTimelineEvent(previous, event) : event);
    }
    return dedupeAssistantAnswerSnapshots(
      dedupeAssistantMessageEchoes([...byKey.values()].sort((left, right) => left.seq - right.seq))
    );
  }

  function timelineEventDedupeKey(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const toolCallId = stringOr(
      data.toolCallId,
      data.tool_call_id,
      data.callId,
      data.call_id,
      payload.toolCallId,
      payload.tool_call_id,
      payload.callId,
      payload.call_id
    );
    if ((event.kind === "tool.call" || event.kind === "tool.result") && toolCallId) {
      return `${event.sessionId}:${event.kind}:${toolCallId}`;
    }
    return event.id || `${event.sessionId}:${event.seq}:${event.kind}`;
  }

  function mergeTimelineEvent(previous: TimelineEvent, incoming: TimelineEvent): TimelineEvent {
    return {
      ...previous,
      payload: mergeTimelinePayload(previous.payload, incoming.payload)
    };
  }

  function mergeTimelinePayload(previous: Record<string, unknown>, incoming: Record<string, unknown>): Record<string, unknown> {
    const previousData = toRecord(previous.data);
    const incomingData = toRecord(incoming.data);
    return {
      ...previous,
      ...incoming,
      ...(Object.keys(previousData).length > 0 || Object.keys(incomingData).length > 0
        ? {
            data: {
              ...previousData,
              ...incomingData
            }
          }
        : {})
    };
  }

  function dedupeAssistantMessageEchoes(events: TimelineEvent[]): TimelineEvent[] {
    const output: TimelineEvent[] = [];
    const assistantMessageByContent = new Map<string, number>();
    for (const event of events) {
      if (event.kind === "user.message" || event.kind === "run.started") {
        assistantMessageByContent.clear();
        output.push(event);
        continue;
      }
      if (event.kind !== "assistant.message") {
        output.push(event);
        continue;
      }

      const contentKey = normalizeAssistantDedupeContent(String(event.payload?.content ?? ""));
      if (!contentKey) {
        output.push(event);
        continue;
      }

      const previousIndex = assistantMessageByContent.get(contentKey);
      if (previousIndex === undefined) {
        assistantMessageByContent.set(contentKey, output.length);
        output.push(event);
        continue;
      }

      const previous = output[previousIndex];
      if (shouldPreferAssistantMessageEvent(event, previous)) {
        output[previousIndex] = {
          ...event,
          payload: mergeTimelinePayload(previous.payload, event.payload)
        };
      }
    }
    return output.sort((left, right) => left.seq - right.seq);
  }

  function dedupeAssistantAnswerSnapshots(events: TimelineEvent[]): TimelineEvent[] {
    const output: TimelineEvent[] = [];
    const candidateIndexes: number[] = [];

    for (const event of events) {
      if (event.kind === "user.message" || event.kind === "run.started") {
        candidateIndexes.length = 0;
        output.push(event);
        continue;
      }

      if (!isAssistantAnswerSnapshotEvent(event)) {
        output.push(event);
        continue;
      }

      const replacementIndex = findAssistantAnswerReplacementIndex(output, candidateIndexes, event);
      if (replacementIndex >= 0) {
        const previous = output[replacementIndex]!;
        if (shouldPreferAssistantAnswerSnapshot(event, previous)) {
          output[replacementIndex] = {
            ...event,
            payload: mergeTimelinePayload(previous.payload, event.payload)
          };
        }
        continue;
      }

      candidateIndexes.push(output.length);
      output.push(event);
    }

    return output.sort((left, right) => left.seq - right.seq);
  }

  function isAssistantAnswerSnapshotEvent(event: TimelineEvent): boolean {
    if (event.kind !== "assistant.message" && event.kind !== "assistant.delta") {
      return false;
    }
    return normalizeAssistantDedupeContent(String(event.payload?.content ?? "")) !== "";
  }

  function findAssistantAnswerReplacementIndex(
    output: TimelineEvent[],
    candidateIndexes: number[],
    incoming: TimelineEvent
  ): number {
    for (let position = candidateIndexes.length - 1; position >= 0; position -= 1) {
      const index = candidateIndexes[position]!;
      const previous = output[index];
      if (previous && canCollapseAssistantAnswerSnapshot(previous, incoming)) {
        return index;
      }
    }
    return -1;
  }

  function canCollapseAssistantAnswerSnapshot(previous: TimelineEvent, incoming: TimelineEvent): boolean {
    if (previous.sessionId !== incoming.sessionId) {
      return false;
    }

    const previousSegmentId = readAssistantAnswerSegmentId(previous);
    const incomingSegmentId = readAssistantAnswerSegmentId(incoming);
    if (previousSegmentId || incomingSegmentId) {
      return previousSegmentId === incomingSegmentId;
    }

    const previousRunId = getAssistantAnswerRunIdentity(previous);
    const incomingRunId = getAssistantAnswerRunIdentity(incoming);
    if (previousRunId && incomingRunId && previousRunId === incomingRunId) {
      const previousRawSeq = readAssistantAnswerRawSeqIdentity(previous);
      const incomingRawSeq = readAssistantAnswerRawSeqIdentity(incoming);
      return previousRawSeq > 0 && previousRawSeq === incomingRawSeq;
    }

    if (isBareSessionAssistantMessageSnapshot(previous) && isAuthoritativeChatAnswerSnapshot(incoming)) {
      const distance = Math.abs(Number(incoming.seq || 0) - Number(previous.seq || 0));
      return distance > 0 && distance <= 20;
    }

    return false;
  }

  function readAssistantAnswerSegmentId(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    return stringOr(timeline.segment_id, payload.segment_id, ledger?.part_type === "answer" ? ledger.part_id : undefined);
  }

  function getAssistantAnswerRunIdentity(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    return stringOr(payload.runId, payload.run_id, ledger?.run_id);
  }

  function readAssistantAnswerRawSeqIdentity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    const ledgerPayload = toRecord(ledger?.payload);
    return firstPositiveNumber(payload.rawSeq, payload.raw_seq, ledgerPayload.rawSeq, ledgerPayload.raw_seq);
  }

  function shouldPreferAssistantAnswerSnapshot(incoming: TimelineEvent, previous: TimelineEvent): boolean {
    return assistantAnswerSnapshotSpecificity(incoming) >= assistantAnswerSnapshotSpecificity(previous);
  }

  function isBareSessionAssistantMessageSnapshot(event: TimelineEvent): boolean {
    if (event.kind !== "assistant.message") {
      return false;
    }
    const payload = toRecord(event.payload);
    return !readStringMetadata(payload, "runId") && payload.rawSeq === undefined && !readAssistantAnswerSegmentId(event);
  }

  function isAuthoritativeChatAnswerSnapshot(event: TimelineEvent): boolean {
    const payload = toRecord(event.payload);
    if (!readStringMetadata(payload, "runId")) {
      return false;
    }
    return event.kind === "assistant.message" || payload.replace === true || readStringMetadata(payload, "mode") === "replace";
  }

  function assistantAnswerSnapshotSpecificity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    let score = assistantMessageSpecificity(event);
    if (event.kind === "assistant.message") score += 8;
    if (event.kind === "assistant.delta") score += 2;
    if (payload.replace === true) score += 2;
    if (payload.final === true) score += 2;
    if (readStringMetadata(payload, "visibility") === "final") score += 2;
    return score;
  }

  function shouldPreferAssistantMessageEvent(incoming: TimelineEvent, previous: TimelineEvent): boolean {
    return assistantMessageSpecificity(incoming) >= assistantMessageSpecificity(previous);
  }

  function assistantMessageSpecificity(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    const ledgerPayload = toRecord(ledger?.payload);
    const sourceKind = readStringMetadata(payload, "source_kind") || readStringMetadata(ledgerPayload, "source_kind");
    let score = 0;
    if (typeof payload.runId === "string" && payload.runId.trim()) score += 4;
    if (ledger?.run_id) score += 4;
    if (ledger && !isOpenClawHistoryLedgerEvent(ledger)) score += 3;
    if (payload.rawSeq !== undefined) score += 2;
    if (payload.state === "final") score += 1;
    if (payload.mode === "replace") score += 1;
    if (ledger?.visibility === "final") score += 1;
    if (ledger?.operation === "replace") score += 1;
    if (sourceKind === "typed_transcript.final_replace") score += 20;
    if (sourceKind === "typed_transcript.live_replace") score += 15;
    if (readBooleanMetadata(payload, "typed_final") === true || readBooleanMetadata(ledgerPayload, "typed_final") === true) score += 20;
    if (readBooleanMetadata(payload, "typed_live") === true || readBooleanMetadata(ledgerPayload, "typed_live") === true) score += 15;
    if (readBooleanMetadata(payload, "trusted_answer") === true || readBooleanMetadata(ledgerPayload, "trusted_answer") === true) score += 10;
    return score;
  }

  function normalizeAssistantDedupeContent(content: string): string {
    return content.replace(/\s+/g, " ").trim();
  }

  async function resolveRuntimeRPC(payload: unknown): Promise<unknown> {
    const include = stringOr(toRecord(payload).include, "all").toLowerCase();
    if (include === "status") {
      return input.rpcContext?.getStatusSnapshot ? await input.rpcContext.getStatusSnapshot() : await buildFallbackStatus();
    }
    if (include === "config") {
      return input.rpcContext?.getConfigSnapshot ? await input.rpcContext.getConfigSnapshot() : buildFallbackConfig();
    }
    if (include === "skills") {
      return buildSkillsPayload(await input.gateway.getRuntimeInfo(), ensuredRuntimeSkillsByKey);
    }

    const runtimeInfo = await input.gateway.getRuntimeInfo();
    return {
      status: input.rpcContext?.getStatusSnapshot ? await input.rpcContext.getStatusSnapshot() : await buildFallbackStatus(),
      config: input.rpcContext?.getConfigSnapshot ? await input.rpcContext.getConfigSnapshot() : buildFallbackConfig(),
      skills: buildSkillsPayload(runtimeInfo, ensuredRuntimeSkillsByKey),
      cronTasks: runtimeInfo.cronTasks ?? []
    };
  }

  function rememberEnsuredRuntimeSkill(request: EnsureHubSkillRequest, result: EnsureHubSkillResult): void {
    if (!result.ok || (result.status !== "installed" && result.status !== "up_to_date")) {
      return;
    }
    const skill = buildEnsuredRuntimeSkill(request, result);
    const key = runtimeSkillIdentity(skill);
    if (key) {
      ensuredRuntimeSkillsByKey.set(key, skill);
    }
  }

  async function resolveCurrentSessionRPC(payload: unknown): Promise<GatewaySession | null> {
    const record = toRecord(payload);
    const userObject = toRecord(record.user);
    const userName = stringOr(record.userName, record.user_name, userObject.name, userObject.userName, userObject.username);
    const chatId = stringOr(
      record.chat_id,
      record.chatId,
      record.user,
      userObject.id,
      userObject.userId,
      record.user_id,
      record.userId
    );
    if (!chatId) {
      throw new HubRPCError("PARAM_ERROR", "chat_id or user is required");
    }

    return restoreLatestHubSession(chatId, userName);
  }

  async function getMappedSession(chatId: string): Promise<GatewaySession | null> {
    const mappedId = state.mappings[chatId];
    if (!mappedId) {
      return null;
    }
    try {
      const session = await input.gateway.getSession(mappedId);
      if (!isHubManagedSessionTitle(session.title, chatId)) {
        delete state.mappings[chatId];
        await persistState();
        input.logger?.warn?.(
          `Ignored stale 53AIHub session mapping for ${chatId}: mapped session ${mappedId} is "${session.title}"`
        );
        return null;
      }
      return session;
    } catch {
      delete state.mappings[chatId];
      await persistState();
      return null;
    }
  }

  async function restoreLatestHubSession(chatId: string, userName?: string): Promise<GatewaySession | null> {
    const page = await input.gateway.listSessionPage({
      limit: 100,
      offset: 0
    });
    const knownSessions = await listKnownSessions();
    const sessions = mergeKnownHubSessions(page.sessions, knownSessions)
      .filter((session) => isRestorableHubSession(session, chatId, userName))
      .sort((left, right) => toTime(right.updatedAt || right.createdAt) - toTime(left.updatedAt || left.createdAt));
    const session = sessions[0];
    if (!session) {
      return null;
    }

    return session;
  }

  async function mergeKnownHubSessionTitles<T extends { sessions: GatewaySession[] }>(page: T): Promise<T> {
    const knownSessions = await listKnownSessions();
    if (!knownSessions.length) {
      return page;
    }
    return {
      ...page,
      sessions: mergeKnownHubSessions(page.sessions, knownSessions)
    };
  }

  async function listKnownSessions(): Promise<GatewaySession[]> {
    try {
      return (await input.callbacks.listKnownSessions?.()) ?? [];
    } catch (error) {
      input.logger?.warn?.(
        `Failed to read known 53AIHub sessions: ${error instanceof Error ? error.message : String(error)}`
      );
      return [];
    }
  }

  async function buildFallbackStatus() {
    const [gatewayHealth, runtimeInfo] = await Promise.allSettled([input.gateway.getHealth(), input.gateway.getRuntimeInfo()]);
    return {
      hub53ai: getStatus(),
      gatewayHealth: gatewayHealth.status === "fulfilled" ? gatewayHealth.value : undefined,
      runtime: runtimeInfo.status === "fulfilled" ? runtimeInfo.value : undefined
    };
  }

  function buildFallbackConfig() {
    return {
      hub53ai: redactHubConfig(input.config)
    };
  }

  function sendRPCFrame(frame: Hub53AIOutgoingRPCFrame): boolean {
    return sendRaw(JSON.stringify(frame), true);
  }

  async function processIncomingMessage(message: Hub53AIIncomingMessage) {
    if (isHub53AIBusinessHeartbeatMessage(message)) {
      traceOpenClawLedger(input.logger, "business-heartbeat-ignored", {
        reqId: message.reqId,
        chatIdHash: hashTraceText(message.chatId),
        textHash: hashTraceText(message.text)
      }, input.config);
      return;
    }

    const accessResult = checkAccessPolicy(message);
    if (!accessResult.allowed) {
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ 访问被拒绝: ${accessResult.reason}`,
        status: "error",
        error: {
          code: "ACCESS_DENIED",
          message: accessResult.reason
        }
      });
      return;
    }

    let close: (() => void) | undefined;
    let sessionId = "";
    try {
      const [session, preparedMessage] = await Promise.all([
        resolveSession(message),
        prepareIncomingFilesWithPromptFallback(message)
      ]);
      const [messageAttachments, outputManifestPath] = await Promise.all([
        buildGatewayMessageAttachmentsWithPromptFallback(preparedMessage),
        Promise.resolve(resolveLocalOutputManifestPath({
          stateDir: input.stateDir,
          conversationId: session.id
        })).then(async (path) => {
          if (path) {
            await mkdir(dirname(path), { recursive: true });
          }
          return path;
        })
      ]);
      const requestIdentity = message.clientMessageId || message.reqId;
      const turnId = buildOpenClawTimelineTurnId(session.id, requestIdentity);
      const workspaceDirs = resolveLocalOutputWorkspaceDirs({
        config: input.config,
        configPath: input.configPath,
        stateDir: input.stateDir
      });
      const prompt = buildPrompt(preparedMessage, {
        includeRuntimeContext: true,
        outputManifest:
          outputManifestPath
            ? { path: outputManifestPath, conversationId: session.id, turnId, activeRequestId: requestIdentity, workspaceDirs }
            : undefined
      });
      const displayContent = buildDisplayUserContent(preparedMessage);
      const userMessageMetadata = buildDisplayUserMessageMetadata(preparedMessage);
      sessionId = session.id;

      const [localOutputSnapshot] = await Promise.all([
        input.config.detectCreatedFiles === true
          ? snapshotLocalOutputFiles({
              config: input.config,
              configPath: input.configPath,
              stateDir: input.stateDir,
              logger: input.logger
            })
          : Promise.resolve(undefined),
        input.callbacks.onEnsureSessionStream(session.id)
      ]);

      await Promise.all([
        input.callbacks.onUserMessage({
          id: `hub53ai-user-${message.msgId}`,
          sessionId: session.id,
          role: "user",
          content: displayContent,
          createdAt: new Date().toISOString(),
          ...(Object.keys(userMessageMetadata).length ? { metadata: userMessageMetadata } : {})
        }),
        input.callbacks.onSessionStatus(session.id, "running")
      ]);

      const eventScope: GatewayEventScope = {
        eventBoundaryMs: Date.now(),
        turnId,
        activeRequestId: requestIdentity,
        nextSegmentIndex: 0,
        nextAnswerSegmentIndex: 0,
        answerBoundaryAfterVisibleResponse: false,
        activityAppliedEventKeys: new Set<string>(),
        answerContentAppliedEventKeys: new Set<string>(),
        answerSegmentTextById: new Map<string, string>(),
        answerSegmentVisibilityById: new Map<string, OpenClawTimelineV2Visibility>(),
        answerSegmentTrustedById: new Map<string, boolean>(),
        nextDeltaIndexBySegment: new Map<string, number>(),
        segmentIndexById: new Map<string, number>(),
        timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
        typedToolCallEnrichedKeys: new Set<string>(),
        currentActivitySeen: false,
        visibleResponseSeen: false,
        lastSeqSeen: 0,
        emittedOutputFileKeys: new Set<string>(),
        outputManifestPath,
        referencedLocalOutputPaths: new Set<string>(),
        writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>(),
        localOutputSnapshot,
        hubUserId: preparedMessage.userId
      };
      trackActiveSessionRequest(sessionId, message.reqId, { message: preparedMessage, eventScope });
      const terminalPromise = waitForTerminalEvent(message.reqId);
      close = input.gateway.subscribe(session.id, input.callbacks.getLastEventSeq(session.id), {
        onEvent: (event) => {
          if (event.kind === "assistant.delta") {
            eventScope.deltaHandlingQueue = (eventScope.deltaHandlingQueue ?? Promise.resolve())
              .catch(() => undefined)
              .then(() => handleGatewayEvent(message, event, sessionId, eventScope))
              .catch((error) => {
                traceOpenClawLedger(input.logger, "event-delta-error", {
                  reqId: message.reqId,
                  sessionId,
                  error: error instanceof Error ? error.message : String(error),
                  event: summarizeTimelineEventForTrace(event)
                }, input.config);
              });
            return;
          }
          eventScope.eventHandlingQueue = (eventScope.eventHandlingQueue ?? Promise.resolve())
            .catch(() => undefined)
            .then(() => handleGatewayEvent(message, event, sessionId, eventScope))
            .catch((error) => {
              traceOpenClawLedger(input.logger, "event-queue-error", {
                reqId: message.reqId,
                sessionId,
                error: error instanceof Error ? error.message : String(error),
                event: summarizeTimelineEventForTrace(event)
              }, input.config);
            });
        },
        onDisconnect: (error) => {
          const messageText = error instanceof Error ? error.message : "gateway stream disconnected";
          void sendReply({
            reqId: message.reqId,
            text: `⚠️ ${messageText}`,
            status: "error",
            sessionId,
            error: {
              code: "WEBSOCKET_ERROR",
              message: messageText
            }
          }).finally(() => {
            resolveTerminalEvent(message.reqId);
          });
        }
      });

      await sendGatewayMessageWithAttachmentFallback(session.id, prompt, messageAttachments);
      await terminalPromise;
    } catch (error) {
      const messageText = error instanceof Error ? error.message : String(error);
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ ${messageText}`,
        status: "error",
        sessionId,
        error: {
          code: inferErrorCode(messageText),
          message: messageText
        }
      });
    } finally {
      close?.();
      if (sessionId) {
        untrackActiveSessionRequest(sessionId, message.reqId);
      }
      clearTerminalResolver(message.reqId);
      lastReplyByReq.delete(message.reqId);
    }
  }

  async function prepareIncomingFilesWithPromptFallback(message: Hub53AIIncomingMessage): Promise<Hub53AIIncomingMessage> {
    try {
      return await prepareIncomingFiles(message);
    } catch (error) {
      input.logger?.warn?.(
        `[53aihub] failed to prepare input files; continuing with runtime prompt context fallback: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return message;
    }
  }

  async function prepareIncomingFiles(message: Hub53AIIncomingMessage): Promise<Hub53AIIncomingMessage> {
    if (!message.files?.length) {
      return message;
    }
    const safeReqId = sanitizePathSegment(message.reqId || message.msgId || randomUUID());
    const inputDir = join(input.stateDir, "input-files", safeReqId);
    await mkdir(inputDir, { recursive: true });
    const preparedFiles: Hub53AIInputFile[] = [];
    const preparedFileUrls = new Set(message.fileUrls ?? []);

    for (const [index, file] of message.files.entries()) {
      const fileURL = stringOr(file.signed_download_url, file.download_url, file.preview_url, file.url);
      if (!fileURL) {
        preparedFiles.push(file);
        continue;
      }
      try {
        const localPath = await downloadIncomingFile(fileURL, file, inputDir, index);
        preparedFiles.push({ ...file, local_path: localPath });
        preparedFileUrls.add(localPath);
      } catch (error) {
        input.logger?.warn?.(
          `[53aihub] failed to download input file ${fileURL}: ${error instanceof Error ? error.message : String(error)}`
        );
        preparedFiles.push(file);
        preparedFileUrls.add(fileURL);
      }
    }

    return {
      ...message,
      files: preparedFiles,
      fileUrls: [...preparedFileUrls]
    };
  }

  async function downloadIncomingFile(
    rawURL: string,
    file: Hub53AIInputFile,
    inputDir: string,
    index: number
  ): Promise<string> {
    const url = resolveHubHTTPURL(rawURL);
    const headers = isHubOriginURL(url) ? buildHubAuthHeaders() : {};
    const response = await fetch(url.toString(), { headers });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const bytes = Buffer.from(await response.arrayBuffer());
    const fallbackExt = extname(new URL(url).pathname) || extname(stringOr(file.name, file.file_name, file.filename));
    const fileName = sanitizeDownloadFileName(
      stringOr(file.file_name, file.filename, file.name) || `input-${index + 1}${fallbackExt || ""}`
    );
    const targetPath = join(inputDir, fileName);
    await writeFile(targetPath, bytes);
    return targetPath;
  }

  async function buildGatewayMessageAttachmentsWithPromptFallback(
    message: Hub53AIIncomingMessage
  ): Promise<GatewayMessageAttachment[]> {
    try {
      return await buildGatewayMessageAttachments(message);
    } catch (error) {
      input.logger?.warn?.(
        `[53aihub] failed to build native attachments; continuing with runtime prompt context fallback: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return [];
    }
  }

  async function buildGatewayMessageAttachments(message: Hub53AIIncomingMessage): Promise<GatewayMessageAttachment[]> {
    const attachments: GatewayMessageAttachment[] = [];
    for (const file of message.files ?? []) {
      const localPath = stringOr(file.local_path);
      if (!localPath) {
        continue;
      }
      try {
        const bytes = await readFile(localPath);
        attachments.push({
          type: "file",
          fileName: sanitizeDownloadFileName(stringOr(file.file_name, file.filename, file.name) || basename(localPath)),
          mimeType: stringOr(file.mime_type) || inferMimeTypeFromFileName(localPath),
          content: bytes.toString("base64")
        });
      } catch (error) {
        input.logger?.warn?.(
          `[53aihub] failed to prepare native attachment ${localPath}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
    return attachments;
  }

  async function sendGatewayMessageWithAttachmentFallback(
    sessionId: string,
    prompt: string,
    attachments: GatewayMessageAttachment[]
  ): Promise<void> {
    if (!attachments.length) {
      await input.gateway.sendMessage(sessionId, prompt);
      return;
    }

    try {
      await input.gateway.sendMessage(sessionId, prompt, { attachments });
    } catch (error) {
      if (!isNativeAttachmentSendError(error)) {
        throw error;
      }
      input.logger?.warn?.(
        `[53aihub] native attachment send failed; retrying with runtime prompt context fallback: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      await input.gateway.sendMessage(sessionId, prompt);
    }
  }

  function isNativeAttachmentSendError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error || "");
    return /attachment|attached|file|base64|payload|request entity too large|body too large|unsupported/i.test(message);
  }

  function resolveHubHTTPURL(rawURL: string): URL {
    const base = getHubHTTPBaseURL();
    try {
      return new URL(rawURL);
    } catch {
      return new URL(rawURL, base);
    }
  }

  function getHubHTTPBaseURL(): URL {
    const base = new URL(input.config.wsUrl);
    base.protocol = base.protocol === "wss:" ? "https:" : "http:";
    base.pathname = "/";
    base.search = "";
    base.hash = "";
    return base;
  }

  function isHubOriginURL(url: URL): boolean {
    return url.origin === getHubHTTPBaseURL().origin;
  }

  function buildHubAuthHeaders(): Record<string, string> {
    const authBase64 = Buffer.from(`${input.config.botId}:${input.config.secret}`).toString("base64");
    return {
      Authorization: `Bearer ${input.config.secret}`,
      "Proxy-Authorization": `Basic ${authBase64}`,
      "X-Bot-Id": input.config.botId,
      "X-Api-Key": input.config.secret
    };
  }

  function trackActiveSessionRequest(sessionId: string, reqId: string, details?: ActiveSessionRequest) {
    const reqIds = activeReqIdsBySession.get(sessionId) ?? new Set<string>();
    reqIds.add(reqId);
    activeReqIdsBySession.set(sessionId, reqIds);
    if (details) {
      const requests = activeRequestDetailsBySession.get(sessionId) ?? new Map<string, ActiveSessionRequest>();
      requests.set(reqId, details);
      activeRequestDetailsBySession.set(sessionId, requests);
    }
  }

  function untrackActiveSessionRequest(sessionId: string, reqId: string) {
    const reqIds = activeReqIdsBySession.get(sessionId);
    if (!reqIds) {
      return;
    }
    reqIds.delete(reqId);
    if (!reqIds.size) {
      activeReqIdsBySession.delete(sessionId);
    }
    const requests = activeRequestDetailsBySession.get(sessionId);
    if (!requests) {
      return;
    }
    requests.delete(reqId);
    if (!requests.size) {
      activeRequestDetailsBySession.delete(sessionId);
    }
  }

  async function resolveActiveSessionRequests(sessionId: string, reason: string) {
    const handledReqIds = new Set<string>();
    for (const [reqId, details] of activeRequestDetailsBySession.get(sessionId) ?? []) {
      handledReqIds.add(reqId);
      await emitSyntheticTerminalEvent(details.message, sessionId, details.eventScope, {
        kind: "run.interrupted",
        status: "interrupted",
        reason
      });
      lastReplyByReq.delete(reqId);
      resolveTerminalEvent(reqId);
    }
    for (const reqId of activeReqIdsBySession.get(sessionId) ?? []) {
      if (handledReqIds.has(reqId)) continue;
      lastReplyByReq.delete(reqId);
      resolveTerminalEvent(reqId);
    }
  }

  async function emitSyntheticTerminalEvent(
    message: Hub53AIIncomingMessage,
    sessionId: string,
    eventScope: GatewayEventScope,
    inputTerminal: {
      kind: "run.completed" | "run.failed" | "run.interrupted";
      status: Exclude<OpenClawLedgerTerminalStatus, "running">;
      reason: string;
    }
  ) {
    if (eventScope.terminalSeen) {
      return;
    }

    const seq = await getNextSyntheticEventSeq(sessionId, eventScope);
    const createdAt = new Date().toISOString();
    const event: TimelineEvent = {
      id: `synthetic:${inputTerminal.reason}:${message.reqId}:${seq}`,
      sessionId,
      seq,
      kind: inputTerminal.kind,
      payload: {
        synthetic: true,
        synthetic_terminal: true,
        synthetic_reason: inputTerminal.reason,
        active_request_id: eventScope.activeRequestId,
        req_id: message.reqId,
        ...(message.clientMessageId ? { client_message_id: message.clientMessageId } : {}),
        turn_id: eventScope.currentTurnId || eventScope.turnId,
        ...(eventScope.currentRunId ? { runId: eventScope.currentRunId, run_id: eventScope.currentRunId } : {}),
        terminal_status: inputTerminal.status
      },
      createdAt
    };
    const payload = augmentPayloadWithEventMeta(event, eventScope);
    const syntheticEvent = {
      ...event,
      payload
    };
    appendSyntheticSessionEvent(sessionId, syntheticEvent);
    eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, seq);
    eventScope.terminalSeen = true;
    const ledgerEvent = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(syntheticEvent));
    traceOpenClawLedger(input.logger, "synthetic-terminal", {
      reqId: message.reqId,
      sessionId,
      reason: inputTerminal.reason,
      event: summarizeTimelineEventForTrace(syntheticEvent),
      ...(ledgerEvent ? { ledger: summarizeOpenClawLedgerEvent(ledgerEvent) } : {})
    }, input.config);
    await sendReply({
      reqId: message.reqId,
      text: "",
      status: "done",
      sessionId,
      eventKind: inputTerminal.kind,
      payload
    });
  }

  async function getNextSyntheticEventSeq(sessionId: string, eventScope: GatewayEventScope): Promise<number> {
    const syntheticSeq = (syntheticEventsBySession.get(sessionId) ?? []).reduce(
      (maxSeq, event) => Math.max(maxSeq, event.seq || 0),
      0
    );
    let gatewaySeq = 0;
    try {
      const lastSeq = getLastBackfillGatewaySeq(sessionId);
      const events = await input.gateway.listEvents(sessionId, lastSeq);
      gatewaySeq = events.reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
      const maxGatewaySeq = events.reduce((max, e) => Math.max(max, (e as TimelineEvent & { seq?: number }).seq ?? 0), 0);
      if (maxGatewaySeq > lastSeq) {
        setLastBackfillGatewaySeq(sessionId, maxGatewaySeq);
      }
    } catch {
      gatewaySeq = 0;
    }
    return Math.max(eventScope.lastSeqSeen, syntheticSeq, gatewaySeq) + 1;
  }

  function appendSyntheticSessionEvent(sessionId: string, event: TimelineEvent) {
    if (!sessionId) {
      return;
    }
    const events = syntheticEventsBySession.get(sessionId) ?? [];
    if (events.some((candidate) => candidate.id === event.id)) {
      return;
    }
    const nextEvents = [...events, event].slice(-MAX_SYNTHETIC_EVENTS_PER_SESSION);
    syntheticEventsBySession.set(sessionId, nextEvents);
    setPersistedSessionEvents("syntheticEventsBySession", sessionId, nextEvents, MAX_SYNTHETIC_EVENTS_PER_SESSION);
    rememberOpenClawLedgerSeq(sessionId, event);
    persistStateSoon("synthetic ledger event");
  }

  function appendCanonicalSessionEvent(sessionId: string, event: TimelineEvent) {
    if (!sessionId) {
      return;
    }
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    if (ledger && shouldRejectCanonicalHistoryAnswerEvent(ledger, event)) {
      traceOpenClawLedger(input.logger, "history-answer-canonical-skip", {
        sessionId,
        reason: "synthetic_history_typed_transcript_answer",
        event: summarizeTimelineEventForTrace(event),
        ledger: summarizeOpenClawLedgerEvent(ledger)
      }, input.config);
      return;
    }
    const events = canonicalEventsBySession.get(sessionId) ?? [];
    const key = event.id || `${event.sessionId}:${event.seq}:${event.kind}`;
    const next = events.filter((candidate) => (candidate.id || `${candidate.sessionId}:${candidate.seq}:${candidate.kind}`) !== key);
    const nextEvents = [...next, event].slice(-MAX_CANONICAL_EVENTS_PER_SESSION);
    canonicalEventsBySession.set(sessionId, nextEvents);
    setPersistedSessionEvents("canonicalEventsBySession", sessionId, nextEvents, MAX_CANONICAL_EVENTS_PER_SESSION);
    rememberOpenClawLedgerSeq(sessionId, event);
    persistStateSoon("canonical ledger event");
  }

  function shouldRejectCanonicalHistoryAnswerEvent(ledger: OpenClawLedgerEvent, event: TimelineEvent): boolean {
    if (!isOpenClawHistoryLedgerEvent(ledger) || ledger.part_type !== "answer") {
      return false;
    }
    const payload = toRecord(event.payload);
    const sourceKind = readStringMetadata(payload, "source_kind");
    return sourceKind === "typed_transcript.live_replace" ||
      sourceKind === "typed_transcript.final_replace" ||
      readBooleanMetadata(payload, "typed_live") === true ||
      readBooleanMetadata(payload, "typed_final") === true;
  }

  function shouldExposeCanonicalSessionEvent(event: TimelineEvent): boolean {
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    return !(ledger && shouldRejectCanonicalHistoryAnswerEvent(ledger, event));
  }

  function filterCanonicalSessionEventsForExposure(sessionId: string, events: TimelineEvent[]): TimelineEvent[] {
    const nextEvents = events.filter(shouldExposeCanonicalSessionEvent);
    if (nextEvents.length !== events.length) {
      traceOpenClawLedger(input.logger, "history-answer-exposure-skip", {
        sessionId,
        before: events.length,
        after: nextEvents.length,
        removed: events.length - nextEvents.length
      }, input.config);
    }
    return nextEvents;
  }

  const terminalResolvers = new Map<
    string,
    {
      resolve: () => void;
      reject: (error: Error) => void;
      timer: NodeJS.Timeout;
    }
  >();
  function waitForTerminalEvent(reqId: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        terminalResolvers.delete(reqId);
        reject(new Error(`Timed out waiting for run completion (${reqId})`));
      }, RUN_WAIT_TIMEOUT_MS);
      terminalResolvers.set(reqId, { resolve, reject, timer });
    });
  }

  function resolveTerminalEvent(reqId: string) {
    const resolver = terminalResolvers.get(reqId);
    if (!resolver) {
      return;
    }
    clearTimeout(resolver.timer);
    resolver.resolve();
    terminalResolvers.delete(reqId);
  }

  function clearTerminalResolver(reqId: string) {
    const resolver = terminalResolvers.get(reqId);
    if (!resolver) {
      return;
    }
    clearTimeout(resolver.timer);
    terminalResolvers.delete(reqId);
  }

  type GatewayEventScope = {
    eventBoundaryMs: number;
    turnId: string;
    activeRequestId: string;
    currentTurnId?: string;
    currentRunId?: string;
    nextSegmentIndex: number;
    nextAnswerSegmentIndex: number;
    currentAnswerSegmentId?: string;
    answerBoundaryAfterVisibleResponse: boolean;
    currentAnswerRawOrder?: number;
    activityAppliedEventKeys: Set<string>;
    answerContentAppliedEventKeys: Set<string>;
    answerSegmentTextById: Map<string, string>;
    answerSegmentVisibilityById: Map<string, OpenClawTimelineV2Visibility>;
    answerSegmentTrustedById: Map<string, boolean>;
    nextDeltaIndexBySegment: Map<string, number>;
    segmentIndexById: Map<string, number>;
    timelineMetaByEventKey: Map<string, OpenClawTimelineV2Meta>;
    typedToolCallEnrichedKeys: Set<string>;
    currentActivitySeen: boolean;
    visibleResponseSeen: boolean;
    lastSeqSeen: number;
    terminalSeen?: boolean;
    emittedOutputFileKeys: Set<string>;
    outputManifestPath?: string;
    localOutputSnapshot?: LocalOutputFileSnapshot;
    hubUserId?: string;
    referencedLocalOutputPaths: Set<string>;
    writeOutputFilesByToolCallId: Map<string, Hub53AIOutputFile[]>;
    localOutputFilesSent?: boolean;
    typedLiveLastTextHash?: string;
    typedLiveLastResolvedAtMs?: number;
    typedLiveInFlight?: Promise<boolean>;
    eventHandlingQueue?: Promise<void>;
    deltaHandlingQueue?: Promise<void>;
  };

  type ActiveSessionRequest = {
    message: Hub53AIIncomingMessage;
    eventScope: GatewayEventScope;
  };

  function applyOpenClawEventScopeActivity(event: GatewayEvent, eventScope: GatewayEventScope) {
    const cacheKey = getTimelineEventCacheKey(event);
    if (eventScope.activityAppliedEventKeys.has(cacheKey)) {
      return;
    }
    eventScope.activityAppliedEventKeys.add(cacheKey);

    const isAnswerEvent = event.kind === "assistant.delta" || event.kind === "assistant.message";
    const isVisibleEvent = isVisibleOpenClawResponseEvent(event);
    if (isCurrentRunActivityEvent(event)) {
      if (!isAnswerEvent && eventScope.visibleResponseSeen && isVisibleEvent) {
        const eventRawOrder = getOpenClawRawOrder(event);
        if (!eventRawOrder || !eventScope.currentAnswerRawOrder || eventRawOrder >= eventScope.currentAnswerRawOrder) {
          eventScope.answerBoundaryAfterVisibleResponse = true;
        } else {
          traceOpenClawLedger(input.logger, "stale", {
            reason: "late_non_answer_activity_before_current_answer",
            event: summarizeTimelineEventForTrace(event),
            eventRawOrder,
            currentAnswerRawOrder: eventScope.currentAnswerRawOrder
          }, input.config);
        }
      }
      eventScope.currentActivitySeen = true;
    }
    if (isVisibleEvent) {
      eventScope.visibleResponseSeen = true;
    }
    rememberWriteToolOutputFileCandidate(event, eventScope);
  }

  async function handleGatewayEvent(
    message: Hub53AIIncomingMessage,
    event: GatewayEvent,
    sessionId: string,
    eventScope: GatewayEventScope
  ) {
    if (!(activeReqIdsBySession.get(sessionId)?.has(message.reqId) ?? true)) {
      traceOpenClawLedger(input.logger, "stale", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      traceOpenClawDuplicate(input.logger, "hub.event.skip_inactive_req", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return;
    }
    traceOpenClawLedger(input.logger, "raw", {
      reqId: message.reqId,
      sessionId,
      event: summarizeTimelineEventForTrace(event)
    }, input.config);
    if (isReplayFromPreviousRun(event, eventScope)) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_previous_run", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return;
    }
    if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
      eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, event.seq);
    }
    if (isTerminalRunEvent(event) && event.kind !== "run.failed") {
      eventScope.terminalSeen = true;
    }
    traceOpenClawDuplicate(input.logger, "hub.event.received", {
      reqId: message.reqId,
      sessionId,
      event: summarizeTimelineEventForTrace(event)
    }, input.config);
    applyOpenClawEventScopeActivity(event, eventScope);
    if (shouldAttachOpenClawTimeline(event)) {
      augmentPayloadWithEventMeta(event, eventScope);
    }
    if (event.kind !== "assistant.delta") {
      for (const path of extractReferencedLocalOutputPaths(event.payload, eventScope.localOutputSnapshot, {
        config: input.config,
        configPath: input.configPath,
        stateDir: input.stateDir,
        logger: input.logger
      })) {
        eventScope.referencedLocalOutputPaths.add(path);
      }
      await sendOutputFilesForEvent(message.reqId, sessionId, event, eventScope);
      await sendWriteToolOutputFilesForEvent(message.reqId, sessionId, event, eventScope);
    }

    if (event.kind === "assistant.delta" || event.kind === "assistant.message") {
      if (isUntrustedRawOpenClawAnswerEvent(event)) {
        if (event.kind === "assistant.message") {
          const replaced = await maybeApplyTypedTranscriptLiveReplace({
            sessionId,
            sourceEvent: event,
            eventScope,
            reqId: message.reqId
          });
          await sendCreatedLocalOutputFiles(message.reqId, sessionId, eventScope);
          if (replaced) {
            return;
          }
        }
      }
      const content = String(event.payload?.content ?? "");
      const replaceReply = isReplyReplaceEvent(event);
      const delta = extractReplyDelta(message.reqId, content, replaceReply);
      traceOpenClawDuplicate(input.logger, "hub.reply.delta_decision", {
        reqId: message.reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event),
        replaceReply,
        inputContentLength: content.length,
        outputDeltaLength: delta.length,
        outputDeltaHash: hashTraceText(delta)
      }, input.config);
      if (delta) {
        await sendReply({
          reqId: message.reqId,
          text: delta,
          status: "streaming",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind,
          payload: augmentPayloadWithEventMeta(event, eventScope)
        });
      }
      if (event.kind === "assistant.message") {
        await sendCreatedLocalOutputFiles(message.reqId, sessionId, eventScope);
      }
      return;
    }

    if (event.kind === "assistant.thinking") {
      const content = String(event.payload?.content ?? "");
      if (content.trim() && input.config.sendThinkingMessage) {
        await sendReply({
          reqId: message.reqId,
          text: content,
          status: "thinking",
          sessionId,
          mode: readStringMetadata(event.payload, "mode"),
          replace: readBooleanMetadata(event.payload, "replace"),
          eventKind: event.kind,
          payload: augmentPayloadWithEventMeta(event, eventScope)
        });
      }
      return;
    }

    if (event.kind === "status.update") {
      const derivedFailedEvent = buildFailedRunEventFromStatusUpdate(event, sessionId, message.reqId, eventScope);
      if (derivedFailedEvent) {
        await handleRunFailureEvent(message, sessionId, derivedFailedEvent, eventScope, {
          appendToSessionEvents: true,
          traceReason: "status.update.failed"
        });
      }
      return;
    }

    if (event.kind === "tool.call" || event.kind === "tool.result") {
      const enrichment = await enrichToolEventFromTypedHistory(sessionId, event, eventScope);
      if (enrichment.syntheticCallEvent && input.config.sendThinkingMessage) {
        applyOpenClawEventScopeActivity(enrichment.syntheticCallEvent, eventScope);
        const syntheticPayload = augmentPayloadWithEventMeta(enrichment.syntheticCallEvent, eventScope);
        enrichment.syntheticCallEvent.payload = syntheticPayload;
        const syntheticSummary = summarizeVisibleActivity(enrichment.syntheticCallEvent);
        if (syntheticSummary) {
          await sendReply({
            reqId: message.reqId,
            text: syntheticSummary,
            status: "thinking",
            sessionId,
            mode: "replace",
            replace: true,
            eventKind: enrichment.syntheticCallEvent.kind,
            payload: syntheticPayload
          });
        }
      }

      const visibleToolEvent = enrichment.event;
      const summary = summarizeVisibleActivity(visibleToolEvent);
      if (summary && input.config.sendThinkingMessage) {
        await sendReply({
          reqId: message.reqId,
          text: summary,
          status: "thinking",
          sessionId,
          mode: "append",
          replace: false,
          eventKind: visibleToolEvent.kind,
          payload: augmentPayloadWithEventMeta(visibleToolEvent, eventScope)
        });
      }
      return;
    }

    if (event.kind === "run.completed") {
      const typedFinalReplaced = await applyTypedTranscriptFinalReplace({
        sessionId,
        terminalEvent: event,
        eventScope,
        reqId: message.reqId,
        sendToHub: true
      });
      await sendCreatedLocalOutputFiles(message.reqId, sessionId, eventScope);
      const doneText = typedFinalReplaced ? "" : readCurrentOpenClawAnswerText(eventScope, message.reqId);
      await sendReply({
        reqId: message.reqId,
        text: doneText,
        status: "done",
        sessionId,
        eventKind: event.kind,
        payload: augmentPayloadWithEventMeta(event, eventScope)
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
      return;
    }

    if (event.kind === "run.failed" || event.kind === "run.interrupted") {
      if (event.kind === "run.failed") {
        await handleRunFailureEvent(message, sessionId, event, eventScope);
        return;
      }

      const errorText = String(event.payload?.error ?? event.payload?.message ?? event.kind);
      await sendReply({
        reqId: message.reqId,
        text: `⚠️ ${errorText}`,
        status: "error",
        sessionId,
        error: {
          code: inferErrorCode(errorText),
          message: errorText
        },
        eventKind: event.kind,
        payload: augmentPayloadWithEventMeta(event, eventScope)
      });
      lastReplyByReq.delete(message.reqId);
      resolveTerminalEvent(message.reqId);
    }
  }

  async function handleRunFailureEvent(
    message: Hub53AIIncomingMessage,
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    options?: {
      appendToSessionEvents?: boolean;
      traceReason?: string;
    }
  ) {
    if (eventScope.terminalSeen) {
      return;
    }

    const classification = classifyOpenClawRunFailure(event, eventScope);
    const payload = augmentPayloadWithEventMeta(enrichFailedRunEvent(event, message.reqId, eventScope, classification), eventScope);
    const failedEvent: TimelineEvent = {
      ...event,
      payload
    };
    if (options?.appendToSessionEvents) {
      appendSyntheticSessionEvent(sessionId, failedEvent);
    }
    eventScope.terminalSeen = true;
    if (typeof event.seq === "number" && Number.isFinite(event.seq)) {
      eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, event.seq);
    }
    const ledgerEvent = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(failedEvent));
    traceOpenClawLedger(input.logger, "synthetic-terminal", {
      reqId: message.reqId,
      sessionId,
      reason: options?.traceReason ?? "run.failed",
      classification: {
        code: classification.code,
        reason: classification.reason,
        provider: classification.provider,
        model: classification.model,
        runtimeMs: classification.runtimeMs,
        authRelated: classification.authRelated,
        confidence: classification.confidence
      },
      event: summarizeTimelineEventForTrace(failedEvent),
      ...(ledgerEvent ? { ledger: summarizeOpenClawLedgerEvent(ledgerEvent) } : {})
    }, input.config);
    await input.callbacks.onSessionStatus(sessionId, "failed");
    await sendReply({
      reqId: message.reqId,
      text: `⚠️ ${classification.userMessage}`,
      status: "error",
      sessionId,
      error: {
        code: classification.code,
        message: classification.userMessage,
        ...(classification.rawMessage ? { details: classification.rawMessage } : {})
      },
      eventKind: event.kind,
      payload
    });
    lastReplyByReq.delete(message.reqId);
    resolveTerminalEvent(message.reqId);
  }

  async function applyTypedTranscriptFinalReplace(inputReplace: {
    sessionId: string;
    terminalEvent: TimelineEvent;
    eventScope: GatewayEventScope;
    reqId: string;
    sendToHub: boolean;
  }): Promise<boolean> {
    if (inputReplace.terminalEvent.kind !== "run.completed") {
      return false;
    }

    const typedFinal = await resolveTypedTranscriptFinal(
      inputReplace.sessionId,
      inputReplace.terminalEvent,
      inputReplace.eventScope
    );
    if (!typedFinal) {
      traceOpenClawLedger(input.logger, "typed-final", {
        result: "miss",
        sessionId: inputReplace.sessionId,
        reqId: inputReplace.reqId,
        terminalEvent: summarizeTimelineEventForTrace(inputReplace.terminalEvent)
      }, input.config);
      return false;
    }

    const currentAnswer = readCurrentOpenClawAnswerText(inputReplace.eventScope, inputReplace.reqId);
    const typedFinalHash = hashTraceText(typedFinal.text);
    const shouldPromoteTypedLiveToFinal = Boolean(
      typedFinalHash && inputReplace.eventScope.typedLiveLastTextHash === typedFinalHash
    );
    const currentAnswerIsVisibleOrTrusted = isCurrentOpenClawAnswerVisibleOrTrusted(
      inputReplace.eventScope,
      inputReplace.reqId
    );
    if (
      currentAnswer.trim() === typedFinal.text.trim() &&
      !shouldPromoteTypedLiveToFinal &&
      currentAnswerIsVisibleOrTrusted
    ) {
      traceOpenClawLedger(input.logger, "typed-final", {
        result: "noop",
        sessionId: inputReplace.sessionId,
        reqId: inputReplace.reqId,
        matchStrategy: typedFinal.matchStrategy,
        textLength: typedFinal.text.length,
        textHash: typedFinalHash,
        segmentCount: typedFinal.segmentCount,
        currentAnswerIsVisibleOrTrusted
      }, input.config);
      return false;
    }

    const event = buildTypedTranscriptFinalEvent(
      inputReplace.sessionId,
      inputReplace.terminalEvent,
      inputReplace.eventScope,
      inputReplace.reqId,
      typedFinal,
      currentAnswer
    );
    applyOpenClawEventScopeActivity(event, inputReplace.eventScope);
    const payload = augmentPayloadWithEventMeta(event, inputReplace.eventScope);
    event.payload = payload;
    inputReplace.eventScope.lastSeqSeen = Math.max(inputReplace.eventScope.lastSeqSeen, event.seq || 0);

    traceOpenClawLedger(input.logger, "typed-final", {
      result: "replace",
      sessionId: inputReplace.sessionId,
      reqId: inputReplace.reqId,
      matchStrategy: typedFinal.matchStrategy,
      terminalEvent: summarizeTimelineEventForTrace(inputReplace.terminalEvent),
      event: summarizeTimelineEventForTrace(event),
      originalAnswerLength: currentAnswer.length,
      originalAnswerHash: hashTraceText(currentAnswer),
      typedFinalLength: typedFinal.text.length,
      typedFinalHash: hashTraceText(typedFinal.text),
      segmentCount: typedFinal.segmentCount,
      messageSeqs: typedFinal.messageSeqs
    }, input.config);

    if (inputReplace.sendToHub) {
      const delta = extractReplyDelta(inputReplace.reqId, typedFinal.text, true);
      if (delta) {
        await sendReply({
          reqId: inputReplace.reqId,
          text: delta,
          status: "streaming",
          sessionId: inputReplace.sessionId,
          mode: "replace",
          replace: true,
          eventKind: event.kind,
          payload
        });
      }
    }
    return true;
  }

  async function maybeApplyTypedTranscriptLiveReplace(inputLive: {
    sessionId: string;
    sourceEvent: TimelineEvent;
    eventScope: GatewayEventScope;
    reqId: string;
  }): Promise<boolean> {
    if (inputLive.eventScope.terminalSeen) {
      return false;
    }
    if (inputLive.eventScope.typedLiveInFlight) {
      return inputLive.eventScope.typedLiveInFlight;
    }

    const now = Date.now();
    const lastResolvedAt = inputLive.eventScope.typedLiveLastResolvedAtMs ?? 0;
    if (lastResolvedAt > 0 && now - lastResolvedAt < 300) {
      traceOpenClawLedger(input.logger, "typed-live", {
        result: "throttle",
        sessionId: inputLive.sessionId,
        reqId: inputLive.reqId,
        sourceEvent: summarizeTimelineEventForTrace(inputLive.sourceEvent),
        elapsedMs: now - lastResolvedAt
      }, input.config);
      return false;
    }

    const promise: Promise<boolean> = applyTypedTranscriptLiveReplace(inputLive)
      .then((result) => {
        if (result) {
          inputLive.eventScope.typedLiveLastResolvedAtMs = Date.now();
        }
        return result;
      })
      .finally(() => {
        if (inputLive.eventScope.typedLiveInFlight === promise) {
          inputLive.eventScope.typedLiveInFlight = undefined;
        }
      });
    inputLive.eventScope.typedLiveInFlight = promise;
    return promise;
  }

  async function applyTypedTranscriptLiveReplace(inputLive: {
    sessionId: string;
    sourceEvent: TimelineEvent;
    eventScope: GatewayEventScope;
    reqId: string;
  }): Promise<boolean> {
    const typedLive = await resolveTypedTranscriptLive(
      inputLive.sessionId,
      inputLive.sourceEvent,
      inputLive.eventScope
    );
    if (!typedLive) {
      traceOpenClawLedger(input.logger, "typed-live", {
        result: "miss",
        sessionId: inputLive.sessionId,
        reqId: inputLive.reqId,
        sourceEvent: summarizeTimelineEventForTrace(inputLive.sourceEvent)
      }, input.config);
      return false;
    }

    const typedLiveHash = hashTraceText(typedLive.text);
    if (typedLiveHash && typedLiveHash === inputLive.eventScope.typedLiveLastTextHash) {
      traceOpenClawLedger(input.logger, "typed-live", {
        result: "noop",
        sessionId: inputLive.sessionId,
        reqId: inputLive.reqId,
        matchStrategy: typedLive.matchStrategy,
        textLength: typedLive.text.length,
        textHash: typedLiveHash,
        segmentCount: typedLive.segmentCount
      }, input.config);
      return false;
    }

    const currentAnswer = readCurrentOpenClawAnswerText(inputLive.eventScope, inputLive.reqId);
    const event = buildTypedTranscriptLiveEvent(
      inputLive.sessionId,
      inputLive.sourceEvent,
      inputLive.eventScope,
      inputLive.reqId,
      typedLive,
      currentAnswer
    );
    applyOpenClawEventScopeActivity(event, inputLive.eventScope);
    const payload = augmentPayloadWithEventMeta(event, inputLive.eventScope);
    event.payload = payload;
    inputLive.eventScope.lastSeqSeen = Math.max(inputLive.eventScope.lastSeqSeen, event.seq || 0);
    inputLive.eventScope.typedLiveLastTextHash = typedLiveHash;

    traceOpenClawLedger(input.logger, "typed-live", {
      result: "replace",
      sessionId: inputLive.sessionId,
      reqId: inputLive.reqId,
      matchStrategy: typedLive.matchStrategy,
      sourceEvent: summarizeTimelineEventForTrace(inputLive.sourceEvent),
      event: summarizeTimelineEventForTrace(event),
      originalAnswerLength: currentAnswer.length,
      originalAnswerHash: hashTraceText(currentAnswer),
      typedLiveLength: typedLive.text.length,
      typedLiveHash,
      segmentCount: typedLive.segmentCount,
      messageSeqs: typedLive.messageSeqs
    }, input.config);

    const delta = extractReplyDelta(inputLive.reqId, typedLive.text, true);
    if (delta) {
      await sendReply({
        reqId: inputLive.reqId,
        text: delta,
        status: "streaming",
        sessionId: inputLive.sessionId,
        mode: "replace",
        replace: true,
        eventKind: event.kind,
        payload
      });
    }
    return true;
  }

  async function resolveTypedTranscriptLive(
    sessionId: string,
    sourceEvent: TimelineEvent,
    eventScope: GatewayEventScope
  ): Promise<OpenClawTypedLiveTranscript | null> {
    let messages: SessionMessage[] = [];
    try {
      messages = await input.gateway.getSessionMessages(sessionId, 200);
    } catch (error) {
      traceOpenClawLedger(input.logger, "typed-live", {
        result: "miss",
        reason: "get_session_messages_failed",
        sessionId,
        error: error instanceof Error ? error.message : String(error)
      }, input.config);
      return null;
    }

    const candidates = messages.filter((message) => message.role === "assistant" && readTypedTextSegmentsFromMessage(message).length > 0);
    if (!candidates.length) {
      return null;
    }

    const runId = getGatewayEventRunIdentity(sourceEvent) || eventScope.currentRunId || "";
    if (runId) {
      const byRunId = candidates.filter((message) => readSessionMessageRunIdentity(message) === runId);
      const resolved = buildTypedFinalTranscriptFromMessages(byRunId, "run_id");
      if (resolved) {
        return resolved;
      }
    }

    const responseId = getGatewayEventResponseIdentity(sourceEvent);
    if (responseId) {
      const byResponseId = candidates.filter((message) => readSessionMessageResponseIdentity(message) === responseId);
      const resolved = buildTypedFinalTranscriptFromMessages(byResponseId, "response_id");
      if (resolved) {
        return resolved;
      }
    }

    const byWindow = selectTypedTranscriptMessagesInRequestWindow(candidates, sourceEvent, eventScope, 1_000);
    return buildTypedFinalTranscriptFromMessages(byWindow, "request_window");
  }

  function buildTypedTranscriptLiveEvent(
    sessionId: string,
    sourceEvent: TimelineEvent,
    eventScope: GatewayEventScope,
    reqId: string,
    typedLive: OpenClawTypedLiveTranscript,
    currentAnswer: string
  ): TimelineEvent {
    const runId = getGatewayEventRunIdentity(sourceEvent) || eventScope.currentRunId || "";
    const turnId = eventScope.currentTurnId || eventScope.turnId;
    const textHash = hashTraceText(typedLive.text);
    const id = `typed-live:${reqId || eventScope.activeRequestId}:${runId || sourceEvent.id || "run"}:${textHash || "text"}`;
    return {
      id,
      sessionId,
      seq: getTypedTranscriptLiveEventSeq(sourceEvent, eventScope),
      kind: "assistant.delta",
      payload: {
        content: typedLive.text,
        state: "delta",
        mode: "replace",
        replace: true,
        source_kind: "typed_transcript.live_replace",
        typed_live: true,
        typed_live_match_strategy: typedLive.matchStrategy,
        typed_live_text_segment_count: typedLive.segmentCount,
        typed_live_original_answer_hash: hashTraceText(currentAnswer),
        typed_live_text_hash: textHash,
        typed_live_message_ids: typedLive.messageIds,
        typed_live_message_seqs: typedLive.messageSeqs,
        typed_live_source_event_id: sourceEvent.id,
        active_request_id: eventScope.activeRequestId,
        req_id: reqId,
        turn_id: turnId,
        segment_id: `${turnId}:answer:0`,
        segment_type: "answer",
        ...(runId ? { runId, run_id: runId } : {})
      },
      createdAt: sourceEvent.createdAt || new Date().toISOString()
    };
  }

  function getTypedTranscriptLiveEventSeq(sourceEvent: TimelineEvent, eventScope: GatewayEventScope): number {
    const sourceSeq = typeof sourceEvent.seq === "number" && Number.isFinite(sourceEvent.seq)
      ? sourceEvent.seq
      : eventScope.lastSeqSeen;
    return sourceSeq + 0.05;
  }

  async function resolveTypedTranscriptFinal(
    sessionId: string,
    terminalEvent: TimelineEvent,
    eventScope: GatewayEventScope
  ): Promise<OpenClawTypedFinalTranscript | null> {
    let messages: SessionMessage[] = [];
    try {
      messages = await input.gateway.getSessionMessages(sessionId, 200);
    } catch (error) {
      traceOpenClawLedger(input.logger, "typed-final", {
        result: "miss",
        reason: "get_session_messages_failed",
        sessionId,
        error: error instanceof Error ? error.message : String(error)
      }, input.config);
      return null;
    }

    const candidates = messages.filter((message) => message.role === "assistant" && readTypedTextSegmentsFromMessage(message).length > 0);
    if (!candidates.length) {
      return null;
    }

    const runId = getGatewayEventRunIdentity(terminalEvent) || eventScope.currentRunId || "";
    if (runId) {
      const byRunId = candidates.filter((message) => readSessionMessageRunIdentity(message) === runId);
      const resolved = buildTypedFinalTranscriptFromMessages(byRunId, "run_id");
      if (resolved) {
        return resolved;
      }
    }

    const responseId = getGatewayEventResponseIdentity(terminalEvent);
    if (responseId) {
      const byResponseId = candidates.filter((message) => readSessionMessageResponseIdentity(message) === responseId);
      const resolved = buildTypedFinalTranscriptFromMessages(byResponseId, "response_id");
      if (resolved) {
        return resolved;
      }
    }

    const byWindow = selectTypedFinalMessagesInRequestWindow(candidates, terminalEvent, eventScope);
    const windowResolved = buildTypedFinalTranscriptFromMessages(byWindow, "request_window");
    if (windowResolved) {
      return windowResolved;
    }

    const latestAfterUser = selectTypedFinalMessagesAfterLatestUser(messages);
    return buildTypedFinalTranscriptFromMessages(latestAfterUser, "latest_after_user");
  }

  function buildTypedTranscriptFinalEvent(
    sessionId: string,
    terminalEvent: TimelineEvent,
    eventScope: GatewayEventScope,
    reqId: string,
    typedFinal: OpenClawTypedFinalTranscript,
    currentAnswer: string
  ): TimelineEvent {
    const runId = getGatewayEventRunIdentity(terminalEvent) || eventScope.currentRunId || "";
    const turnId = eventScope.currentTurnId || eventScope.turnId;
    const seq = getTypedTranscriptFinalEventSeq(terminalEvent, eventScope);
    const id = `typed-final:${reqId || eventScope.activeRequestId}:${runId || terminalEvent.id || "run"}`;
    return {
      id,
      sessionId,
      seq,
      kind: "assistant.message",
      payload: {
        content: typedFinal.text,
        state: "final",
        mode: "replace",
        replace: true,
        source_kind: "typed_transcript.final_replace",
        typed_final: true,
        typed_final_match_strategy: typedFinal.matchStrategy,
        typed_final_text_segment_count: typedFinal.segmentCount,
        typed_final_original_answer_hash: hashTraceText(currentAnswer),
        typed_final_text_hash: hashTraceText(typedFinal.text),
        typed_final_message_ids: typedFinal.messageIds,
        typed_final_message_seqs: typedFinal.messageSeqs,
        typed_final_terminal_event_id: terminalEvent.id,
        active_request_id: eventScope.activeRequestId,
        req_id: reqId,
        turn_id: turnId,
        segment_id: `${turnId}:answer:0`,
        segment_type: "answer",
        ...(runId ? { runId, run_id: runId } : {})
      },
      createdAt: terminalEvent.createdAt || new Date().toISOString()
    };
  }

  function getTypedTranscriptFinalEventSeq(terminalEvent: TimelineEvent, eventScope: GatewayEventScope): number {
    const terminalSeq = typeof terminalEvent.seq === "number" && Number.isFinite(terminalEvent.seq)
      ? terminalEvent.seq
      : eventScope.lastSeqSeen;
    return terminalSeq + 0.1;
  }

  function readCurrentOpenClawAnswerText(eventScope: GatewayEventScope, reqId: string): string {
    const turnId = eventScope.currentTurnId || eventScope.turnId;
    const answerText =
      eventScope.answerSegmentTextById.get(`${turnId}:answer:0`) ||
      (eventScope.currentAnswerSegmentId ? eventScope.answerSegmentTextById.get(eventScope.currentAnswerSegmentId) : "") ||
      "";
    return answerText || lastReplyByReq.get(reqId) || "";
  }

  function isCurrentOpenClawAnswerVisibleOrTrusted(eventScope: GatewayEventScope, reqId: string): boolean {
    const turnId = eventScope.currentTurnId || eventScope.turnId;
    const candidateSegmentIds = [
      `${turnId}:answer:0`,
      eventScope.currentAnswerSegmentId
    ].filter((segmentId): segmentId is string => Boolean(segmentId));

    for (const segmentId of candidateSegmentIds) {
      if (eventScope.answerSegmentTrustedById.get(segmentId)) {
        return true;
      }
      const visibility = eventScope.answerSegmentVisibilityById.get(segmentId);
      if (visibility === "stream" || visibility === "final") {
        return true;
      }
    }

    return Boolean(lastReplyByReq.get(reqId));
  }

  async function enrichToolEventFromTypedHistory(
    sessionId: string,
    event: TimelineEvent,
    eventScope: GatewayEventScope
  ): Promise<{ event: TimelineEvent; syntheticCallEvent?: TimelineEvent }> {
    if (!shouldResolveTypedToolCallForEvent(event)) {
      return { event };
    }

    const typedCall = await resolveTypedToolCallFromHistory(sessionId, event, eventScope);
    if (!typedCall) {
      traceOpenClawLedger(input.logger, "typed-tool", {
        result: "miss",
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return { event };
    }

    const enrichedEvent = mergeTypedToolCallIntoEvent(event, typedCall);
    const enrichmentKey = `${eventScope.currentTurnId || eventScope.turnId}:${typedCall.id || readToolCallIdFromPayload(event.payload) || typedCall.name}`;
    let syntheticCallEvent: TimelineEvent | undefined;
    if (event.kind === "tool.call") {
      eventScope.typedToolCallEnrichedKeys.add(enrichmentKey);
    } else if (!eventScope.typedToolCallEnrichedKeys.has(enrichmentKey)) {
      eventScope.typedToolCallEnrichedKeys.add(enrichmentKey);
      syntheticCallEvent = buildTypedToolCallEnrichmentEvent(sessionId, event, eventScope, typedCall);
    }

    traceOpenClawLedger(input.logger, "typed-tool", {
      result: "enrich",
      sessionId,
      event: summarizeTimelineEventForTrace(event),
      sourceEventId: typedCall.sourceEventId,
      sourceSeq: typedCall.sourceSeq,
      toolCallIdHash: hashTraceText(typedCall.id),
      toolName: typedCall.name,
      commandLength: typedCall.command?.length ?? 0,
      commandHash: hashTraceText(typedCall.command || "")
    }, input.config);
    return { event: enrichedEvent, syntheticCallEvent };
  }

  function shouldResolveTypedToolCallForEvent(event: TimelineEvent): boolean {
    if (event.kind !== "tool.call" && event.kind !== "tool.result") {
      return false;
    }
    const toolName = readToolNameFromPayload(event.payload);
    if (!isExecToolName(toolName)) {
      return false;
    }
    return !readToolCommandFromPayload(event.payload);
  }

  async function resolveTypedToolCallFromHistory(
    sessionId: string,
    event: TimelineEvent,
    eventScope: GatewayEventScope
  ): Promise<OpenClawTypedToolCall | null> {
    let events: TimelineEvent[] = [];
    try {
      events = await input.gateway.listEvents(sessionId, 0);
    } catch (error) {
      traceOpenClawLedger(input.logger, "typed-tool", {
        result: "miss",
        reason: "list_events_failed",
        sessionId,
        error: error instanceof Error ? error.message : String(error)
      }, input.config);
      return null;
    }

    const typedCalls = events
      .filter((candidate) => candidate.kind === "tool.call")
      .map(readTypedToolCallFromEvent)
      .filter((candidate): candidate is OpenClawTypedToolCall => Boolean(candidate && candidate.command));
    if (!typedCalls.length) {
      return null;
    }

    const toolCallId = readToolCallIdFromPayload(event.payload);
    if (toolCallId) {
      const byId = typedCalls.find((candidate) => candidate.id === toolCallId);
      if (byId) {
        return byId;
      }
    }

    const eventToolName = normalizeToolName(readToolNameFromPayload(event.payload));
    const eventSeq = typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : Number.POSITIVE_INFINITY;
    const sameToolCalls = typedCalls
      .filter((candidate) => normalizeToolName(candidate.name) === eventToolName)
      .sort((left, right) => (right.sourceSeq ?? 0) - (left.sourceSeq ?? 0));
    return sameToolCalls.find((candidate) => (candidate.sourceSeq ?? 0) <= eventSeq) ?? sameToolCalls[0] ?? null;
  }

  function readTypedToolCallFromEvent(event: TimelineEvent): OpenClawTypedToolCall | null {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const name = normalizeToolName(stringOr(data.name, data.toolName, data.tool_name, payload.name, payload.toolName, "tool"));
    const id = readToolCallIdFromPayload(payload) || `${name}:${event.seq || event.id}`;
    const args = readToolArgsFromPayload(payload);
    const command = readToolCommandFromPayload(payload);
    if (!id || !name || Object.keys(args).length === 0) {
      return null;
    }
    return {
      id,
      name,
      args,
      ...(command ? { command } : {}),
      ...(stringOr(data.meta, payload.meta) ? { meta: stringOr(data.meta, payload.meta) } : {}),
      sourceEventId: event.id,
      sourceSeq: event.seq
    };
  }

  function mergeTypedToolCallIntoEvent(event: TimelineEvent, typedCall: OpenClawTypedToolCall): TimelineEvent {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const args = {
      ...readToolArgsFromPayload(payload),
      ...typedCall.args,
      ...(typedCall.command ? { command: typedCall.command } : {})
    };
    return {
      ...event,
      payload: {
        ...payload,
        source_kind: stringOr(payload.source_kind) || event.kind,
        typed_tool_call_enriched: true,
        typed_tool_call_source_event_id: typedCall.sourceEventId,
        typed_tool_call_source_seq: typedCall.sourceSeq,
        typed_tool_call_command_hash: hashTraceText(typedCall.command || ""),
        data: {
          ...data,
          name: typedCall.name,
          toolCallId: typedCall.id,
          args,
          ...(typedCall.command ? { command: typedCall.command } : {}),
          ...(typedCall.meta && !stringOr(data.meta) ? { meta: typedCall.meta } : {})
        }
      }
    };
  }

  function buildTypedToolCallEnrichmentEvent(
    sessionId: string,
    sourceEvent: TimelineEvent,
    eventScope: GatewayEventScope,
    typedCall: OpenClawTypedToolCall
  ): TimelineEvent {
    const payload = toRecord(sourceEvent.payload);
    const runId = getGatewayEventRunIdentity(sourceEvent) || eventScope.currentRunId || "";
    return {
      id: `typed-tool-call:${typedCall.id || sourceEvent.id}:${hashTraceText(typedCall.command || typedCall.name) || "tool"}`,
      sessionId,
      seq: getTypedToolCallEnrichmentEventSeq(sourceEvent, eventScope),
      kind: "tool.call",
      payload: {
        source_kind: "typed_transcript.tool_call_enrich",
        typed_tool_call_enriched: true,
        typed_tool_call_source_event_id: typedCall.sourceEventId,
        typed_tool_call_source_seq: typedCall.sourceSeq,
        typed_tool_call_command_hash: hashTraceText(typedCall.command || ""),
        active_request_id: eventScope.activeRequestId,
        turn_id: eventScope.currentTurnId || eventScope.turnId,
        data: {
          phase: "call",
          name: typedCall.name,
          toolCallId: typedCall.id,
          args: {
            ...typedCall.args,
            ...(typedCall.command ? { command: typedCall.command } : {})
          },
          ...(typedCall.command ? { command: typedCall.command } : {}),
          ...(typedCall.meta ? { meta: typedCall.meta } : {})
        },
        ...(runId ? { runId, run_id: runId } : {}),
        ...(payload.rawSeq ? { rawSeq: payload.rawSeq } : {})
      },
      createdAt: sourceEvent.createdAt || new Date().toISOString()
    };
  }

  function getTypedToolCallEnrichmentEventSeq(sourceEvent: TimelineEvent, eventScope: GatewayEventScope): number {
    const sourceSeq = typeof sourceEvent.seq === "number" && Number.isFinite(sourceEvent.seq)
      ? sourceEvent.seq
      : eventScope.lastSeqSeen;
    return sourceSeq - 0.01;
  }

  function readToolNameFromPayload(payload: unknown): string {
    const record = toRecord(payload);
    const data = toRecord(record.data);
    return normalizeToolName(stringOr(data.name, data.toolName, data.tool_name, data.tool, record.name, record.toolName, record.tool_name, record.tool));
  }

  function readToolCallIdFromPayload(payload: unknown): string {
    const record = toRecord(payload);
    const data = toRecord(record.data);
    return stringOr(
      data.toolCallId,
      data.tool_call_id,
      data.callId,
      data.call_id,
      data.id,
      record.toolCallId,
      record.tool_call_id,
      record.callId,
      record.call_id,
      record.id
    );
  }

  function readToolArgsFromPayload(payload: unknown): Record<string, unknown> {
    const record = toRecord(payload);
    const data = toRecord(record.data);
    const fn = toRecord(data.function ?? record.function);
    for (const value of [
      data.args,
      data.arguments,
      data.input,
      data.parameters,
      record.args,
      record.arguments,
      record.input,
      record.parameters,
      fn.arguments
    ]) {
      const parsed = parseToolArgumentsRecord(value);
      if (Object.keys(parsed).length > 0) {
        return parsed;
      }
    }
    return {};
  }

  function parseToolArgumentsRecord(value: unknown): Record<string, unknown> {
    const normalized = normalizeToolArguments(value);
    return toRecord(normalized);
  }

  function normalizeToolArguments(value: unknown): unknown {
    if (typeof value !== "string") {
      return value;
    }
    const trimmed = value.trim();
    if (!trimmed) {
      return undefined;
    }
    try {
      return JSON.parse(trimmed);
    } catch {
      return trimmed;
    }
  }

  function readToolCommandFromPayload(payload: unknown): string {
    const record = toRecord(payload);
    const data = toRecord(record.data);
    const args = readToolArgsFromPayload(record);
    for (const candidate of [args, data, record]) {
      const command = normalizeToolCommandText(stringOr(
        candidate.command,
        candidate.cmd,
        candidate.script,
        candidate.shell,
        candidate.commandLine,
        candidate.command_line,
        candidate.code
      ));
      if (command) {
        return command;
      }
    }
    return "";
  }

  function normalizeToolCommandText(value: string): string {
    const command = value.replace(/\r\n/g, "\n").replace(/\\"/g, "\"").replace(/\\'/g, "'").trim();
    const normalized = command.replace(/\s+/g, " ").trim().toLowerCase();
    if (!normalized || normalized === "exec" || normalized === "used exec" || normalized === "used tool" || normalized === "tool output") {
      return "";
    }
    return command;
  }

  function normalizeToolName(value: string): string {
    const normalized = value.replace(/[\s-]+/g, "_").trim().toLowerCase();
    return isExecToolName(normalized) ? "exec" : normalized || "tool";
  }

  function isExecToolName(value: string): boolean {
    const normalized = value.replace(/[\s-]+/g, "_").trim().toLowerCase();
    return normalized === "exec" || normalized === "used_exec" || normalized === "bash" || normalized === "shell" || normalized === "run_command";
  }

  function selectTypedFinalMessagesInRequestWindow(
    messages: SessionMessage[],
    terminalEvent: TimelineEvent,
    eventScope: GatewayEventScope
  ): SessionMessage[] {
    return selectTypedTranscriptMessagesInRequestWindow(messages, terminalEvent, eventScope, 10_000);
  }

  function selectTypedTranscriptMessagesInRequestWindow(
    messages: SessionMessage[],
    anchorEvent: TimelineEvent,
    eventScope: GatewayEventScope,
    maxAfterAnchorMs: number
  ): SessionMessage[] {
    if (!eventScope.eventBoundaryMs) {
      return [];
    }
    const anchorMs = Date.parse(anchorEvent.createdAt || "");
    const maxMs = Number.isFinite(anchorMs)
      ? Math.max(anchorMs, Date.now()) + maxAfterAnchorMs
      : Date.now() + maxAfterAnchorMs;
    const minMs = eventScope.eventBoundaryMs - 2_000;
    return messages.filter((message) => {
      const messageMs = Date.parse(message.createdAt || "");
      return Number.isFinite(messageMs) && messageMs >= minMs && messageMs <= maxMs;
    });
  }

  function selectTypedFinalMessagesAfterLatestUser(messages: SessionMessage[]): SessionMessage[] {
    const lastUserIndex = findLastSessionMessageIndex(messages, (message) => message.role === "user");
    return messages
      .slice(lastUserIndex + 1)
      .filter((message) => message.role === "assistant" && readTypedTextSegmentsFromMessage(message).length > 0);
  }

  function findLastSessionMessageIndex(
    messages: SessionMessage[],
    predicate: (message: SessionMessage) => boolean
  ): number {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      if (predicate(messages[index]!)) {
        return index;
      }
    }
    return -1;
  }

  function buildTypedFinalTranscriptFromMessages(
    messages: SessionMessage[],
    matchStrategy: OpenClawTypedFinalMatchStrategy
  ): OpenClawTypedFinalTranscript | null {
    const orderedMessages = [...messages].sort(compareSessionMessageOrder);
    const segments: string[] = [];
    const messageIds: string[] = [];
    const messageSeqs: number[] = [];
    for (const message of orderedMessages) {
      const messageSegments = readTypedTextSegmentsFromMessage(message);
      if (!messageSegments.length) {
        continue;
      }
      segments.push(...messageSegments);
      messageIds.push(message.id);
      const seq = readSessionMessageSeq(message);
      if (seq > 0) {
        messageSeqs.push(seq);
      }
    }
    const text = joinTypedTranscriptTextSegments(segments);
    if (!text.trim()) {
      return null;
    }
    return {
      text,
      segmentCount: segments.length,
      matchStrategy,
      messageIds,
      messageSeqs
    };
  }

  function compareSessionMessageOrder(left: SessionMessage, right: SessionMessage): number {
    const leftSeq = readSessionMessageSeq(left);
    const rightSeq = readSessionMessageSeq(right);
    if (leftSeq !== rightSeq) {
      if (!leftSeq) return 1;
      if (!rightSeq) return -1;
      return leftSeq - rightSeq;
    }
    const leftMs = Date.parse(left.createdAt || "");
    const rightMs = Date.parse(right.createdAt || "");
    if (Number.isFinite(leftMs) && Number.isFinite(rightMs) && leftMs !== rightMs) {
      return leftMs - rightMs;
    }
    return left.id.localeCompare(right.id);
  }

  function readTypedTextSegmentsFromMessage(message: SessionMessage): string[] {
    for (const source of [message.payload, message.metadata, message.data, message.__openclaw]) {
      const segments = readStringArrayMetadata(toRecord(source).openclaw_typed_text_segments);
      if (segments.length > 0) {
        return segments;
      }
    }
    return [];
  }

  function readStringArrayMetadata(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }
    return value.filter((item): item is string => typeof item === "string" && item.trim().length > 0);
  }

  function joinTypedTranscriptTextSegments(segments: string[]): string {
    let output = "";
    for (const segment of segments) {
      if (!segment.trim()) {
        continue;
      }
      if (!output) {
        output = segment;
        continue;
      }
      if (/\s$/.test(output) || /^\s/.test(segment)) {
        output += segment;
      } else {
        output += `\n\n${segment}`;
      }
    }
    return output.trim();
  }

  function readSessionMessageSeq(message: SessionMessage): number {
    const payload = toRecord(message.payload);
    const metadata = toRecord(message.metadata);
    const data = toRecord(message.data);
    const rawMeta = toRecord(message.__openclaw);
    return firstPositiveNumber(
      message.seq,
      message.messageSeq,
      message.message_seq,
      rawMeta.seq,
      rawMeta.messageSeq,
      rawMeta.message_seq,
      payload.rawSeq,
      payload.seq,
      payload.messageSeq,
      payload.message_seq,
      metadata.rawSeq,
      metadata.seq,
      metadata.messageSeq,
      metadata.message_seq,
      data.rawSeq,
      data.seq,
      data.messageSeq,
      data.message_seq
    );
  }

  function readSessionMessageRunIdentity(message: SessionMessage): string {
    return identityStringOr(
      toRecord(message.payload).runId,
      toRecord(message.payload).run_id,
      toRecord(message.metadata).runId,
      toRecord(message.metadata).run_id,
      toRecord(message.data).runId,
      toRecord(message.data).run_id,
      toRecord(message.__openclaw).runId,
      toRecord(message.__openclaw).run_id
    );
  }

  function readSessionMessageResponseIdentity(message: SessionMessage): string {
    return identityStringOr(
      toRecord(message.payload).responseId,
      toRecord(message.payload).response_id,
      toRecord(message.metadata).responseId,
      toRecord(message.metadata).response_id,
      toRecord(message.data).responseId,
      toRecord(message.data).response_id,
      toRecord(message.__openclaw).responseId,
      toRecord(message.__openclaw).response_id
    );
  }

  function isReplayFromPreviousRun(event: GatewayEvent, eventScope: GatewayEventScope): boolean {
    if (eventScope.currentActivitySeen || !isTerminalRunEvent(event)) {
      return false;
    }
    const eventMs = Date.parse(event.createdAt);
    return Number.isFinite(eventMs) && eventMs < eventScope.eventBoundaryMs;
  }

  function isTerminalRunEvent(event: GatewayEvent): boolean {
    return event.kind === "run.completed" || event.kind === "run.failed" || event.kind === "run.interrupted";
  }

  function isFailedStatusUpdate(event: GatewayEvent): boolean {
    if (event.kind !== "status.update") {
      return false;
    }
    const payload = toRecord(event.payload);
    const session = toRecord(payload.session);
    const phase = String(payload.phase ?? session.phase ?? "").toLowerCase();
    const status = String(payload.status ?? session.status ?? "").toLowerCase();
    return phase === "error" || phase === "failed" || status === "error" || status === "failed";
  }

  function buildFailedRunEventFromStatusUpdate(
    event: GatewayEvent,
    sessionId: string,
    reqId: string,
    eventScope: GatewayEventScope
  ): TimelineEvent | null {
    if (!isFailedStatusUpdate(event)) {
      return null;
    }
    const runIdentity = getGatewayEventRunIdentity(event) || eventScope.currentRunId;
    const eventMs = Date.parse(event.createdAt);
    const eventBeforeBoundary = Number.isFinite(eventMs) && eventMs < eventScope.eventBoundaryMs;
    const classification = classifyOpenClawRunFailure(event, eventScope);
    const requestScopedAuthFailure = classification.authRelated && !eventBeforeBoundary;
    if (eventScope.currentRunId && runIdentity && eventScope.currentRunId !== runIdentity) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_status_failed_mismatched_run", {
        reqId,
        sessionId,
        currentRunId: eventScope.currentRunId,
        eventRunId: runIdentity,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return null;
    }
    if (!runIdentity && !eventScope.currentActivitySeen && !requestScopedAuthFailure) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_status_failed_without_current_run", {
        reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return null;
    }

    if (!eventScope.currentActivitySeen && eventBeforeBoundary) {
      traceOpenClawDuplicate(input.logger, "hub.event.skip_status_failed_before_boundary", {
        reqId,
        sessionId,
        event: summarizeTimelineEventForTrace(event)
      }, input.config);
      return null;
    }

    const payload = enrichFailedRunEvent(
      {
        ...event,
        kind: "run.failed",
        payload: {
          ...toRecord(event.payload),
          derived_terminal: true,
          derived_from_kind: event.kind,
          derived_from_event_id: event.id,
          derived_from_event_seq: event.seq
        }
      },
      reqId,
      eventScope,
      classification
    ).payload;
    return {
      id: `derived:run.failed:${event.id || `${event.sessionId || sessionId}:${event.seq || Date.now()}`}`,
      sessionId: event.sessionId || sessionId,
      seq: typeof event.seq === "number" && Number.isFinite(event.seq) ? event.seq : eventScope.lastSeqSeen + 1,
      kind: "run.failed",
      payload,
      createdAt: event.createdAt || new Date().toISOString()
    };
  }

  function getGatewayEventRunIdentity(event: GatewayEvent): string {
    const payload = toRecord(event.payload);
    const session = toRecord(payload.session);
    return identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id,
      session.runId,
      session.run_id,
      session.responseId,
      session.response_id
    );
  }

  function getGatewayEventResponseIdentity(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const session = toRecord(payload.session);
    return identityStringOr(
      payload.responseId,
      payload.response_id,
      session.responseId,
      session.response_id
    );
  }

  function isVisibleOpenClawResponseEvent(event: GatewayEvent): boolean {
    if (event.kind === "assistant.delta" || event.kind === "assistant.message") {
      return !isUntrustedRawOpenClawAnswerEvent(event) && String(event.payload?.content ?? "").trim().length > 0;
    }
    if (event.kind === "assistant.thinking") {
      return String(event.payload?.content ?? "").trim().length > 0;
    }
    if (event.kind === "tool.call" || event.kind === "tool.result") {
      return true;
    }
    if (event.kind === "process.step" && isOutputFilesProcessStepEvent(event)) {
      return true;
    }
    return false;
  }

  function isOpenClawAnswerEvent(event: TimelineEvent): boolean {
    return event.kind === "assistant.delta" || event.kind === "assistant.message";
  }

  function isTrustedOpenClawAnswerEvent(event: TimelineEvent): boolean {
    if (!isOpenClawAnswerEvent(event)) {
      return false;
    }
    const payload = toRecord(event.payload);
    const sourceKind = readStringMetadata(payload, "source_kind");
    if (sourceKind === "typed_transcript.live_replace" || sourceKind === "typed_transcript.final_replace") {
      return true;
    }
    if (readBooleanMetadata(payload, "typed_live") === true || readBooleanMetadata(payload, "typed_final") === true) {
      return true;
    }
    if (readBooleanMetadata(payload, "trusted_answer") === true) {
      return true;
    }

    const eventType = readStringMetadata(payload, "eventType") || readStringMetadata(payload, "event_type");
    return eventType === "response.output_text.delta" ||
      eventType === "response.output_text.done" ||
      eventType === "response.completed";
  }

  function isUntrustedRawOpenClawAnswerEvent(event: TimelineEvent): boolean {
    if (!isOpenClawAnswerEvent(event) || isTrustedOpenClawAnswerEvent(event)) {
      return false;
    }
    const payload = toRecord(event.payload);
    const eventId = event.id || "";
    return payload.rawSeq !== undefined ||
      payload.raw_seq !== undefined ||
      eventId.includes(":chat:") ||
      eventId.includes(":message:");
  }

  function enrichFailedRunEvent(
    event: GatewayEvent,
    reqId: string,
    eventScope: GatewayEventScope,
    classification: OpenClawRunFailureClassification
  ): GatewayEvent {
    const payload = toRecord(event.payload);
    const runIdentity = getGatewayEventRunIdentity(event) || eventScope.currentRunId;
    const turnId = eventScope.currentTurnId || eventScope.turnId || stringOr(payload.turn_id);
    const rawMessage = classification.rawMessage || extractOpenClawFailureMessage(payload);
    return {
      ...event,
      payload: {
        ...payload,
        ...(runIdentity ? { runId: payload.runId ?? runIdentity, run_id: payload.run_id ?? runIdentity } : {}),
        active_request_id: stringOr(payload.active_request_id) || eventScope.activeRequestId || reqId,
        req_id: stringOr(payload.req_id) || reqId,
        request_id: stringOr(payload.request_id) || reqId,
        turn_id: turnId,
        terminal_status: "failed",
        failure_code: classification.code,
        failure_reason: classification.reason,
        failure_auth_related: classification.authRelated,
        failure_confidence: classification.confidence,
        user_message: classification.userMessage,
        error_message: classification.userMessage,
        error: classification.userMessage,
        message: classification.userMessage,
        ...(rawMessage ? { raw_error_message: rawMessage } : {}),
        failure_classification: {
          code: classification.code,
          reason: classification.reason,
          user_message: classification.userMessage,
          ...(rawMessage ? { raw_message: rawMessage } : {}),
          ...(classification.provider ? { provider: classification.provider } : {}),
          ...(classification.model ? { model: classification.model } : {}),
          ...(typeof classification.runtimeMs === "number" ? { runtime_ms: classification.runtimeMs } : {}),
          auth_related: classification.authRelated,
          confidence: classification.confidence,
          visible_response_seen: eventScope.visibleResponseSeen
        }
      }
    };
  }

  function classifyOpenClawRunFailure(
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): OpenClawRunFailureClassification {
    const payload = toRecord(event.payload);
    const existingClassification = toRecord(payload.failure_classification);
    const existingCode = stringOr(payload.failure_code, existingClassification.code);
    const existingUserMessage = stringOr(payload.user_message, payload.error_message, existingClassification.user_message);
    const rawMessage = stringOr(payload.raw_error_message, existingClassification.raw_message) || extractOpenClawFailureMessage(payload);
    const provider = readOpenClawFailureProvider(payload);
    const model = readOpenClawFailureModel(payload);
    const runtimeMs = readOpenClawFailureRuntimeMs(payload);
    if (existingCode && existingUserMessage) {
      return {
        code: existingCode,
        reason: stringOr(payload.failure_reason, existingClassification.reason) || "openclaw_run_failed",
        userMessage: existingUserMessage,
        ...(rawMessage ? { rawMessage } : {}),
        ...(provider ? { provider } : {}),
        ...(model ? { model } : {}),
        ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
        authRelated: Boolean(payload.failure_auth_related ?? existingClassification.auth_related),
        confidence: normalizeFailureConfidence(stringOr(payload.failure_confidence, existingClassification.confidence)) ?? "medium"
      };
    }

    const lowerRaw = rawMessage.toLowerCase();
    const authRelated = isOpenClawAuthenticationFailureText(lowerRaw);
    const qclawSignal = isQClawFailureSignal(`${provider} ${model} ${rawMessage}`.toLowerCase());
    const failedFast = typeof runtimeMs !== "number" || runtimeMs <= 5_000;
    const lowInformationFailure = !rawMessage || isLowInformationFailureMessage(rawMessage);
    if (qclawSignal && (authRelated || (failedFast && lowInformationFailure && !eventScope.visibleResponseSeen))) {
      return {
        code: "QCLAW_LOGIN_REQUIRED",
        reason: authRelated ? "qclaw_auth_failed" : "qclaw_login_or_provider_auth_failed",
        userMessage: buildOpenClawAuthFailureUserMessage({
          scope: "qclaw",
          model
        }),
        ...(rawMessage ? { rawMessage } : {}),
        ...(provider ? { provider } : {}),
        ...(model ? { model } : {}),
        ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
        authRelated: true,
        confidence: authRelated ? "high" : "medium"
      };
    }

    if (authRelated) {
      return {
        code: "OPENCLAW_AUTH_REQUIRED",
        reason: "openclaw_auth_failed",
        userMessage: buildOpenClawAuthFailureUserMessage({
          scope: "openclaw",
          model
        }),
        rawMessage,
        ...(provider ? { provider } : {}),
        ...(model ? { model } : {}),
        ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
        authRelated: true,
        confidence: "high"
      };
    }

    const readableFailure = rawMessage && !isLowInformationFailureMessage(rawMessage)
      ? rawMessage
      : "OpenClaw 智能体运行失败，但 Gateway 没有提供可读错误信息。请查看 OpenClaw/QClaw 登录状态、模型供应商凭据以及 raw event 调试日志。";
    return {
      code: inferErrorCode(rawMessage || "OpenClaw run failed"),
      reason: "openclaw_run_failed",
      userMessage: readableFailure,
      ...(rawMessage ? { rawMessage } : {}),
      ...(provider ? { provider } : {}),
      ...(model ? { model } : {}),
      ...(typeof runtimeMs === "number" ? { runtimeMs } : {}),
      authRelated: false,
      confidence: rawMessage ? "medium" : "low"
    };
  }

  function readOpenClawFailureProvider(payload: Record<string, unknown>): string {
    const session = toRecord(payload.session);
    const modelInfo = toRecord(payload.modelInfo);
    return stringOr(
      payload.modelProvider,
      payload.model_provider,
      payload.provider,
      session.modelProvider,
      session.model_provider,
      session.provider,
      modelInfo.provider
    );
  }

  function readOpenClawFailureModel(payload: Record<string, unknown>): string {
    const session = toRecord(payload.session);
    const modelInfo = toRecord(payload.modelInfo);
    return stringOr(
      payload.model,
      payload.modelName,
      payload.model_name,
      session.model,
      session.modelName,
      session.model_name,
      modelInfo.model,
      modelInfo.name
    );
  }

  function readOpenClawFailureRuntimeMs(payload: Record<string, unknown>): number | undefined {
    const session = toRecord(payload.session);
    return numberOr(
      payload.runtimeMs,
      payload.runtime_ms,
      payload.elapsedMs,
      payload.elapsed_ms,
      session.runtimeMs,
      session.runtime_ms,
      session.elapsedMs,
      session.elapsed_ms
    );
  }

  function extractOpenClawFailureMessage(payload: Record<string, unknown>): string {
    const candidates = collectOpenClawFailureMessageCandidates(payload, 0);
    return candidates.find((candidate) => !isLowInformationFailureMessage(candidate)) || candidates[0] || "";
  }

  function collectOpenClawFailureMessageCandidates(value: unknown, depth: number): string[] {
    if (depth > 3) {
      return [];
    }
    if (typeof value === "string") {
      const normalized = value.trim();
      return normalized ? [normalized] : [];
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return [];
    }
    const record = value as Record<string, unknown>;
    const directKeys = [
      "error",
      "message",
      "error_message",
      "errorMessage",
      "detail",
      "details",
      "reason",
      "failure_reason",
      "cause",
      "description"
    ];
    const direct = directKeys
      .map((key) => record[key])
      .flatMap((candidate) => collectOpenClawFailureMessageCandidates(candidate, depth + 1));
    const nestedKeys = ["error", "cause", "data", "response", "session", "result"];
    const nested = nestedKeys
      .filter((key) => typeof record[key] === "object" && record[key] !== null)
      .flatMap((key) => collectOpenClawFailureMessageCandidates(record[key], depth + 1));
    return [...direct, ...nested];
  }

  function isLowInformationFailureMessage(message: string): boolean {
    const normalized = message.replace(/\s+/g, " ").trim().toLowerCase();
    return (
      !normalized ||
      normalized === "error" ||
      normalized === "failed" ||
      normalized === "failure" ||
      normalized === "run.failed" ||
      normalized === "unknown error" ||
      normalized === "internal_error" ||
      normalized === "status failed"
    );
  }

  function isOpenClawAuthenticationFailureText(lowerText: string): boolean {
    return /login|log in|sign in|auth|unauthorized|forbidden|credential|api key|apikey|token|permission|401|403|not logged in|认证|鉴权|授权|登录|未登录|凭据|密钥|权限|账号|账户/.test(lowerText);
  }

  function isQClawFailureSignal(lowerText: string): boolean {
    return /(?:^|[^a-z0-9])q[\s_-]*claw(?:[^a-z0-9]|$)/.test(lowerText);
  }

  function buildOpenClawAuthFailureUserMessage(inputMessage: {
    scope: "qclaw" | "openclaw";
    model?: string;
  }): string {
    const modelHint = inputMessage.model ? `当前模型：${inputMessage.model}。` : "";
    if (inputMessage.scope === "qclaw") {
      return `QClaw/OpenClaw 智能体登录失败或模型供应商认证失效。请重新登录 QClaw/OpenClaw，或检查模型供应商的 API Key、Base URL 与账号权限。${modelHint}`;
    }
    return `OpenClaw 智能体登录失败或模型供应商认证失效。请重新登录 OpenClaw/QClaw，或检查模型供应商凭据、Base URL 与账号权限。${modelHint}`;
  }

  function normalizeFailureConfidence(value: string): OpenClawRunFailureClassification["confidence"] | undefined {
    if (value === "high" || value === "medium" || value === "low") {
      return value;
    }
    return undefined;
  }

  function isCurrentRunActivityEvent(event: GatewayEvent): boolean {
    if (event.kind === "run.started") {
      return true;
    }
    if (
      event.kind === "assistant.delta" ||
      event.kind === "assistant.message" ||
      event.kind === "assistant.thinking" ||
      event.kind === "tool.call" ||
      event.kind === "tool.result"
    ) {
      return true;
    }
    const payload = toRecord(event.payload);
    return event.kind === "status.update" && String(payload.phase ?? payload.status ?? "") === "running";
  }

  function isReplyReplaceEvent(event: TimelineEvent): boolean {
    return readBooleanMetadata(event.payload, "replace") === true || readStringMetadata(event.payload, "mode") === "replace";
  }

  function extractReplyDelta(reqId: string, content: string, replace = false): string {
    if (!content) {
      return "";
    }

    const previous = lastReplyByReq.get(reqId) ?? "";
    if (!previous) {
      lastReplyByReq.set(reqId, content);
      return content;
    }

    if (replace) {
      if (content === previous) {
        return "";
      }
      lastReplyByReq.set(reqId, content);
      return content;
    }

    if (content === previous || previous.startsWith(content)) {
      return "";
    }

    if (content.startsWith(previous)) {
      const delta = content.slice(previous.length);
      lastReplyByReq.set(reqId, content);
      return delta;
    }

    return "";
  }

  function saveChatMapping(chatId: string, sessionId: string) {
    if (state.mappings[chatId] !== sessionId) {
      state.mappings[chatId] = sessionId;
      persistStateSoon("chat mapping");
    }
  }

  async function resolveSession(message: Hub53AIIncomingMessage): Promise<GatewaySession> {
    const desiredTitle = buildHubSessionTitle(message);
    if (isOpenClawSessionId(message.chatId)) {
      const session = await getSessionWithKnownHubTitle(message.chatId, message.conversationTitle);
      await input.callbacks.onSessionUpsert(session);
      return session;
    }

    const mappedSession = await getMappedSession(message.chatId);
    if (mappedSession) {
      const nextSession = await renamePlaceholderSessionIfNeeded(mappedSession, message, desiredTitle);
      await input.callbacks.onSessionUpsert(nextSession);
      return nextSession;
    }

    const restoredSession = await restoreLatestHubSession(message.chatId, message.userName || message.userId);
    if (restoredSession) {
      const nextSession = await renamePlaceholderSessionIfNeeded(restoredSession, message, desiredTitle);
      saveChatMapping(message.chatId, nextSession.id);
      await input.callbacks.onSessionUpsert(nextSession);
      return nextSession;
    }

    const session = await createSessionWithUniqueTitle(desiredTitle);
    saveChatMapping(message.chatId, session.id);
    await input.callbacks.onSessionUpsert(session);
    return session;
  }

  async function getSessionWithKnownHubTitle(sessionId: string, titleHint?: string): Promise<GatewaySession> {
    const session = await input.gateway.getSession(sessionId);
    const knownSessions = await listKnownSessions();
    const titleHintSession = applyHubTitleHint(session, titleHint);
    return mergeKnownHubSessions([titleHintSession], knownSessions)[0] ?? titleHintSession;
  }

  async function createSessionWithUniqueTitle(baseTitle: string): Promise<GatewaySession> {
    let lastDuplicateError: unknown;

    for (let attempt = 0; attempt < 6; attempt += 1) {
      const title = buildUniqueHubSessionTitle(baseTitle, attempt);
      try {
        return await input.gateway.createSession(title);
      } catch (error) {
        if (!isDuplicateSessionTitleError(error)) {
          throw error;
        }
        lastDuplicateError = error;
      }
    }

    const fallbackTitle = `${baseTitle} ${Date.now().toString(36)}`;
    try {
      return await input.gateway.createSession(fallbackTitle);
    } catch (error) {
      if (isDuplicateSessionTitleError(error) && lastDuplicateError) {
        throw lastDuplicateError;
      }
      throw error;
    }
  }

  async function renamePlaceholderSessionIfNeeded(
    session: GatewaySession,
    message: Hub53AIIncomingMessage,
    desiredTitle: string
  ): Promise<GatewaySession> {
    if (!isOldHubPlaceholderTitle(session.title, message.chatId)) {
      return session;
    }

    await input.gateway.controlSession(session.id, "rename", desiredTitle);
    return {
      ...session,
      title: desiredTitle,
      updatedAt: new Date().toISOString()
    };
  }

  async function sendReply(inputReply: {
    reqId: string;
    text: string;
    status: "streaming" | "thinking" | "done" | "error";
    sessionId?: string;
    error?: {
      code: string;
      message: string;
      details?: string;
    };
    mode?: string;
    replace?: boolean;
    eventKind?: string;
    payload?: Record<string, unknown>;
  }) {
    const frame = buildOutgoingChunk(
      inputReply.reqId,
      inputReply.text,
      inputReply.status,
      inputReply.error,
      inputReply.sessionId,
      {
        mode: inputReply.mode,
        replace: inputReply.replace,
        eventKind: inputReply.eventKind,
        payload: inputReply.payload
      }
    );
    traceOpenClawDuplicate(input.logger, "hub.reply.send", summarizeOutgoingFrameForTrace(frame), input.config);
    if (!sendRaw(JSON.stringify(frame), true)) {
      state.outbox.push(frame);
      state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
      await persistState();
    }
  }

  async function sendQueuedFrame(frame: Hub53AIQueuedFrame) {
    if (!sendRaw(JSON.stringify(frame), true)) {
      state.outbox.push(frame);
      state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
      await persistState();
    }
  }

  function augmentPayloadWithEventMeta(event: GatewayEvent, eventScope: GatewayEventScope) {
    const timeline = buildOpenClawTimelineMeta(event, eventScope);
    const payload = normalizeOpenClawAnswerPayloadForSegment(event, eventScope, timeline);
    const ledger = buildOpenClawLedgerEvent(event, eventScope, timeline);
    const runIdentity = identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id,
      eventScope.currentRunId
    );
    const payloadWithMeta = {
      ...payload,
      event_id: event.id,
      event_kind: event.kind,
      event_created_at: event.createdAt,
      ...(typeof event.seq === "number" ? { seq: event.seq, message_seq: payload.message_seq ?? event.seq } : {}),
      ...(runIdentity ? { runId: payload.runId ?? runIdentity, run_id: payload.run_id ?? runIdentity } : {}),
      turn_id: timeline.turn_id,
      segment_id: timeline.segment_id,
      segment_type: timeline.segment_type,
      segment_index: timeline.segment_index,
      delta_index: timeline.delta_index,
      operation: timeline.operation,
      visibility: timeline.visibility,
      final: timeline.final,
      openclaw_timeline: timeline,
      openclaw_ledger: ledger
    };
    const canonicalPayload = attachOpenClawLedgerToPayload(
      event,
      attachOpenClawTimelineToPayload(event, payloadWithMeta, timeline),
      ledger
    );
    if (eventScope.activeRequestId && eventScope.activeRequestId !== "events") {
      appendCanonicalSessionEvent(event.sessionId || ledger.session_id || "", {
        ...event,
        payload: canonicalPayload
      });
    }
    traceOpenClawLedger(input.logger, "ledger", summarizeOpenClawLedgerEvent(ledger), input.config);
    return canonicalPayload;
  }

  function normalizeOpenClawAnswerPayloadForSegment(
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): Record<string, unknown> {
    const payload = toRecord(event.payload);
    if (timeline.segment_type !== "answer" || typeof payload.content !== "string") {
      return payload;
    }

    const cacheKey = getTimelineEventCacheKey(event);
    if (eventScope.answerContentAppliedEventKeys.has(cacheKey)) {
      return payload;
    }
    eventScope.answerContentAppliedEventKeys.add(cacheKey);

    const originalContent = payload.content;
    const normalizedContent = normalizeCumulativeAnswerSnapshotContent(originalContent, event, eventScope, timeline);
    const nextPayload =
      normalizedContent === originalContent
        ? payload
        : {
            ...payload,
            content: normalizedContent,
            openclaw_normalized_cumulative_answer: true,
            openclaw_original_content_length: originalContent.length,
            openclaw_normalized_content_length: normalizedContent.length
          };
    if (nextPayload !== payload) {
      event.payload = nextPayload;
      traceOpenClawLedger(input.logger, "ledger", {
        event: summarizeTimelineEventForTrace(event),
        reason: "cumulative_answer_snapshot_segment_prefix",
        segmentId: timeline.segment_id,
        originalLength: originalContent.length,
        normalizedLength: normalizedContent.length,
        originalHash: hashTraceText(originalContent),
        normalizedHash: hashTraceText(normalizedContent)
      }, input.config);
    }

    rememberOpenClawAnswerSegmentContent(event, eventScope, timeline, normalizedContent);
    return nextPayload;
  }

  function normalizeCumulativeAnswerSnapshotContent(
    content: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): string {
    if (timeline.operation !== "replace" && !isReplyReplaceEvent(event)) {
      return content;
    }
    const answerIndex = getOpenClawAnswerSegmentIndex(timeline.segment_id);
    if (answerIndex <= 0) {
      return content;
    }
    const priorText = getPriorOpenClawAnswerSegmentsText(timeline.turn_id, answerIndex, eventScope);
    if (!priorText || !content.startsWith(priorText)) {
      return content;
    }
    return content.slice(priorText.length).replace(/^\s+/, "");
  }

  function rememberOpenClawAnswerSegmentContent(
    event: GatewayEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta,
    content: string
  ) {
    const existing = eventScope.answerSegmentTextById.get(timeline.segment_id) || "";
    const next =
      timeline.operation === "append"
        ? `${existing}${content}`
        : timeline.operation === "replace"
          ? content
          : existing || content;
    eventScope.answerSegmentTextById.set(timeline.segment_id, next);
    eventScope.answerSegmentVisibilityById.set(timeline.segment_id, timeline.visibility);
    eventScope.answerSegmentTrustedById.set(timeline.segment_id, isTrustedOpenClawAnswerEvent(event));

    const rawOrder = getOpenClawRawOrder(event);
    if (rawOrder) {
      eventScope.currentAnswerRawOrder = eventScope.currentAnswerRawOrder
        ? Math.max(eventScope.currentAnswerRawOrder, rawOrder)
        : rawOrder;
    }
  }

  function getPriorOpenClawAnswerSegmentsText(turnId: string, currentAnswerIndex: number, eventScope: GatewayEventScope): string {
    const texts: string[] = [];
    for (let index = 0; index < currentAnswerIndex; index += 1) {
      const text = eventScope.answerSegmentTextById.get(`${turnId}:answer:${index}`);
      if (text) {
        texts.push(text);
      }
    }
    return texts.join("");
  }

  function getOpenClawAnswerSegmentIndex(segmentId: string): number {
    const match = segmentId.match(/:answer:(\d+)(?::|$)/);
    if (!match) {
      return 0;
    }
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function getOpenClawRawOrder(event: TimelineEvent): number {
    const payload = toRecord(event.payload);
    return firstPositiveNumber(
      payload.rawSeq,
      payload.raw_seq,
      payload.messageSeq,
      payload.message_seq,
      event.seq
    );
  }

  function buildOpenClawTimelineTurnId(sessionId: string, reqId: string) {
    return `${sessionId || "openclaw"}:turn:${reqId || randomUUID()}`;
  }

  function normalizeTimelineEventsWithProtocol(sessionId: string, events: TimelineEvent[]): TimelineEvent[] {
    const eventScope: GatewayEventScope = {
      eventBoundaryMs: 0,
      turnId: buildOpenClawTimelineTurnId(sessionId, "events"),
      activeRequestId: "events",
      nextSegmentIndex: 0,
      nextAnswerSegmentIndex: 0,
      answerBoundaryAfterVisibleResponse: false,
      activityAppliedEventKeys: new Set<string>(),
      answerContentAppliedEventKeys: new Set<string>(),
      answerSegmentTextById: new Map<string, string>(),
      answerSegmentVisibilityById: new Map<string, OpenClawTimelineV2Visibility>(),
      answerSegmentTrustedById: new Map<string, boolean>(),
      nextDeltaIndexBySegment: new Map<string, number>(),
      segmentIndexById: new Map<string, number>(),
      timelineMetaByEventKey: new Map<string, OpenClawTimelineV2Meta>(),
      typedToolCallEnrichedKeys: new Set<string>(),
      currentActivitySeen: false,
      visibleResponseSeen: false,
      lastSeqSeen: 0,
      emittedOutputFileKeys: new Set<string>(),
      referencedLocalOutputPaths: new Set<string>(),
      writeOutputFilesByToolCallId: new Map<string, Hub53AIOutputFile[]>()
    };
    return events.map((event) => {
      if (!shouldAttachOpenClawTimeline(event)) {
        return event;
      }
      applyOpenClawEventScopeActivity(event, eventScope);
      return {
        ...event,
        payload: augmentPayloadWithEventMeta(event, eventScope)
      };
    });
  }

  function shouldAttachOpenClawTimeline(event: TimelineEvent): boolean {
    return Boolean(getExpectedOpenClawSegmentType(event));
  }

  function readOpenClawTimelineFromEvent(event: TimelineEvent): Record<string, unknown> {
    const payload = toRecord(event.payload);
    const direct = toRecord(payload.openclaw_timeline);
    if (Object.keys(direct).length > 0) {
      return direct;
    }
    const processStep = toRecord(payload.process_step);
    const processData = toRecord(processStep.data);
    return toRecord(processData.openclaw_timeline);
  }

  function attachOpenClawTimelineToPayload(
    event: TimelineEvent,
    payload: Record<string, unknown>,
    timeline: OpenClawTimelineV2Meta
  ): Record<string, unknown> {
    if (event.kind !== "process.step" || timeline.segment_type !== "output_files") {
      return payload;
    }

    const processStep = toRecord(payload.process_step);
    if (Object.keys(processStep).length === 0) {
      return payload;
    }
    const processData = toRecord(processStep.data);
    return {
      ...payload,
      process_step: {
        ...processStep,
        data: {
          ...processData,
          openclaw_timeline: timeline
        }
      }
    };
  }

  function attachOpenClawLedgerToPayload(
    event: TimelineEvent,
    payload: Record<string, unknown>,
    ledger: OpenClawLedgerEvent
  ): Record<string, unknown> {
    if (event.kind !== "process.step" || ledger.part_type !== "output_file") {
      return payload;
    }

    const processStep = toRecord(payload.process_step);
    if (Object.keys(processStep).length === 0) {
      return payload;
    }
    const processData = toRecord(processStep.data);
    return {
      ...payload,
      process_step: {
        ...processStep,
        data: {
          ...processData,
          openclaw_ledger: ledger
        }
      }
    };
  }

  function readOpenClawLedgerFromEvent(event: TimelineEvent): Record<string, unknown> {
    const payload = toRecord(event.payload);
    const direct = toRecord(payload.openclaw_ledger);
    if (Object.keys(direct).length > 0) {
      return direct;
    }
    const processStep = toRecord(payload.process_step);
    const processData = toRecord(processStep.data);
    return toRecord(processData.openclaw_ledger);
  }

  function buildOpenClawLedgerEvent(
    event: TimelineEvent,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): OpenClawLedgerEvent {
    const payload = toRecord(event.payload);
    const runIdentity = identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id,
      eventScope.currentRunId
    );
    const text = typeof payload.content === "string" ? payload.content : undefined;
    const terminalStatus = getOpenClawLedgerTerminalStatus(event);
    const eventType = getOpenClawLedgerEventType(event, timeline);
    return {
      protocol_version: "openclaw.ledger.v1",
      seq: getOpenClawLedgerSeq(event, timeline, eventType),
      session_id: event.sessionId || "",
      conversation_id: event.sessionId || "",
      turn_id: timeline.turn_id,
      ...(runIdentity ? { run_id: runIdentity } : {}),
      active_request_id:
        stringOr(payload.active_request_id) ||
        eventScope.activeRequestId ||
        stringOr(payload.req_id, payload.request_id) ||
        timeline.turn_id,
      part_id: getOpenClawLedgerPartId(timeline),
      part_type: getOpenClawLedgerPartType(timeline.segment_type),
      event_type: eventType,
      operation: eventType === "turn.started" ? "noop" : timeline.operation,
      visibility: timeline.visibility,
      ...(text != null ? { text } : {}),
      payload: buildOpenClawLedgerPayload(event),
      ...(terminalStatus ? { terminal_status: terminalStatus } : {}),
      created_at: event.createdAt || new Date().toISOString(),
      raw_event_ref: buildOpenClawRawEventRef(event)
    };
  }

  function buildOpenClawLedgerPayload(event: TimelineEvent): Record<string, unknown> {
    const payload = toRecord(event.payload);
    const rest = { ...payload };
    delete rest.openclaw_ledger;
    const sourceKind = stringOr(rest.source_kind) || event.kind;
    return {
      ...rest,
      source_kind: sourceKind,
      event_id: event.id,
      event_seq: event.seq
    };
  }

  function getOpenClawLedgerSeq(
    event: TimelineEvent,
    timeline: OpenClawTimelineV2Meta,
    eventType: OpenClawLedgerEventType
  ): number {
    const sessionId = event.sessionId || "";
    const rawEventRef = buildOpenClawRawEventRef(event);
    const existingSeq = findExistingOpenClawLedgerSeq(sessionId, rawEventRef, getOpenClawLedgerPartId(timeline), eventType);
    if (existingSeq) {
      return existingSeq;
    }
    return allocateNextOpenClawLedgerSeq(sessionId);
  }

  function findExistingOpenClawLedgerSeq(
    sessionId: string,
    rawEventRef: string,
    partId: string,
    eventType: OpenClawLedgerEventType
  ): number {
    for (const source of [canonicalEventsBySession, syntheticEventsBySession]) {
      for (const event of source.get(sessionId) ?? []) {
        const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
        if (!ledger) {
          continue;
        }
        if (ledger.raw_event_ref === rawEventRef && ledger.part_id === partId && ledger.event_type === eventType) {
          return ledger.seq;
        }
      }
    }
    return 0;
  }

  function allocateNextOpenClawLedgerSeq(sessionId: string): number {
    const key = sessionId || "openclaw";
    const current = ledgerSeqBySession.get(key) ?? computeMaxOpenClawLedgerSeqForSession(sessionId);
    const next = current + 1;
    ledgerSeqBySession.set(key, next);
    return next;
  }

  function rememberOpenClawLedgerSeq(sessionId: string, event: TimelineEvent) {
    const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
    if (!ledger) {
      return;
    }
    const key = sessionId || ledger.session_id || "openclaw";
    ledgerSeqBySession.set(key, Math.max(ledgerSeqBySession.get(key) ?? 0, ledger.seq));
  }

  function computeMaxOpenClawLedgerSeqForSession(sessionId: string): number {
    let maxSeq = 0;
    for (const source of [canonicalEventsBySession, syntheticEventsBySession]) {
      for (const event of source.get(sessionId) ?? []) {
        const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
        if (ledger) {
          maxSeq = Math.max(maxSeq, ledger.seq);
        }
      }
    }
    return maxSeq;
  }

  function getOpenClawLedgerPartId(timeline: OpenClawTimelineV2Meta): string {
    if (timeline.segment_type === "run") {
      return `${timeline.turn_id}:status`;
    }
    return timeline.segment_id;
  }

  function getOpenClawLedgerPartType(segmentType: OpenClawTimelineV2SegmentType): OpenClawLedgerPartType {
    if (segmentType === "answer") return "answer";
    if (segmentType === "thinking") return "thinking";
    if (segmentType === "tool_call" || segmentType === "tool_result") return "tool";
    if (segmentType === "output_files") return "output_file";
    return "status";
  }

  function getOpenClawLedgerEventType(
    event: TimelineEvent,
    timeline: OpenClawTimelineV2Meta
  ): OpenClawLedgerEventType {
    if (event.kind === "run.started") return "turn.started";
    if (event.kind === "run.completed") return "turn.completed";
    if (event.kind === "run.interrupted") return "turn.interrupted";
    if (event.kind === "run.failed") return "turn.failed";
    if (timeline.operation === "append") return "part.delta";
    if (timeline.operation === "close") return "part.done";
    return "part.replace";
  }

  function getOpenClawLedgerTerminalStatus(event: TimelineEvent): OpenClawLedgerTerminalStatus | undefined {
    if (event.kind === "run.started") return "running";
    if (event.kind === "run.completed") return "completed";
    if (event.kind === "run.interrupted") return "interrupted";
    if (event.kind === "run.failed") return "failed";
    return undefined;
  }

  function buildOpenClawRawEventRef(event: TimelineEvent): string {
    return [event.sessionId || "openclaw", event.seq, event.id || event.kind].filter((part) => part != null && part !== "").join(":");
  }

  function extractOpenClawLedgerEvents(events: TimelineEvent[]): OpenClawLedgerEvent[] {
    return events
      .map((event) => normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event)))
      .filter((event): event is OpenClawLedgerEvent => Boolean(event));
  }

  function normalizeOpenClawLedgerEvent(value: Record<string, unknown>): OpenClawLedgerEvent | null {
    if (value.protocol_version !== "openclaw.ledger.v1") {
      return null;
    }
    const seq = typeof value.seq === "number" ? value.seq : Number(value.seq);
    const sessionId = stringOr(value.session_id);
    const turnId = stringOr(value.turn_id);
    const activeRequestId = stringOr(value.active_request_id);
    const partId = stringOr(value.part_id);
    const partType = normalizeOpenClawLedgerPartType(stringOr(value.part_type));
    const eventType = normalizeOpenClawLedgerEventType(stringOr(value.event_type));
    const operation = normalizeOpenClawLedgerOperation(stringOr(value.operation));
    const visibility = normalizeOpenClawLedgerVisibility(stringOr(value.visibility));
    if (!Number.isFinite(seq) || !sessionId || !turnId || !activeRequestId || !partId || !partType || !eventType || !operation || !visibility) {
      return null;
    }
    const terminalStatus = normalizeOpenClawLedgerTerminalStatus(stringOr(value.terminal_status));
    return {
      protocol_version: "openclaw.ledger.v1",
      seq,
      session_id: sessionId,
      conversation_id: stringOr(value.conversation_id) || sessionId,
      turn_id: turnId,
      ...(stringOr(value.run_id) ? { run_id: stringOr(value.run_id) } : {}),
      active_request_id: activeRequestId,
      part_id: partId,
      part_type: partType,
      event_type: eventType,
      operation,
      visibility,
      ...(typeof value.text === "string" ? { text: value.text } : {}),
      ...(toRecord(value.payload) ? { payload: toRecord(value.payload) } : {}),
      ...(terminalStatus ? { terminal_status: terminalStatus } : {}),
      created_at: stringOr(value.created_at) || new Date().toISOString(),
      ...(stringOr(value.raw_event_ref) ? { raw_event_ref: stringOr(value.raw_event_ref) } : {})
    };
  }

  function normalizeOpenClawLedgerPartType(value: string): OpenClawLedgerPartType | undefined {
    if (value === "answer" || value === "thinking" || value === "tool" || value === "output_file" || value === "status") {
      return value;
    }
    return undefined;
  }

  function normalizeOpenClawLedgerEventType(value: string): OpenClawLedgerEventType | undefined {
    if (
      value === "turn.started" ||
      value === "part.delta" ||
      value === "part.replace" ||
      value === "part.done" ||
      value === "turn.completed" ||
      value === "turn.interrupted" ||
      value === "turn.failed"
    ) {
      return value;
    }
    return undefined;
  }

  function normalizeOpenClawLedgerOperation(value: string): OpenClawLedgerOperation | undefined {
    if (value === "append" || value === "replace" || value === "close" || value === "noop") return value;
    return undefined;
  }

  function normalizeOpenClawLedgerVisibility(value: string): OpenClawTimelineV2Visibility | undefined {
    if (value === "hidden" || value === "stream" || value === "final") return value;
    return undefined;
  }

  function normalizeOpenClawLedgerTerminalStatus(value: string): OpenClawLedgerTerminalStatus | undefined {
    if (value === "running" || value === "completed" || value === "interrupted" || value === "failed" || value === "cancelled") {
      return value;
    }
    return undefined;
  }

  function buildOpenClawSessionSnapshot(sessionId: string, inputLedgerEvents: OpenClawLedgerEvent[], afterSeq = 0): OpenClawSessionSnapshot {
    let ledgerEvents = inputLedgerEvents;
    let { turns, stats, lastSeq } = collectOpenClawLedgerTurnSnapshotState(ledgerEvents);
    const syntheticTerminals = synthesizeTerminalEventsForOrphanedRunningTurns(sessionId, [...turns.values()], stats, lastSeq);
    if (syntheticTerminals.length) {
      ledgerEvents = [...ledgerEvents, ...syntheticTerminals].sort((left, right) => left.seq - right.seq);
      ({ turns, lastSeq } = collectOpenClawLedgerTurnSnapshotState(ledgerEvents));
    }
    const activeTurns = selectOpenClawActiveTurnsForSnapshot(sessionId, [...turns.values()], lastSeq);
    const { recentEvents, ledgerEventsAfterSeq } = buildOpenClawSnapshotEventWindows(ledgerEvents, activeTurns, afterSeq);
    const snapshot = {
      session_id: sessionId,
      conversation_id: sessionId,
      last_seq: lastSeq,
      active_turns: activeTurns,
      recent_events: recentEvents,
      ledger_events: ledgerEventsAfterSeq,
      ledgerEvents: ledgerEventsAfterSeq
    };
    traceOpenClawLedger(input.logger, "snapshot.summary", summarizeOpenClawSessionSnapshotForTrace({
      sessionId,
      afterSeq,
      snapshot,
      turns: [...turns.values()],
      syntheticTerminals,
      tracked: getTrackedActiveOpenClawTurns(sessionId)
    }), input.config);
    return snapshot;
  }

  function buildOpenClawSnapshotEventWindows(
    ledgerEvents: OpenClawLedgerEvent[],
    activeTurns: OpenClawLedgerTurnSnapshot[],
    afterSeq = 0
  ): { recentEvents: OpenClawLedgerEvent[]; ledgerEventsAfterSeq: OpenClawLedgerEvent[] } {
    const activeTurnIds = new Set(activeTurns.map((turn) => turn.turn_id).filter(Boolean));
    const activeRunIds = new Set(activeTurns.map((turn) => turn.run_id).filter((runId): runId is string => Boolean(runId)));
    const activeEvents = ledgerEvents.filter((event) => {
      return activeTurnIds.has(event.turn_id) || (event.run_id ? activeRunIds.has(event.run_id) : false);
    });
    const eventsAfterSeq = ledgerEvents.filter((event) => event.seq > afterSeq);
    const isIncrementalPoll = afterSeq > 0;
    const recoveryEvents = isIncrementalPoll
      ? eventsAfterSeq
      : ledgerEvents.slice(-MAX_CANONICAL_EVENTS_PER_SNAPSHOT);

    return {
      recentEvents: capOpenClawSnapshotLedgerEvents([...(isIncrementalPoll ? [] : activeEvents), ...recoveryEvents]),
      ledgerEventsAfterSeq: capOpenClawSnapshotLedgerEvents(eventsAfterSeq)
    };
  }

  function capOpenClawSnapshotLedgerEvents(events: OpenClawLedgerEvent[]): OpenClawLedgerEvent[] {
    if (events.length === 0) {
      return [];
    }
    const byKey = new Map<string, OpenClawLedgerEvent>();
    for (const event of events) {
      byKey.set(getOpenClawLedgerEventObjectRef(event), event);
    }
    const sorted = [...byKey.values()].sort((left, right) => left.seq - right.seq);
    return sorted.length > MAX_CANONICAL_EVENTS_PER_SNAPSHOT
      ? sorted.slice(-MAX_CANONICAL_EVENTS_PER_SNAPSHOT)
      : sorted;
  }

  function collectOpenClawLedgerTurnSnapshotState(ledgerEvents: OpenClawLedgerEvent[]): {
    turns: Map<string, OpenClawLedgerTurnSnapshot>;
    stats: Map<string, OpenClawLedgerTurnStats>;
    lastSeq: number;
  } {
    const turns = new Map<string, OpenClawLedgerTurnSnapshot>();
    const stats = new Map<string, OpenClawLedgerTurnStats>();
    for (const event of ledgerEvents) {
      const existing = turns.get(event.turn_id) || {
        turn_id: event.turn_id,
        run_id: event.run_id,
        active_request_id: event.active_request_id,
        status: "running" as OpenClawLedgerTerminalStatus,
        last_seq: 0,
        part_ids: []
      };
      if (event.run_id && !existing.run_id) {
        existing.run_id = event.run_id;
      }
      existing.active_request_id = event.active_request_id || existing.active_request_id;
      existing.last_seq = Math.max(existing.last_seq, event.seq);
      if (!existing.part_ids.includes(event.part_id)) {
        existing.part_ids.push(event.part_id);
      }
      if (event.terminal_status && event.terminal_status !== "running" && existing.status === "running") {
        existing.status = event.terminal_status;
        existing.terminal_seq = event.seq;
      } else if (event.terminal_status === "running" && existing.status === "running") {
        existing.status = event.terminal_status;
      }
      turns.set(event.turn_id, existing);

      const currentStats = stats.get(event.turn_id) || {
        last_created_at_ms: 0,
        has_visible_part: false
      };
      currentStats.last_created_at_ms = Math.max(
        currentStats.last_created_at_ms,
        getOpenClawLedgerCreatedAtMs(event)
      );
      if (isOpenClawLedgerVisiblePart(event)) {
        currentStats.has_visible_part = true;
      }
      stats.set(event.turn_id, currentStats);
    }
    const lastSeq = ledgerEvents.reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
    return { turns, stats, lastSeq };
  }

  function synthesizeTerminalEventsForOrphanedRunningTurns(
    sessionId: string,
    turns: OpenClawLedgerTurnSnapshot[],
    stats: Map<string, OpenClawLedgerTurnStats>,
    lastSeq: number
  ): OpenClawLedgerEvent[] {
    const tracked = getTrackedActiveOpenClawTurns(sessionId);
    let nextSeq = Math.max(lastSeq, getMaxOpenClawStoredSessionEventSeq(sessionId));
    const syntheticEvents: OpenClawLedgerEvent[] = [];
    for (const turn of turns) {
      if (!shouldSynthesizeTerminalForOrphanedRunningTurn(turn, stats.get(turn.turn_id), tracked)) {
        continue;
      }
      nextSeq += 1;
      syntheticEvents.push(appendSyntheticTerminalLedgerEventForOrphanedTurn(sessionId, turn, nextSeq));
    }
    return syntheticEvents;
  }

  function shouldSynthesizeTerminalForOrphanedRunningTurn(
    turn: OpenClawLedgerTurnSnapshot,
    stats: OpenClawLedgerTurnStats | undefined,
    tracked: TrackedActiveOpenClawTurns
  ): boolean {
    if (turn.status !== "running") {
      return false;
    }
    if (String(turn.active_request_id || "").startsWith("history:")) {
      return false;
    }
    if (tracked.turnIds.has(turn.turn_id) || (turn.run_id ? tracked.runIds.has(turn.run_id) : false)) {
      return false;
    }
    if (!stats?.has_visible_part) {
      return false;
    }
    const lastCreatedAtMs = stats.last_created_at_ms;
    if (!lastCreatedAtMs || Date.now() - lastCreatedAtMs < OPENCLAW_ORPHAN_RUNNING_TURN_TERMINAL_MS) {
      return false;
    }
    return true;
  }

  function appendSyntheticTerminalLedgerEventForOrphanedTurn(
    sessionId: string,
    turn: OpenClawLedgerTurnSnapshot,
    seq: number
  ): OpenClawLedgerEvent {
    const createdAt = new Date().toISOString();
    const reason = "orphaned_running_turn_after_host_restart";
    const rawEventRef = `synthetic:${reason}:${turn.turn_id}:${seq}`;
    const ledger: OpenClawLedgerEvent = {
      protocol_version: "openclaw.ledger.v1",
      seq,
      session_id: sessionId,
      conversation_id: sessionId,
      turn_id: turn.turn_id,
      ...(turn.run_id ? { run_id: turn.run_id } : {}),
      active_request_id: turn.active_request_id,
      part_id: `${turn.turn_id}:status`,
      part_type: "status",
      event_type: "turn.interrupted",
      operation: "close",
      visibility: "hidden",
      terminal_status: "interrupted",
      created_at: createdAt,
      raw_event_ref: rawEventRef,
      payload: {
        synthetic: true,
        synthetic_terminal: true,
        synthetic_reason: reason,
        terminal_status: "interrupted"
      }
    };
    const event: TimelineEvent = {
      id: `${sessionId}:synthetic:${reason}:${seq}`,
      sessionId,
      seq,
      kind: "run.interrupted",
      payload: {
        synthetic: true,
        synthetic_terminal: true,
        synthetic_reason: reason,
        terminal_status: "interrupted",
        active_request_id: turn.active_request_id,
        turn_id: turn.turn_id,
        ...(turn.run_id ? { runId: turn.run_id, run_id: turn.run_id } : {}),
        openclaw_ledger: ledger
      },
      createdAt
    };
    appendSyntheticSessionEvent(sessionId, event);
    traceOpenClawLedger(input.logger, "synthetic-terminal", {
      sessionId,
      reason,
      turnId: turn.turn_id,
      runId: turn.run_id,
      activeRequestId: turn.active_request_id,
      ledger: summarizeOpenClawLedgerEvent(ledger)
    }, input.config);
    return ledger;
  }

  function getOpenClawLedgerCreatedAtMs(event: OpenClawLedgerEvent): number {
    const time = Date.parse(event.created_at);
    return Number.isFinite(time) ? time : 0;
  }

  function isOpenClawLedgerVisiblePart(event: OpenClawLedgerEvent): boolean {
    return event.part_type === "answer" || event.part_type === "thinking" || event.part_type === "tool" || event.part_type === "output_file";
  }

  function getMaxOpenClawStoredSessionEventSeq(sessionId: string): number {
    return listCanonicalSessionEvents(sessionId).reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
  }

  function selectOpenClawActiveTurnsForSnapshot(
    sessionId: string,
    turns: OpenClawLedgerTurnSnapshot[],
    lastSeq: number
  ): OpenClawLedgerTurnSnapshot[] {
    const terminalRunIds = new Set(
      turns
        .filter((turn) => turn.status !== "running" && turn.run_id)
        .map((turn) => String(turn.run_id))
    );
    const runningTurns = turns.filter((turn) => {
      if (turn.status !== "running") {
        return false;
      }
      if (
        turn.run_id &&
        terminalRunIds.has(turn.run_id) &&
        String(turn.active_request_id || "").startsWith("history:")
      ) {
        traceOpenClawLedger(input.logger, "stale-history-active-turn", {
          sessionId,
          turnId: turn.turn_id,
          runId: turn.run_id,
          activeRequestId: turn.active_request_id,
          lastSeq: turn.last_seq
        }, input.config);
        return false;
      }
      return true;
    });
    if (!runningTurns.length) {
      return [];
    }

    const tracked = getTrackedActiveOpenClawTurns(sessionId);
    if (tracked.turnIds.size || tracked.runIds.size) {
      return runningTurns
        .filter((turn) => tracked.turnIds.has(turn.turn_id) || (turn.run_id ? tracked.runIds.has(turn.run_id) : false))
        .map((turn) => ({
          ...turn,
          active_request_id:
            tracked.requestIdByTurnId.get(turn.turn_id) ||
            (turn.run_id ? tracked.requestIdByRunId.get(turn.run_id) : "") ||
            turn.active_request_id
        }));
    }

    const latestRunningSeq = runningTurns.reduce((maxSeq, turn) => Math.max(maxSeq, turn.last_seq || 0), 0);
    if (latestRunningSeq <= 0 || latestRunningSeq !== lastSeq) {
      return [];
    }
    return runningTurns.filter((turn) => turn.last_seq === latestRunningSeq);
  }

  function getTrackedActiveOpenClawTurns(sessionId: string): TrackedActiveOpenClawTurns {
    const tracked: TrackedActiveOpenClawTurns = {
      turnIds: new Set<string>(),
      runIds: new Set<string>(),
      requestIdByTurnId: new Map<string, string>(),
      requestIdByRunId: new Map<string, string>()
    };

    for (const [reqId, details] of activeRequestDetailsBySession.get(sessionId) ?? []) {
      const scope = details.eventScope;
      const turnIds = [
        scope.currentTurnId,
        scope.turnId,
        scope.currentRunId ? buildOpenClawTimelineTurnId(sessionId, scope.currentRunId) : "",
        buildOpenClawTimelineTurnId(sessionId, reqId)
      ].filter((turnId): turnId is string => Boolean(turnId));

      for (const turnId of turnIds) {
        tracked.turnIds.add(turnId);
        tracked.requestIdByTurnId.set(turnId, reqId);
      }

      if (scope.currentRunId) {
        tracked.runIds.add(scope.currentRunId);
        tracked.requestIdByRunId.set(scope.currentRunId, reqId);
      }
    }

    return tracked;
  }

  function getTimelineEventCacheKey(event: TimelineEvent): string {
    return event.id || `${event.sessionId}:${event.seq}:${event.kind}`;
  }

  function resolveOpenClawTimelineTurnId(
    event: TimelineEvent,
    eventScope: GatewayEventScope,
    payloadTimeline: Record<string, unknown>
  ): string {
    const payload = toRecord(event.payload);
    const runIdentity = identityStringOr(
      payload.runId,
      payload.run_id,
      payload.responseId,
      payload.response_id
    );
    const explicitTurnId = stringOr(payloadTimeline.turn_id, payload.turn_id);
    if (eventScope.activeRequestId && eventScope.activeRequestId !== "events") {
      if (runIdentity) {
        eventScope.currentRunId = runIdentity;
      }
      const turnId = eventScope.currentTurnId || eventScope.turnId || explicitTurnId;
      eventScope.currentTurnId = turnId;
      return turnId;
    }

    if (explicitTurnId) {
      if (runIdentity) {
        eventScope.currentRunId = runIdentity;
      }
      eventScope.currentTurnId = explicitTurnId;
      return explicitTurnId;
    }

    if (runIdentity) {
      eventScope.currentRunId = runIdentity;
      const turnId = buildOpenClawTimelineTurnId(event.sessionId || "", runIdentity);
      eventScope.currentTurnId = turnId;
      return turnId;
    }

    if (eventScope.currentTurnId) {
      return eventScope.currentTurnId;
    }

    const historyIdentity = identityStringOr(
      payload.messageSeq,
      payload.message_seq,
      payload.rawSeq,
      payload.raw_seq,
      readHistoryMessageSeq(event)
    );
    const turnId = historyIdentity
      ? buildOpenClawTimelineTurnId(event.sessionId || "", historyIdentity)
      : eventScope.currentTurnId || eventScope.turnId;
    eventScope.currentTurnId = turnId;
    return turnId;
  }

  function buildOpenClawTimelineMeta(
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): OpenClawTimelineV2Meta {
    const cacheKey = getTimelineEventCacheKey(event);
    const cached = eventScope.timelineMetaByEventKey.get(cacheKey);
    if (cached) {
      return cached;
    }

    const payload = toRecord(event.payload);
    const payloadTimeline = readOpenClawTimelineFromEvent(event);
    const turnId = resolveOpenClawTimelineTurnId(event, eventScope, payloadTimeline);
    const payloadSegmentId = stringOr(payloadTimeline.segment_id, payload.segment_id);
    const payloadSegmentType = stringOr(payloadTimeline.segment_type, payload.segment_type);
    const segmentType = normalizeOpenClawSegmentType(payloadSegmentType) || getDefaultOpenClawSegmentType(event);
    const segmentId =
      segmentType === "answer"
        ? getDefaultOpenClawSegmentId(event, turnId, segmentType, eventScope)
        : payloadSegmentId || getDefaultOpenClawSegmentId(event, turnId, segmentType, eventScope);
    const segmentIndex = getOpenClawSegmentIndex(segmentId, eventScope);
    const deltaIndex = getNextOpenClawDeltaIndex(segmentId, eventScope);
    const operation = getDefaultOpenClawOperation(event);
    const visibility = getDefaultOpenClawVisibility(event);
    const timeline: OpenClawTimelineV2Meta = {
      protocol_version: "openclaw.timeline.v2",
      turn_id: turnId,
      segment_id: segmentId,
      segment_type: segmentType,
      segment_index: segmentIndex,
      delta_index: deltaIndex,
      operation,
      visibility,
      final: visibility === "final"
    };
    eventScope.timelineMetaByEventKey.set(cacheKey, timeline);
    return timeline;
  }

  function buildOpenClawOutputFilesTimelineMeta(
    eventScope: GatewayEventScope,
    files: Hub53AIOutputFile[]
  ): OpenClawTimelineV2Meta {
    const fileKey = files
      .map((file) => getOutputFilePartIdentityKey(file))
      .filter(Boolean)
      .sort()
      .join(",");
    const turnId = eventScope.currentTurnId || eventScope.turnId;
    const segmentId = `${turnId}:output_files:${fileKey || "generated"}`;
    return {
      protocol_version: "openclaw.timeline.v2",
      turn_id: turnId,
      segment_id: segmentId,
      segment_type: "output_files",
      segment_index: getOpenClawSegmentIndex(segmentId, eventScope),
      delta_index: getNextOpenClawDeltaIndex(segmentId, eventScope),
      operation: "replace",
      visibility: "final",
      final: true
    };
  }

  function normalizeOpenClawSegmentType(value: string): OpenClawTimelineV2SegmentType | undefined {
    if (
      value === "answer" ||
      value === "thinking" ||
      value === "tool_call" ||
      value === "tool_result" ||
      value === "run" ||
      value === "output_files"
    ) {
      return value;
    }
    return undefined;
  }

  function getExpectedOpenClawSegmentType(event: TimelineEvent): OpenClawTimelineV2SegmentType | undefined {
    const kind = event.kind;
    if (kind === "assistant.delta" || kind === "assistant.message") return "answer";
    if (kind === "assistant.thinking") return "thinking";
    if (kind === "tool.call") return "tool_call";
    if (kind === "tool.result") return "tool_result";
    if (kind === "run.started" || kind === "run.completed" || kind === "run.failed" || kind === "run.interrupted") return "run";
    if (kind === "process.step" && isOutputFilesProcessStepEvent(event)) return "output_files";
    return undefined;
  }

  function isOutputFilesProcessStepEvent(event: TimelineEvent): boolean {
    if (event.kind !== "process.step") {
      return false;
    }
    const payload = toRecord(event.payload);
    const processStep = toRecord(payload.process_step);
    return processStep.step_code === "output_files" && processStep.status === "completed";
  }

  function normalizeTimelineEventSegmentType(event: TimelineEvent): TimelineEvent {
    const expectedSegmentType = getExpectedOpenClawSegmentType(event);
    if (!expectedSegmentType) {
      return event;
    }

    const payload = toRecord(event.payload);
    const timeline = readOpenClawTimelineFromEvent(event);
    const currentSegmentType = normalizeOpenClawSegmentType(stringOr(timeline.segment_type, payload.segment_type));
    if (currentSegmentType === expectedSegmentType) {
      return event;
    }

    return {
      ...event,
      payload: {
        ...payload,
        segment_type: expectedSegmentType,
        ...(Object.keys(timeline).length > 0
          ? {
              openclaw_timeline: {
                ...timeline,
                segment_type: expectedSegmentType
              }
            }
          : {})
      }
    };
  }

  function readHistoryMessageSeq(event: TimelineEvent): number {
    const match = String(event.id || "").match(/:history:(\d+)(?::|$)/);
    if (!match) return 0;
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function normalizeTimelineEventMessageSeq(event: TimelineEvent): TimelineEvent {
    const historyMessageSeq = readHistoryMessageSeq(event);
    if (!historyMessageSeq) {
      return event;
    }

    const payload = toRecord(event.payload);
    return {
      ...event,
      payload: {
        ...payload,
        messageSeq: payload.messageSeq ?? historyMessageSeq,
        message_seq: payload.message_seq ?? historyMessageSeq
      }
    };
  }

  function readTimelinePayloadSeq(event: TimelineEvent, key: string): number {
    const payload = toRecord(event.payload);
    const value = payload[key];
    const parsed = typeof value === "number" ? value : Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function normalizeThinkingContentForDedupe(event: TimelineEvent): string {
    if (event.kind !== "assistant.thinking") {
      return "";
    }
    return String(event.payload?.content ?? "").replace(/\s+/g, " ").trim();
  }

  function filterSupersededHistoryThinkingEvents(events: TimelineEvent[]): TimelineEvent[] {
    const canonicalHistoryThinking = new Set<string>();

    for (const event of events) {
      if (event.kind !== "assistant.thinking") continue;
      const historyMessageSeq = readHistoryMessageSeq(event);
      const content = normalizeThinkingContentForDedupe(event);
      if (!historyMessageSeq || !content) continue;
      canonicalHistoryThinking.add(`${event.sessionId}:${historyMessageSeq}:${content}`);
    }

    if (canonicalHistoryThinking.size === 0) {
      return events;
    }

    return events.filter((event) => {
      if (event.kind !== "assistant.thinking") return true;
      if (readHistoryMessageSeq(event)) return true;

      const content = normalizeThinkingContentForDedupe(event);
      if (!content) return true;

      const messageSeq =
        readTimelinePayloadSeq(event, "messageSeq") ||
        readTimelinePayloadSeq(event, "message_seq") ||
        readTimelinePayloadSeq(event, "rawSeq");
      if (!messageSeq) return true;

      return !canonicalHistoryThinking.has(`${event.sessionId}:${messageSeq}:${content}`);
    });
  }

  function getDefaultOpenClawSegmentType(event: TimelineEvent): OpenClawTimelineV2SegmentType {
    const kind = event.kind;
    if (kind === "assistant.delta" || kind === "assistant.message") return "answer";
    if (kind === "assistant.thinking") return "thinking";
    if (kind === "tool.call") return "tool_call";
    if (kind === "tool.result") return "tool_result";
    if (kind === "process.step" && isOutputFilesProcessStepEvent(event)) return "output_files";
    return "run";
  }

  function getDefaultOpenClawSegmentId(
    event: GatewayEvent,
    turnId: string,
    segmentType: OpenClawTimelineV2SegmentType,
    eventScope: GatewayEventScope
  ) {
    if (segmentType === "answer") {
      return resolveOpenClawAnswerSegmentId(turnId, eventScope);
    }
    if (segmentType === "output_files") {
      const fileKey = extractOutputFilesFromPayload(event.payload)
        .map((file) => getOutputFilePartIdentityKey(file))
        .filter(Boolean)
        .sort()
        .join(",");
      return `${turnId}:output_files:${fileKey || event.seq || event.id || "generated"}`;
    }
    if (segmentType === "tool_call" || segmentType === "tool_result") {
      const payload = toRecord(event.payload);
      const data = toRecord(payload.data);
      const toolIdentity = stringOr(
        data.toolCallId,
        data.tool_call_id,
        data.callId,
        data.call_id,
        data.id,
        payload.toolCallId,
        payload.tool_call_id,
        payload.callId,
        payload.call_id,
        payload.id
      );
      if (toolIdentity) {
        return `${turnId}:${segmentType}:${toolIdentity}`;
      }
      const toolName = stringOr(data.name, payload.name, data.toolName, payload.toolName, "tool");
      const eventIdentity = identityStringOr(
        data.rawSeq,
        data.raw_seq,
        payload.rawSeq,
        payload.raw_seq,
        data.seq,
        payload.seq,
        event.seq,
        event.id
      );
      return `${turnId}:${segmentType}:${toolName}:${eventIdentity || randomUUID()}`;
    }
    return `${turnId}:${segmentType}:${event.seq ?? event.id ?? randomUUID()}`;
  }

  function resolveOpenClawAnswerSegmentId(turnId: string, eventScope: GatewayEventScope): string {
    eventScope.currentAnswerSegmentId = `${turnId}:answer:0`;
    eventScope.nextAnswerSegmentIndex = Math.max(eventScope.nextAnswerSegmentIndex, 1);
    eventScope.answerBoundaryAfterVisibleResponse = false;
    return eventScope.currentAnswerSegmentId;
  }

  function getDefaultOpenClawOperation(event: GatewayEvent): OpenClawTimelineV2Operation {
    if (event.kind === "assistant.delta") {
      return isReplyReplaceEvent(event) ? "replace" : "append";
    }
    if (event.kind === "run.completed" || event.kind === "run.failed" || event.kind === "run.interrupted") return "close";
    return "replace";
  }

  function getDefaultOpenClawVisibility(event: GatewayEvent): OpenClawTimelineV2Visibility {
    if (isUntrustedRawOpenClawAnswerEvent(event)) return "hidden";
    if (event.kind === "assistant.delta") return "stream";
    return "final";
  }

  function getOpenClawSegmentIndex(segmentId: string, eventScope: GatewayEventScope) {
    const existing = eventScope.segmentIndexById.get(segmentId);
    if (typeof existing === "number") {
      return existing;
    }
    const next = eventScope.nextSegmentIndex;
    eventScope.segmentIndexById.set(segmentId, next);
    eventScope.nextSegmentIndex += 1;
    return next;
  }

  function getNextOpenClawDeltaIndex(segmentId: string, eventScope: GatewayEventScope) {
    const next = eventScope.nextDeltaIndexBySegment.get(segmentId) ?? 0;
    eventScope.nextDeltaIndexBySegment.set(segmentId, next + 1);
    return next;
  }

  async function sendOutputFilesForEvent(
    reqId: string,
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ) {
    const files = extractOutputFilesFromPayload(event.payload);
    const timeline =
      getExpectedOpenClawSegmentType(event) === "output_files"
        ? buildOpenClawTimelineMeta(event, eventScope)
        : undefined;
    await sendOutputFiles(reqId, sessionId, files, eventScope, timeline, event);
  }

  function rememberWriteToolOutputFileCandidate(event: GatewayEvent, eventScope: GatewayEventScope) {
    if (event.kind !== "tool.call") {
      return;
    }
    const toolCallId = getOpenClawToolCallId(event);
    if (!toolCallId) {
      return;
    }
    const files = extractWriteToolOutputFiles(event);
    if (files.length === 0) {
      return;
    }
    eventScope.writeOutputFilesByToolCallId.set(toolCallId, files);
  }

  async function sendWriteToolOutputFilesForEvent(
    reqId: string,
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ) {
    if (!isSuccessfulWriteToolResultEvent(event)) {
      return;
    }
    const files = consumeWriteToolOutputFiles(event, eventScope);
    if (files.length === 0) {
      return;
    }
    await sendOutputFiles(reqId, sessionId, files, eventScope);
  }

  function appendWriteToolOutputFilesForHistoryEvent(
    sessionId: string,
    event: GatewayEvent,
    eventScope: GatewayEventScope
  ): boolean {
    if (!isSuccessfulWriteToolResultEvent(event)) {
      return false;
    }
    const files = consumeWriteToolOutputFiles(event, eventScope);
    if (files.length === 0) {
      return false;
    }
    return appendCanonicalOutputFilesEvent(sessionId, files, eventScope, event);
  }

  function consumeWriteToolOutputFiles(event: GatewayEvent, eventScope: GatewayEventScope): Hub53AIOutputFile[] {
    const toolCallId = getOpenClawToolCallId(event);
    if (!toolCallId) {
      return [];
    }
    const files = eventScope.writeOutputFilesByToolCallId.get(toolCallId) ?? [];
    eventScope.writeOutputFilesByToolCallId.delete(toolCallId);
    return files;
  }

  function extractWriteToolOutputFiles(event: GatewayEvent): Hub53AIOutputFile[] {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const args = toRecord(data.args);
    const toolName = stringOr(data.name, data.toolName, payload.name, payload.toolName).toLowerCase();
    if (toolName !== "write") {
      return [];
    }
    const path = stringOr(args.path, args.file_path, args.filePath, args.filename, args.fileName);
    const content = typeof args.content === "string" ? args.content : "";
    if (!path || !content) {
      return [];
    }
    return [
      {
        id: `local:${path}`,
        file_name: basename(path),
        mime_type: "text/plain",
        size: Buffer.byteLength(content, "utf8"),
        content,
        source_kind: "tool.write"
      }
    ];
  }

  function isSuccessfulWriteToolResultEvent(event: GatewayEvent): boolean {
    if (event.kind !== "tool.result") {
      return false;
    }
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    const toolName = stringOr(data.name, data.toolName, payload.name, payload.toolName).toLowerCase();
    if (toolName !== "write") {
      return false;
    }
    if (data.isError === true || data.error || payload.error) {
      return false;
    }
    const phase = stringOr(data.phase, payload.phase).toLowerCase();
    return !phase || phase === "result" || phase === "completed" || phase === "done";
  }

  function getOpenClawToolCallId(event: TimelineEvent): string {
    const payload = toRecord(event.payload);
    const data = toRecord(payload.data);
    return stringOr(
      data.toolCallId,
      data.tool_call_id,
      data.callId,
      data.call_id,
      data.id,
      payload.toolCallId,
      payload.tool_call_id,
      payload.callId,
      payload.call_id,
      payload.id
    );
  }

  async function sendCreatedLocalOutputFiles(
    reqId: string,
    sessionId: string,
    eventScope: GatewayEventScope
  ) {
    const localOutputRuntime = {
      config: input.config,
      configPath: input.configPath,
      stateDir: input.stateDir,
      logger: input.logger
    };
    const manifestFiles = await collectManifestLocalOutputFiles({
      ...localOutputRuntime,
      manifestPath: eventScope.outputManifestPath,
      conversationId: sessionId,
      turnId: eventScope.turnId,
      activeRequestId: eventScope.activeRequestId
    });
    if (manifestFiles.length > 0) {
      input.logger?.info?.(
        `[53aihub] local output manifest: files=${manifestFiles.length}, path=${eventScope.outputManifestPath ?? "none"}, files=${
          manifestFiles.map((file) => file.file_name).join(",") || "none"
        }`
      );
      const sent = await sendOutputFiles(reqId, sessionId, manifestFiles, eventScope);
      if (sent) {
        eventScope.localOutputFilesSent = true;
      }
      return;
    }

    if (input.config.detectCreatedFiles !== true) {
      input.logger?.info?.(
        `[53aihub] local output manifest empty and legacy created-file scan disabled: path=${eventScope.outputManifestPath ?? "none"}`
      );
      return;
    }

    const files = await collectCreatedLocalOutputFiles(eventScope.localOutputSnapshot, localOutputRuntime);
    const referencedFiles = await collectReferencedLocalOutputFiles(
      eventScope.referencedLocalOutputPaths,
      eventScope.localOutputSnapshot,
      localOutputRuntime
    );
    const recentReferencedPaths = [...eventScope.referencedLocalOutputPaths].filter(
      (path) => !eventScope.localOutputSnapshot?.files.has(path)
    );
    const recentReferencedFiles = await collectRecentReferencedLocalOutputFiles(
      recentReferencedPaths,
      localOutputRuntime,
      eventScope.eventBoundaryMs
    );
    const outputFiles = [...files, ...referencedFiles, ...recentReferencedFiles];
    input.logger?.info?.(
      `[53aihub] local output scan: changed=${files.length}, referenced=${referencedFiles.length}, recentReferenced=${recentReferencedFiles.length}, refs=${eventScope.referencedLocalOutputPaths.size}, files=${
        outputFiles.map((file) => file.file_name).join(",") || "none"
      }`
    );
    const sent = await sendOutputFiles(reqId, sessionId, outputFiles, eventScope);
    if (sent) {
      eventScope.localOutputFilesSent = true;
    }
  }

  async function sendOutputFiles(
    reqId: string,
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    timeline?: OpenClawTimelineV2Meta,
    sourceEvent?: GatewayEvent
  ): Promise<boolean> {
    if (files.length === 0) {
      return false;
    }

    const freshFiles = files.filter((file) => {
      const keys = getOutputFileEmissionKeys(file);
      if (keys.some((key) => eventScope.emittedOutputFileKeys.has(key))) {
        return false;
      }
      for (const key of keys) {
        eventScope.emittedOutputFileKeys.add(key);
      }
      return true;
    });
    if (freshFiles.length === 0) {
      return false;
    }

    const outputTimeline = timeline ?? buildOpenClawOutputFilesTimelineMeta(eventScope, freshFiles);
    const hubFiles = await uploadOutputFilesToHub(reqId, sessionId, freshFiles, eventScope, outputTimeline);
    const ledgerSourceEvent = await buildOutputFilesLedgerSourceEvent(
      sessionId,
      hubFiles,
      eventScope,
      outputTimeline,
      sourceEvent
    );
    const outputLedger = buildOpenClawLedgerEvent(ledgerSourceEvent, eventScope, outputTimeline);
    if (eventScope.activeRequestId && eventScope.activeRequestId !== "events") {
      appendCanonicalSessionEvent(sessionId, buildCanonicalOutputFilesTimelineEvent(ledgerSourceEvent, outputTimeline, outputLedger));
    }
    await sendQueuedFrame(
      buildOutputFilesProcessStep(
        reqId,
        hubFiles,
        sessionId,
        outputTimeline,
        outputLedger
      )
    );
    return true;
  }

  async function uploadOutputFilesToHub(
    reqId: string,
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): Promise<Hub53AIOutputFile[]> {
    const uploaded: Hub53AIOutputFile[] = [];
    for (const file of files) {
      uploaded.push(await uploadSingleOutputFileToHub(reqId, sessionId, file, eventScope, timeline));
    }
    return uploaded;
  }

  async function uploadSingleOutputFileToHub(
    reqId: string,
    sessionId: string,
    file: Hub53AIOutputFile,
    eventScope: GatewayEventScope,
    timeline: OpenClawTimelineV2Meta
  ): Promise<Hub53AIOutputFile> {
    if (file.artifact_id || (!file.base64 && !file.content && !file.path)) {
      return stripLocalOnlyOutputFileFields(file);
    }
    try {
      const bytes = await readOutputFileBytes(file);
      if (!bytes.length) {
        return stripLocalOnlyOutputFileFields(file);
      }
      const form = new FormData();
      form.append("agent_id", input.config.botId);
      form.append("user_id", eventScope.hubUserId ?? "");
      form.append("conversation_id", sessionId);
      form.append("turn_id", timeline.turn_id);
      form.append("active_request_id", eventScope.activeRequestId || reqId);
      form.append("part_id", timeline.segment_id);
      form.append("logical_path", file.file_name || file.id || "output");
      form.append(
        "file",
        new Blob([bufferToArrayBuffer(bytes)], { type: file.mime_type || "application/octet-stream" }),
        sanitizeDownloadFileName(file.file_name || basename(file.path || "") || "output")
      );

      const abortController = new AbortController();
      const timeout = setTimeout(
        () => abortController.abort(),
        getArtifactUploadTimeoutMs(bytes.length)
      );
      let response: Response;
      try {
        response = await fetch(new URL("/api/v1/openclaw/artifacts", getHubHTTPBaseURL()).toString(), {
          method: "POST",
          headers: buildHubAuthHeaders(),
          body: form,
          signal: abortController.signal
        });
      } finally {
        clearTimeout(timeout);
      }
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const raw = (await response.json()) as Record<string, unknown>;
      const data = toRecord(raw.data ?? raw);
      return {
        id: stringOr(data.artifact_id, data.id, file.id),
        artifact_id: stringOr(data.artifact_id, data.id),
        upload_file_id: stringOr(data.upload_file_id),
        file_name: stringOr(data.file_name, file.file_name),
        mime_type: stringOr(data.mime_type, file.mime_type),
        size: numberOr(data.size, file.size),
        sha256: stringOr(data.sha256, file.sha256),
        url: stringOr(data.preview_url, data.url),
        preview_url: stringOr(data.preview_url, data.url),
        download_url: stringOr(data.download_url, file.download_url),
        signed_download_url: stringOr(data.signed_download_url, file.signed_download_url),
        preview_key: stringOr(data.preview_key, file.preview_key),
        message_id: file.message_id,
        source_kind: stringOr(data.source_kind, file.source_kind, "openclaw_artifact")
      };
    } catch (error) {
      input.logger?.warn?.(
        `[53aihub] failed to upload output file ${file.file_name || file.id}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return stripLocalOnlyOutputFileFields(file);
    }
  }

  async function readOutputFileBytes(file: Hub53AIOutputFile): Promise<Buffer> {
    if (file.path) {
      return readFile(file.path);
    }
    if (file.base64) {
      return Buffer.from(file.base64, "base64");
    }
    if (typeof file.content === "string") {
      return Buffer.from(file.content, "utf8");
    }
    return Buffer.alloc(0);
  }

  function stripLocalOnlyOutputFileFields(file: Hub53AIOutputFile): Hub53AIOutputFile {
    const { path: _path, ...rest } = file;
    return rest;
  }

  function getArtifactUploadTimeoutMs(byteLength: number): number {
    const value = input.config.artifactUploadTimeoutMs;
    if (typeof value === "number" && Number.isFinite(value) && value > 0) {
      return value;
    }
    const mib = Math.max(0, byteLength) / (1024 * 1024);
    return Math.min(30_000, 250 + Math.round(mib * 1_500));
  }

  function bufferToArrayBuffer(bytes: Buffer): ArrayBuffer {
    const arrayBuffer = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(arrayBuffer).set(bytes);
    return arrayBuffer;
  }

  function appendCanonicalOutputFilesEvent(
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    sourceEvent?: GatewayEvent
  ): boolean {
    if (files.length === 0 || eventScope.activeRequestId === "events") {
      return false;
    }
    const freshFiles = files.filter((file) => {
      const keys = getOutputFileEmissionKeys(file);
      if (keys.some((key) => eventScope.emittedOutputFileKeys.has(key))) {
        return false;
      }
      for (const key of keys) {
        eventScope.emittedOutputFileKeys.add(key);
      }
      return true;
    });
    if (freshFiles.length === 0) {
      return false;
    }
    const outputTimeline = buildOpenClawOutputFilesTimelineMeta(eventScope, freshFiles);
    const ledgerSourceEvent = buildSyntheticOutputFilesTimelineEvent(
      sessionId,
      freshFiles,
      eventScope,
      outputTimeline,
      reserveLocalSyntheticEventSeq(eventScope, sourceEvent),
      sourceEvent
    );
    const outputLedger = buildOpenClawLedgerEvent(ledgerSourceEvent, eventScope, outputTimeline);
    appendCanonicalSessionEvent(sessionId, buildCanonicalOutputFilesTimelineEvent(ledgerSourceEvent, outputTimeline, outputLedger));
    return true;
  }

  async function buildOutputFilesLedgerSourceEvent(
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    outputTimeline: OpenClawTimelineV2Meta,
    sourceEvent?: GatewayEvent
  ): Promise<TimelineEvent> {
    const seq = await getNextSyntheticEventSeq(sessionId, eventScope);
    eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, seq);
    return buildSyntheticOutputFilesTimelineEvent(sessionId, files, eventScope, outputTimeline, seq, sourceEvent);
  }

  function reserveLocalSyntheticEventSeq(eventScope: GatewayEventScope, sourceEvent?: GatewayEvent): number {
    const sourceSeq = typeof sourceEvent?.seq === "number" && Number.isFinite(sourceEvent.seq) ? sourceEvent.seq : 0;
    const seq = Math.max(eventScope.lastSeqSeen, sourceSeq) + 1;
    eventScope.lastSeqSeen = Math.max(eventScope.lastSeqSeen, seq);
    return seq;
  }

  function buildSyntheticOutputFilesTimelineEvent(
    sessionId: string,
    files: Hub53AIOutputFile[],
    eventScope: GatewayEventScope,
    outputTimeline: OpenClawTimelineV2Meta,
    seq: number,
    sourceEvent?: GatewayEvent
  ): TimelineEvent {
    return {
      id: `synthetic:output_files:${outputTimeline.segment_id}:${seq}`,
      sessionId,
      seq,
      kind: "process.step",
      payload: {
        process_step: {
          step_code: "output_files",
          status: "completed",
          data: {
            files,
            ...(sourceEvent ? { source_event_ref: buildOpenClawRawEventRef(sourceEvent) } : {})
          }
        }
      },
      createdAt: sourceEvent?.createdAt || new Date().toISOString()
    };
  }

  function buildCanonicalOutputFilesTimelineEvent(
    event: TimelineEvent,
    outputTimeline: OpenClawTimelineV2Meta,
    outputLedger: OpenClawLedgerEvent
  ): TimelineEvent {
    return {
      ...event,
      payload: attachOpenClawLedgerToPayload(
        event,
        attachOpenClawTimelineToPayload(event, toRecord(event.payload), outputTimeline),
        outputLedger
      )
    };
  }

  function sendRaw(payload: string, countMessage = false): boolean {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    try {
      socket.send(payload);
      if (countMessage) {
        sentMessageCount += 1;
      }
      notifyStatus();
      return true;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      notifyStatus();
      return false;
    }
  }

  async function replayOutbox() {
    if (state.outbox.length === 0) {
      return;
    }
    const pending = [...state.outbox];
    state.outbox = [];
    for (const frame of pending) {
      if (!sendRaw(JSON.stringify(frame), true)) {
        state.outbox.push(frame);
      }
    }
    state.outbox = state.outbox.slice(-MAX_OUTBOX_FRAMES);
    await persistState();
  }

  function sendAppPing() {
    if (sendRaw(JSON.stringify({ action: "ping", data: { botId: input.config.botId } }))) {
      if (lastHeartbeatProbeAtMs === 0 || lastHeartbeatAckAtMs >= lastHeartbeatProbeAtMs) {
        lastHeartbeatProbeAtMs = Date.now();
      }
    }
  }

  function checkAccessPolicy(message: Hub53AIIncomingMessage): { allowed: boolean; reason: string } {
    if (input.config.accessPolicy === "open") {
      return { allowed: true, reason: "" };
    }
    const allowed = new Set(input.config.allowFrom);
    const candidates = [
      message.userId,
      message.chatId,
      `user:${message.userId}`,
      `53aihub:${message.userId}`,
      `53aihub:${message.chatId}`
    ];
    if (candidates.some((candidate) => allowed.has(candidate))) {
      return { allowed: true, reason: "" };
    }
    return { allowed: false, reason: "user is not in allowlist" };
  }

  async function loadState() {
    await mkdir(dirname(statePath), { recursive: true });
    try {
      const parsed = JSON.parse(await readFile(statePath, "utf8")) as StoredHubState;
      state = {
        mappings: parsed?.mappings && typeof parsed.mappings === "object" ? parsed.mappings : {},
        outbox: Array.isArray(parsed?.outbox) ? parsed.outbox : [],
        canonicalEventsBySession: normalizeStoredSessionEvents(parsed?.canonicalEventsBySession, MAX_CANONICAL_EVENTS_PER_SESSION),
        syntheticEventsBySession: normalizeStoredSessionEvents(parsed?.syntheticEventsBySession, MAX_SYNTHETIC_EVENTS_PER_SESSION)
      };
      hydrateSessionEventMap(canonicalEventsBySession, state.canonicalEventsBySession);
      hydrateSessionEventMap(syntheticEventsBySession, state.syntheticEventsBySession);
      if (normalizeLoadedOpenClawLedgerSequences()) {
        await persistState({ force: true });
      }
    } catch {
      await persistState();
    }
  }

  function normalizeLoadedOpenClawLedgerSequences(): boolean {
    ledgerSeqBySession.clear();
    let changed = false;
    const sessionIds = new Set<string>([
      ...canonicalEventsBySession.keys(),
      ...syntheticEventsBySession.keys()
    ]);

    for (const sessionId of sessionIds) {
      let maxSeq = 0;
      const normalizeList = (events: TimelineEvent[]) =>
        events.map((event) => {
          const ledger = normalizeOpenClawLedgerEvent(readOpenClawLedgerFromEvent(event));
          if (!ledger) {
            return event;
          }
          const nextSeq = ledger.seq > maxSeq ? ledger.seq : maxSeq + 1;
          maxSeq = nextSeq;
          if (nextSeq === ledger.seq) {
            return event;
          }
          changed = true;
          return rewriteTimelineEventOpenClawLedgerSeq(event, nextSeq);
        });

      const canonicalSource = canonicalEventsBySession.get(sessionId) ?? [];
      const syntheticSource = syntheticEventsBySession.get(sessionId) ?? [];
      const canonicalExposed = canonicalSource.filter(shouldExposeCanonicalSessionEvent);
      const syntheticExposed = syntheticSource.filter(shouldExposeCanonicalSessionEvent);
      if (canonicalExposed.length !== canonicalSource.length || syntheticExposed.length !== syntheticSource.length) {
        changed = true;
        traceOpenClawLedger(input.logger, "history-answer-state-prune", {
          sessionId,
          canonicalRemoved: canonicalSource.length - canonicalExposed.length,
          syntheticRemoved: syntheticSource.length - syntheticExposed.length
        }, input.config);
      }

      const canonical = normalizeList(canonicalExposed);
      const synthetic = normalizeList(syntheticExposed);
      canonicalEventsBySession.set(sessionId, canonical);
      syntheticEventsBySession.set(sessionId, synthetic);
      setPersistedSessionEvents("canonicalEventsBySession", sessionId, canonical, MAX_CANONICAL_EVENTS_PER_SESSION);
      setPersistedSessionEvents("syntheticEventsBySession", sessionId, synthetic, MAX_SYNTHETIC_EVENTS_PER_SESSION);
      ledgerSeqBySession.set(sessionId || "openclaw", maxSeq);
    }

    return changed;
  }

  function rewriteTimelineEventOpenClawLedgerSeq(event: TimelineEvent, seq: number): TimelineEvent {
    const payload = toRecord(event.payload);
    const direct = toRecord(payload.openclaw_ledger);
    if (direct.protocol_version === "openclaw.ledger.v1") {
      return {
        ...event,
        payload: {
          ...payload,
          openclaw_ledger: {
            ...direct,
            seq
          }
        }
      };
    }

    const processStep = toRecord(payload.process_step);
    const processData = toRecord(processStep.data);
    const nested = toRecord(processData.openclaw_ledger);
    if (nested.protocol_version !== "openclaw.ledger.v1") {
      return event;
    }
    return {
      ...event,
      payload: {
        ...payload,
        process_step: {
          ...processStep,
          data: {
            ...processData,
            openclaw_ledger: {
              ...nested,
              seq
            }
          }
        }
      }
    };
  }

  async function persistState(options: { force?: boolean } = {}) {
    if (stopped && !options.force) {
      return;
    }
    if (persistStateTimer) {
      clearTimeout(persistStateTimer);
      persistStateTimer = null;
    }
    const nextPersist = persistStateQueue
      .catch(() => undefined)
      .then(() => writePersistedState(`${JSON.stringify(state, null, 2)}\n`));
    persistStateQueue = nextPersist.catch(() => undefined);
    await nextPersist;
  }

  async function writePersistedState(serialized: string) {
    await mkdir(dirname(statePath), { recursive: true });
    const tempPath = `${statePath}.${randomUUID()}.tmp`;
    try {
      await writeFile(tempPath, serialized);
      await rename(tempPath, statePath);
    } catch (error) {
      if (stopped && isMissingPersistStatePathError(error)) {
        input.logger?.warn?.(
          `[53aihub] skipped late persist after bridge stop: ${error instanceof Error ? error.message : String(error)}`
        );
        return;
      }
      throw error;
    }
  }

  function persistStateSoon(reason: string) {
    if (stopped) {
      return;
    }
    if (persistStateTimer) {
      return;
    }
    persistStateTimer = setTimeout(() => {
      persistStateTimer = null;
      void persistState().catch((error) => {
        input.logger?.warn?.(
          `[53aihub] failed to persist ${reason}: ${error instanceof Error ? error.message : String(error)}`
        );
      });
    }, PERSIST_STATE_DEBOUNCE_MS);
    persistStateTimer.unref?.();
  }

  function isMissingPersistStatePathError(error: unknown): boolean {
    return Boolean(error && typeof error === "object" && "code" in error && error.code === "ENOENT");
  }

  function normalizeStoredSessionEvents(value: unknown, limit: number): Record<string, TimelineEvent[]> {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return {};
    }
    const output: Record<string, TimelineEvent[]> = {};
    for (const [sessionId, events] of Object.entries(value as Record<string, unknown>)) {
      if (!sessionId || !Array.isArray(events)) continue;
      output[sessionId] = events
        .filter((event): event is TimelineEvent => Boolean(event && typeof event === "object"))
        .slice(-limit);
    }
    return output;
  }

  function hydrateSessionEventMap(
    target: Map<string, TimelineEvent[]>,
    source: Record<string, TimelineEvent[]> | undefined
  ) {
    target.clear();
    for (const [sessionId, events] of Object.entries(source || {})) {
      target.set(sessionId, events);
    }
  }

  function setPersistedSessionEvents(
    key: "canonicalEventsBySession" | "syntheticEventsBySession",
    sessionId: string,
    events: TimelineEvent[],
    limit: number
  ) {
    const current = state[key] && typeof state[key] === "object" ? { ...state[key] } : {};
    current[sessionId] = events.slice(-limit);
    state[key] = current;
  }

  function notifyStatus() {
    input.callbacks.onStatusChange();
  }

  return {
    start,
    stop,
    getStatus
  };
}

function isConfigDebugFlagEnabled(value: unknown): boolean {
  if (value === true) {
    return true;
  }
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function isOpenClawDuplicateTraceEnabled(config?: Hub53AIConfig): boolean {
  if (
    isConfigDebugFlagEnabled(config?.diagnosticLogs) ||
    isConfigDebugFlagEnabled(config?.duplicateTrace) ||
    isConfigDebugFlagEnabled(config?.debug?.all) ||
    isConfigDebugFlagEnabled(config?.debug?.duplicates)
  ) {
    return true;
  }
  const value = String(process.env.OPENCLAW_TRACE_DUPLICATES ?? process.env.OPENCLAW_DIAG_LOGS ?? "").toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function traceOpenClawDuplicate(
  logger: HubBridgeInput["logger"] | undefined,
  label: string,
  payload: Record<string, unknown>,
  config?: Hub53AIConfig
): void {
  if (!isOpenClawDuplicateTraceEnabled(config)) {
    return;
  }
  const line = `[openclaw-dup-trace] ${label} ${safeTraceJson(payload)}`;
  if (logger?.info) {
    logger.info(line);
    return;
  }
  console.info(line);
}

function isOpenClawLedgerDebugEnabled(config?: Hub53AIConfig): boolean {
  if (
    isConfigDebugFlagEnabled(config?.diagnosticLogs) ||
    isConfigDebugFlagEnabled(config?.ledgerDebug) ||
    isConfigDebugFlagEnabled(config?.debug?.all) ||
    isConfigDebugFlagEnabled(config?.debug?.ledger)
  ) {
    return true;
  }
  const value = String(process.env.OPENCLAW_LEDGER_DEBUG ?? process.env.OPENCLAW_DIAG_LOGS ?? "").toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function traceOpenClawLedger(
  logger: HubBridgeInput["logger"] | undefined,
  label: string,
  payload: Record<string, unknown>,
  config?: Hub53AIConfig
): void {
  if (!isOpenClawLedgerDebugEnabled(config)) {
    return;
  }
  const line = `[openclaw-ledger] ${label} ${safeTraceJson(payload)}`;
  if (logger?.info) {
    logger.info(line);
    return;
  }
  console.info(line);
}

function summarizeOpenClawSessionSnapshotForTrace(input: {
  sessionId: string;
  afterSeq: number;
  snapshot: OpenClawSessionSnapshot;
  turns: OpenClawLedgerTurnSnapshot[];
  syntheticTerminals: OpenClawLedgerEvent[];
  tracked: TrackedActiveOpenClawTurns;
}): Record<string, unknown> {
  const runningTurns = input.turns.filter((turn) => turn.status === "running");
  const ledgerEvents = input.snapshot.ledger_events ?? input.snapshot.ledgerEvents ?? [];
  return {
    sessionIdHash: hashTraceText(input.sessionId),
    afterSeq: input.afterSeq,
    lastSeq: input.snapshot.last_seq,
    turnCount: input.turns.length,
    runningTurnCount: runningTurns.length,
    turnStatusCounts: countOpenClawSnapshotValues(input.turns, (turn) => turn.status),
    activeTurnCount: input.snapshot.active_turns.length,
    runningActiveTurnCount: input.snapshot.active_turns.filter((turn) => turn.status === "running").length,
    activeTurns: summarizeOpenClawSnapshotTurnsForTrace(input.snapshot.active_turns, 5),
    runningTurns: summarizeOpenClawSnapshotTurnsForTrace(runningTurns, 5),
    trackedTurnCount: input.tracked.turnIds.size,
    trackedRunCount: input.tracked.runIds.size,
    recentEventCount: input.snapshot.recent_events.length,
    recentEventMaxSeq: maxOpenClawLedgerEventSeq(input.snapshot.recent_events),
    recentEventTypeCounts: countOpenClawSnapshotValues(input.snapshot.recent_events, (event) => event.event_type),
    recentTerminalStatusCounts: countOpenClawSnapshotValues(input.snapshot.recent_events, (event) => event.terminal_status, true),
    recentTailEvents: summarizeOpenClawSnapshotEventsForTrace(input.snapshot.recent_events, 3),
    ledgerEventCount: ledgerEvents.length,
    ledgerEventMaxSeq: maxOpenClawLedgerEventSeq(ledgerEvents),
    ledgerEventTypeCounts: countOpenClawSnapshotValues(ledgerEvents, (event) => event.event_type),
    ledgerTerminalStatusCounts: countOpenClawSnapshotValues(ledgerEvents, (event) => event.terminal_status, true),
    ledgerTailEvents: summarizeOpenClawSnapshotEventsForTrace(ledgerEvents, 3),
    syntheticTerminalCount: input.syntheticTerminals.length,
    syntheticTerminalStatuses: countOpenClawSnapshotValues(input.syntheticTerminals, (event) => event.terminal_status, true)
  };
}

function summarizeOpenClawSnapshotTurnsForTrace(turns: OpenClawLedgerTurnSnapshot[], limit: number): Record<string, unknown>[] {
  if (limit <= 0 || !turns.length) {
    return [];
  }
  return turns.slice(Math.max(0, turns.length - limit)).map((turn) => ({
    turnIdHash: hashTraceText(turn.turn_id),
    runIdHash: hashTraceText(turn.run_id || ""),
    activeRequestIdHash: hashTraceText(turn.active_request_id || ""),
    status: turn.status,
    terminalSeq: turn.terminal_seq,
    lastSeq: turn.last_seq,
    partCount: turn.part_ids.length,
    partIdHashes: turn.part_ids.slice(0, 5).map((partId) => hashTraceText(partId))
  }));
}

function summarizeOpenClawSnapshotEventsForTrace(events: OpenClawLedgerEvent[], limit: number): Record<string, unknown>[] {
  if (limit <= 0 || !events.length) {
    return [];
  }
  return events.slice(Math.max(0, events.length - limit)).map((event) => ({
    seq: event.seq,
    turnIdHash: hashTraceText(event.turn_id),
    runIdHash: hashTraceText(event.run_id || ""),
    activeRequestIdHash: hashTraceText(event.active_request_id),
    partIdHash: hashTraceText(event.part_id),
    partType: event.part_type,
    eventType: event.event_type,
    operation: event.operation,
    visibility: event.visibility,
    terminalStatus: event.terminal_status,
    textLength: event.text?.length ?? 0,
    textHash: hashTraceText(event.text || ""),
    rawEventRefHash: hashTraceText(event.raw_event_ref || "")
  }));
}

function countOpenClawSnapshotValues<T>(
  items: T[],
  readValue: (item: T) => string | undefined,
  skipEmpty = false
): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const item of items) {
    const value = String(readValue(item) || "").trim();
    if (!value && skipEmpty) {
      continue;
    }
    const key = value || "unknown";
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function maxOpenClawLedgerEventSeq(events: OpenClawLedgerEvent[]): number {
  return events.reduce((maxSeq, event) => Math.max(maxSeq, event.seq || 0), 0);
}

function summarizeTimelineEventForTrace(event: TimelineEvent): Record<string, unknown> {
  const payload = toRecord(event.payload);
  const timeline = toRecord(payload.openclaw_timeline);
  const content = typeof payload.content === "string" ? payload.content : "";
  return {
    id: event.id,
    kind: event.kind,
    seq: event.seq,
    rawSeq: payload.rawSeq,
    messageSeq: payload.message_seq ?? payload.messageSeq,
    runId: payload.runId,
    state: payload.state,
    mode: payload.mode,
    replace: payload.replace,
    segmentId: stringOr(timeline.segment_id, payload.segment_id),
    segmentType: stringOr(timeline.segment_type, payload.segment_type),
    deltaIndex: timeline.delta_index ?? payload.delta_index,
    visibility: timeline.visibility ?? payload.visibility,
    final: timeline.final ?? payload.final,
    contentLength: content.length,
    contentHash: hashTraceText(content)
  };
}

function summarizeOpenClawLedgerEvent(event: OpenClawLedgerEvent): Record<string, unknown> {
  return {
    seq: event.seq,
    sessionId: event.session_id,
    turnId: event.turn_id,
    runId: event.run_id,
    activeRequestId: event.active_request_id,
    partId: event.part_id,
    partType: event.part_type,
    eventType: event.event_type,
    operation: event.operation,
    visibility: event.visibility,
    terminalStatus: event.terminal_status,
    rawEventRef: event.raw_event_ref,
    textLength: event.text?.length ?? 0,
    textHash: hashTraceText(event.text || "")
  };
}

function summarizeOutgoingFrameForTrace(frame: Hub53AIOutgoingFrame): Record<string, unknown> {
  if ("data" in frame && toRecord(frame.data).object === "chat.completion.chunk") {
    const data = toRecord(frame.data);
    const choice = toRecord((Array.isArray(data.choices) ? data.choices : [])[0]);
    const delta = toRecord(choice.delta);
    const payload = toRecord(data.payload);
    const timeline = toRecord(payload.openclaw_timeline);
    const content =
      typeof delta.content === "string"
        ? delta.content
        : typeof delta.reasoning_content === "string"
          ? delta.reasoning_content
          : "";
    return {
      reqId: frame.req_id,
      status: frame.status,
      eventKind: data.event_kind,
      sessionId: data.session_id,
      finishReason: choice.finish_reason,
      mode: data.mode,
      replace: data.replace,
      payloadSeq: payload.seq,
      rawSeq: payload.rawSeq,
      runId: payload.runId,
      segmentId: stringOr(timeline.segment_id, payload.segment_id),
      segmentType: stringOr(timeline.segment_type, payload.segment_type),
      deltaIndex: timeline.delta_index ?? payload.delta_index,
      contentLength: content.length,
      contentHash: hashTraceText(content)
    };
  }
  return {
    reqId: frame.req_id,
    action: frame.action,
    status: frame.status
  };
}

function hashTraceText(text: string): string {
  return text ? createHash("sha1").update(text).digest("hex").slice(0, 12) : "";
}

function safeTraceJson(payload: Record<string, unknown>): string {
  try {
    return JSON.stringify(payload);
  } catch {
    return "{}";
  }
}

export function parseIncomingMessage(rawJson: string): Hub53AIIncomingMessage | null {
  try {
    const wsMsg = JSON.parse(rawJson) as Record<string, any>;
    if (wsMsg.action === "ping" || wsMsg.action === "pong") {
      return null;
    }

    if (wsMsg.action === "chat") {
      const openAIReq = toRecord(wsMsg.data);
      const metadata = toRecord(openAIReq.metadata);
      const messages = Array.isArray(openAIReq.messages) ? openAIReq.messages : [];
      const lastUserMsg = [...messages].reverse().find((message) => toRecord(message).role === "user");
      if (!lastUserMsg) {
        return null;
      }
      const userMessage = toRecord(lastUserMsg);
      const content = userMessage.content;
      const userObject = toRecord(openAIReq.user);
      const userId = stringOr(
        openAIReq.user,
        userObject.id,
        userObject.userId,
        userMessage.userId,
        userMessage.name,
        `user-${String(wsMsg.req_id ?? randomUUID())}`
      );
      const chatId = stringOr(openAIReq.conversation_id, userId);
      const inputFiles = extractInputFilesFromContent(content, metadata);
      return {
        type: "message",
        msgId: String(wsMsg.req_id ?? randomUUID()),
        reqId: String(wsMsg.req_id ?? randomUUID()),
        chatId,
        userId,
        userName: extractUserName(openAIReq, metadata, userObject, userMessage),
        conversationTitle: extractConversationTitle(openAIReq, metadata),
        clientMessageId: extractOpenClawClientMessageId(openAIReq, metadata, userMessage),
        text: extractTextFromContent(content),
        imageUrls: extractImagesFromContent(content),
        fileUrls: dedupeStrings([...extractFilesFromContent(content), ...inputFiles.map(getInputFileURL).filter(Boolean)]),
        files: inputFiles,
        skill: extractSkillSelection(metadata)
      };
    }

    if (typeof wsMsg.status === "string" && wsMsg.status !== "request") {
      return null;
    }

    const data = toRecord(wsMsg.data);
    const userObject = toRecord(data.user);
    const chatId = stringOr(data.chatId, data.userId, "default-chat");
    const userId = stringOr(data.userId, userObject.id, userObject.userId, data.chatId, "default-user");
    const inputFiles = normalizeInputFiles(data.openclaw_input_files, data.files);
    return {
      type: stringOr(data.type, "message"),
      msgId: stringOr(data.msgId, data.id, `msg-${Date.now()}`),
      reqId: String(wsMsg.req_id ?? data.msgId ?? data.id ?? `msg-${Date.now()}`),
      chatId,
      userId,
      userName: extractUserName(data, userObject),
      conversationTitle: extractConversationTitle(data),
      clientMessageId: extractOpenClawClientMessageId(data, userObject),
      text: stringOr(data.text, data.content, ""),
      imageUrls: normalizeUrlList(data.imageUrls, data.images),
      fileUrls: dedupeStrings([...normalizeUrlList(data.fileUrls, data.files), ...inputFiles.map(getInputFileURL).filter(Boolean)]),
      files: inputFiles,
      skill: extractSkillSelection(data),
      quoteContent: typeof data.quoteContent === "string" ? data.quoteContent : undefined
    };
  } catch {
    return null;
  }
}

function parseRPCRequest(rawJson: string): Hub53AIRPCRequest | null {
  try {
    const parsed = JSON.parse(rawJson) as Record<string, unknown>;
    const reqId = stringOr(parsed.req_id, parsed.reqId);
    const action = stringOr(parsed.action);
    const status = stringOr(parsed.status);
    if (!reqId || !action) {
      return null;
    }
    if (status === "request") {
      return {
        reqId,
        action,
        data: parsed.data
      };
    }
    if (status) {
      return null;
    }
    if (!HUB_RPC_ACTIONS.has(action)) {
      return null;
    }
    return {
      reqId,
      action,
      data: parsed.data
    };
  } catch {
    return null;
  }
}

function readRPCPagination(payload: unknown, defaultLimit: number): RPCPagination {
  const record = toRecord(payload);
  return {
    limit: clampPositiveInteger(record.limit, defaultLimit, 200),
    offset: clampNonNegativeInteger(record.offset, 0)
  };
}

function readRPCSessionId(payload: Record<string, unknown>): string {
  const sessionId = stringOr(payload.session_id, payload.sessionId, payload.conversation_id, payload.conversationId);
  if (!sessionId) {
    throw new HubRPCError("PARAM_ERROR", "session_id or conversation_id is required");
  }
  return sessionId;
}

function readRPCAfterSeq(payload: Record<string, unknown>): number {
  return clampNonNegativeInteger(payload.after_seq ?? payload.afterSeq, 0);
}

function clampPositiveInteger(value: unknown, fallback: number, max: number): number {
  const numberValue = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  if (!Number.isInteger(numberValue) || numberValue <= 0) {
    return fallback;
  }
  return Math.min(numberValue, max);
}

function clampNonNegativeInteger(value: unknown, fallback: number): number {
  const numberValue = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  if (!Number.isInteger(numberValue) || numberValue < 0) {
    return fallback;
  }
  return numberValue;
}

function buildPagination(limit: number, offset: number, total: number, pageSize: number) {
  const nextOffset = offset + pageSize;
  return buildPaginationWithNextOffset(limit, offset, total, nextOffset);
}

function buildPaginationWithNextOffset(limit: number, offset: number, total: number, nextOffset: number) {
  const safeNextOffset = Math.max(offset, nextOffset);
  const hasMore = safeNextOffset < total;
  return {
    limit,
    offset,
    total,
    hasMore,
    ...(hasMore ? { nextOffset: safeNextOffset } : {})
  };
}

function getLatestWindowPageBounds<T>(items: T[], limit: number, offset: number): { start: number; end: number } {
  const end = Math.max(0, items.length - offset);
  const start = Math.max(0, end - limit);
  return { start, end };
}

type LatestWindowPage<T> = {
  items: T[];
  start: number;
  end: number;
};

export function sliceLatestWindowPage<T>(items: T[], limit: number, offset: number): T[] {
  const { start, end } = getLatestWindowPageBounds(items, limit, offset);
  return items.slice(start, end);
}

function sliceLatestWindowPageWithTurnBoundary(items: SessionMessage[], limit: number, offset: number): LatestWindowPage<SessionMessage> {
  const { start, end } = getLatestWindowPageBounds(items, limit, offset);
  if (start >= end) {
    return { items: [], start, end };
  }

  let expandedStart = start;
  let expandedEnd = end;
  for (let index = start; index < end; index += 1) {
    const message = items[index];
    if (!message) continue;
    if (message.role === "assistant") {
      const userIndex = findPreviousTurnUserIndex(items, index);
      if (userIndex >= 0 && userIndex < expandedStart) {
        expandedStart = userIndex;
      }
      continue;
    }
    if (message.role === "user") {
      const assistantIndex = findNextTurnAssistantIndex(items, index);
      if (assistantIndex >= end && assistantIndex + 1 > expandedEnd) {
        expandedEnd = assistantIndex + 1;
      }
    }
  }

  return {
    items: items.slice(expandedStart, expandedEnd),
    start: expandedStart,
    end: expandedEnd
  };
}

function findPreviousTurnUserIndex(items: SessionMessage[], assistantIndex: number): number {
  const minIndex = Math.max(0, assistantIndex - MAX_SESSION_MESSAGE_TURN_BOUNDARY_OVERSCAN);
  for (let index = assistantIndex - 1; index >= minIndex; index -= 1) {
    const role = items[index]?.role;
    if (role === "user") {
      return index;
    }
  }
  return -1;
}

function findNextTurnAssistantIndex(items: SessionMessage[], userIndex: number): number {
  const maxIndex = Math.min(items.length - 1, userIndex + MAX_SESSION_MESSAGE_TURN_BOUNDARY_OVERSCAN);
  for (let index = userIndex + 1; index <= maxIndex; index += 1) {
    const role = items[index]?.role;
    if (role === "user") {
      return -1;
    }
    if (role === "assistant") {
      return index;
    }
  }
  return -1;
}

function buildSkillsPayload(
  runtimeInfo: GatewayRuntimeInfo,
  ensuredRuntimeSkills?: Map<string, RuntimeSkillDisplayItem>
) {
  const enabledSkills = mergeRuntimeSkills(
    runtimeInfo.enabledSkills,
    ensuredRuntimeSkills ? Array.from(ensuredRuntimeSkills.values()) : []
  );
  return {
    ...(runtimeInfo.modelPrimary ? { modelPrimary: runtimeInfo.modelPrimary } : {}),
    skills: enabledSkills,
    enabledSkills
  };
}

function buildEnsuredRuntimeSkill(
  request: EnsureHubSkillRequest,
  result: EnsureHubSkillResult
): Record<string, unknown> {
  const skillName = stringOr(result.skill_name, request.skill_name);
  const skillID = stringOr(result.skill_id, request.skill_id);
  const displayName = stringOr(result.display_name, request.display_name, skillName, skillID);
  return {
    id: skillID || skillName || displayName,
    ...(skillID ? { skill_id: skillID } : {}),
    ...(skillName ? { skill_name: skillName } : {}),
    name: displayName || skillName || skillID,
    title: displayName || skillName || skillID,
    ...(displayName ? { display_name: displayName } : {}),
    enabled: true,
    status: "enabled",
    source: "53aihub",
    ensure_status: result.status,
    ...(result.version ? { version: result.version } : {}),
    ...(result.install_path ? { install_path: result.install_path } : {})
  };
}

function mergeRuntimeSkills(
  gatewaySkills: unknown[],
  ensuredSkills: RuntimeSkillDisplayItem[]
): RuntimeSkillDisplayItem[] {
  const seen = new Set<string>();
  const output: RuntimeSkillDisplayItem[] = [];
  for (const skill of [...gatewaySkills, ...ensuredSkills]) {
    const key = runtimeSkillIdentity(skill);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    output.push(skill as RuntimeSkillDisplayItem);
  }
  return output;
}

function runtimeSkillIdentity(skill: unknown): string {
  if (typeof skill === "string") {
    return skill.trim().toLowerCase();
  }
  const record = toRecord(skill);
  return stringOr(
    record.skill_name,
    record.name,
    record.title,
    record.display_name,
    record.skill_id,
    record.id
  ).toLowerCase();
}

function redactHubConfig(config: Hub53AIConfig): Omit<Hub53AIConfig, "secret"> & { secret: string } {
  return {
    ...config,
    secret: "[redacted]"
  };
}

function normalizeRPCError(error: unknown) {
  if (error instanceof HubRPCError) {
    return {
      code: error.code,
      message: error.message,
      ...(error.details ? { details: error.details } : {})
    };
  }
  const message = error instanceof Error ? error.message : String(error);
  return {
    code: inferErrorCode(message),
    message
  };
}

function extractUserName(...sources: unknown[]): string | undefined {
  const nameKeys = [
    "userName",
    "username",
    "nickName",
    "nickname",
    "displayName",
    "senderName",
    "fromUserName",
    "name"
  ];

  for (const source of sources) {
    const record = toRecord(source);
    const direct = stringFromKeys(record, nameKeys);
    if (direct) {
      return direct;
    }

    const nestedUser = toRecord(record.user);
    const nestedName = stringFromKeys(nestedUser, nameKeys);
    if (nestedName) {
      return nestedName;
    }
  }

  return undefined;
}

function extractConversationTitle(...sources: unknown[]): string | undefined {
  const titleKeys = [
    "openclaw_conversation_title",
    "openclawConversationTitle",
    "conversation_title",
    "conversationTitle",
    "title"
  ];
  for (const source of sources) {
    const record = toRecord(source);
    const title = stringFromKeys(record, titleKeys);
    if (title) {
      return title;
    }
  }
  return undefined;
}

function extractOpenClawClientMessageId(...sources: unknown[]): string | undefined {
  const idKeys = [
    "openclaw_client_message_id",
    "openclawClientMessageId",
    "client_message_id",
    "clientMessageId"
  ];
  for (const source of sources) {
    const record = toRecord(source);
    const id = stringFromKeys(record, idKeys);
    if (id) {
      return id;
    }
  }
  return undefined;
}

function stringFromKeys(record: Record<string, any>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function buildHubSessionTitle(message: Hub53AIIncomingMessage): string {
  const userName = sanitizeTitlePart(message.userName || message.userId || message.chatId || "未知用户");
  const summary = summarizeHubMessageForTitle(message);
  return `${HUB_SESSION_TITLE_PREFIX}${userName}：${summary}`;
}

function buildUniqueHubSessionTitle(baseTitle: string, attempt: number): string {
  if (attempt === 0) {
    return baseTitle;
  }
  return `${baseTitle} (${attempt + 1})`;
}

function isDuplicateSessionTitleError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /label.*already.*in use|already.*in use|duplicate|title.*exists/i.test(message);
}

function summarizeHubMessageForTitle(message: Hub53AIIncomingMessage): string {
  const text = normalizeTitleText(message.text || message.quoteContent || "");
  if (text) {
    return truncateVisibleText(text, HUB_TITLE_SUMMARY_LENGTH);
  }
  if (message.imageUrls?.length && message.fileUrls?.length) {
    return "多媒体消息";
  }
  if (message.imageUrls?.length) {
    return "图片消息";
  }
  if (message.fileUrls?.length) {
    return "文件消息";
  }
  return "新会话";
}

function sanitizeTitlePart(value: string): string {
  return normalizeTitleText(value).replace(/[：:]/g, "").slice(0, 40) || "未知用户";
}

function normalizeTitleText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncateVisibleText(value: string, maxLength: number): string {
  const chars = Array.from(value);
  return chars.length > maxLength ? `${chars.slice(0, maxLength).join("")}…` : value;
}

function isOldHubPlaceholderTitle(title: string, chatId: string): boolean {
  const normalized = title.trim();
  return [`53AIHub ${chatId}`, `53AIHub:${chatId}`, `53AIHub-${chatId}`, chatId].includes(normalized);
}

function isHubManagedSessionTitle(title: string, chatId: string): boolean {
  const normalized = title.trim();
  return normalized.startsWith(HUB_SESSION_TITLE_PREFIX) || isOldHubPlaceholderTitle(normalized, chatId);
}

function isRestorableHubSession(session: GatewaySession, chatId: string, userName?: string): boolean {
  const normalized = session.title.trim();
  if (isOldHubPlaceholderTitle(normalized, chatId)) {
    return true;
  }
  if (!normalized.startsWith(HUB_SESSION_TITLE_PREFIX) || !userName) {
    return false;
  }
  return normalized.startsWith(`${HUB_SESSION_TITLE_PREFIX}${sanitizeTitlePart(userName)}：`);
}

function mergeKnownHubSessions(gatewaySessions: GatewaySession[], knownSessions: GatewaySession[]): GatewaySession[] {
  const knownHubById = new Map(
    knownSessions.filter((session) => isHubTitle(session.title)).map((session) => [session.id, session])
  );
  if (!knownHubById.size) {
    return gatewaySessions;
  }

  const merged = gatewaySessions.map((session) => {
    const knownSession = knownHubById.get(session.id);
    if (!knownSession || !shouldPreferKnownHubTitle(session.title, knownSession.title)) {
      return session;
    }
    return {
      ...session,
      title: knownSession.title
    };
  });
  return merged;
}

function applyHubTitleHint(session: GatewaySession, titleHint?: string): GatewaySession {
  const normalizedTitle = titleHint?.trim();
  if (!normalizedTitle || !isHubTitle(normalizedTitle)) {
    return session;
  }
  return {
    ...session,
    title: normalizedTitle
  };
}

function shouldPreferKnownHubTitle(gatewayTitle: string, knownTitle: string): boolean {
  return isHubTitle(knownTitle) && (isControlCenterTitle(gatewayTitle) || !isHubTitle(gatewayTitle));
}

function isHubTitle(title: string): boolean {
  return title.trim().startsWith(HUB_SESSION_TITLE_PREFIX);
}

function isControlCenterTitle(title: string): boolean {
  return title.trim() === CONTROL_CENTER_SESSION_TITLE;
}

function toTime(value?: string): number {
  if (!value) return 0;
  const time = Date.parse(value);
  return Number.isFinite(time) ? time : 0;
}

function isOpenClawSessionId(value: string): boolean {
  return value.startsWith("agent:");
}

function inferMimeTypeFromFileName(fileName: string): string {
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  const mimeByExt: Record<string, string> = {
    csv: "text/csv",
    gif: "image/gif",
    htm: "text/html",
    html: "text/html",
    jpeg: "image/jpeg",
    jpg: "image/jpeg",
    json: "application/json",
    log: "text/plain",
    md: "text/markdown",
    pdf: "application/pdf",
    png: "image/png",
    svg: "image/svg+xml",
    txt: "text/plain",
    webp: "image/webp",
    xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
  };
  return mimeByExt[ext] ?? "application/octet-stream";
}

function buildPrompt(
  message: Hub53AIIncomingMessage,
  options: { includeRuntimeContext?: boolean; outputManifest?: OutputManifestPromptContext } = {}
): string {
  const parts = [message.text.trim()].filter(Boolean);
  if (message.imageUrls?.length) {
    parts.push(`Images:\n${message.imageUrls.join("\n")}`);
  }

  if (options.includeRuntimeContext !== false) {
    const runtimeContext = buildRuntimePromptContext(message, options.outputManifest);
    if (runtimeContext) {
      parts.push(runtimeContext);
    }
  }
  return parts.join("\n\n");
}

function buildDisplayUserContent(message: Hub53AIIncomingMessage): string {
  const parts = [message.text.trim()].filter(Boolean);
  if (message.imageUrls?.length) {
    parts.push(`Images:\n${message.imageUrls.join("\n")}`);
  }
  return parts.join("\n\n");
}

function buildDisplayUserMessageMetadata(message: Hub53AIIncomingMessage): Record<string, unknown> {
  return {
    ...(message.clientMessageId ? { openclaw_client_message_id: message.clientMessageId } : {}),
    ...(message.skill ? { openclaw_skill: message.skill } : {}),
    ...(message.files?.length ? { openclaw_input_files: message.files } : {})
  };
}

type OutputManifestPromptContext = {
  path: string;
  conversationId: string;
  turnId: string;
  activeRequestId: string;
  workspaceDirs: string[];
};

function buildRuntimePromptContext(
  message: Hub53AIIncomingMessage,
  outputManifest?: OutputManifestPromptContext
): string {
  const lines: string[] = [];
  const localFileRefs = dedupeStrings(
    (message.files ?? [])
      .map((file) => stringOr(file.local_path))
      .filter((filePath): filePath is string => Boolean(filePath))
      .map((filePath) => `@${filePath}`)
  );
  if (localFileRefs.length) {
    lines.push("Local input files:", ...localFileRefs);
  }

  const remoteFileRefs = dedupeStrings([
    ...(message.files ?? []).map((file) => getInputFileURL(file)).filter(Boolean),
    ...(message.fileUrls ?? []).filter((fileURL) => !fileURL.startsWith("/"))
  ]).filter((fileURL) => !localFileRefs.includes(`@${fileURL}`));
  if (!localFileRefs.length && remoteFileRefs.length) {
    lines.push("Remote input files:", ...remoteFileRefs);
  }

  if (message.skill?.skill_name) {
    lines.push(`Selected skill: /${message.skill.skill_name}`);
    lines.push("Use the installed local skill with this name for this turn. Do not quote the skill instructions back to the user.");
  }

  if (outputManifest) {
    lines.push("Output artifact manifest:");
    lines.push(`Manifest path: ${outputManifest.path}`);
    lines.push(`conversation_id: ${JSON.stringify(outputManifest.conversationId)}`);
    lines.push(`turn_id: ${JSON.stringify(outputManifest.turnId)}`);
    lines.push(`active_request_id: ${JSON.stringify(outputManifest.activeRequestId)}`);
    if (outputManifest.workspaceDirs.length) {
      lines.push("Allowed output workspace roots:", ...outputManifest.workspaceDirs.map((workspaceDir) => `@${workspaceDir}`));
    }
    lines.push(
      "When you create or modify a user-visible output file for this turn, append exactly one JSON object per line to the manifest path."
    );
    lines.push(
      "Each JSON object must include conversation_id, turn_id, active_request_id, part_id, path, logical_path, mime_type, size, sha256, created_at, and source_kind."
    );
    lines.push("Identifier fields must be JSON strings, even when an id looks numeric.");
    lines.push("Do not rewrite prior manifest lines.");
  }

  if (!lines.length) {
    return "";
  }
  return [OPENCLAW_RUNTIME_CONTEXT_START, ...lines, OPENCLAW_RUNTIME_CONTEXT_END].join("\n");
}

function buildOutgoingChunk(
  reqId: string,
  text: string,
  status: Hub53AIOutgoingChunk["status"],
  error?: Hub53AIOutgoingChunk["data"]["error"],
  sessionId?: string,
  metadata?: {
    mode?: string;
    replace?: boolean;
    eventKind?: string;
    payload?: Record<string, unknown>;
  }
): Hub53AIOutgoingChunk {
  const delta =
    status === "thinking"
      ? {
          reasoning_content: text,
          role: "assistant" as const
        }
      : {
          content: text,
          role: "assistant" as const
        };
  return {
    req_id: reqId,
    action: "chat",
    status,
    data: {
      id: reqId,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: "openclaw-agent",
      status,
      ...(metadata?.mode ? { mode: metadata.mode } : {}),
      ...(typeof metadata?.replace === "boolean" ? { replace: metadata.replace } : {}),
      ...(metadata?.eventKind ? { event_kind: metadata.eventKind } : {}),
      ...(metadata?.payload ? { payload: metadata.payload } : {}),
      ...(sessionId ? { session_id: sessionId, conversation_id: sessionId } : {}),
      choices: [
        {
          index: 0,
          delta,
          finish_reason: status === "done" ? "stop" : status === "error" ? "error" : null
        }
      ],
      ...(error ? { error } : {})
    }
  };
}

function buildOutputFilesProcessStep(
  reqId: string,
  files: Hub53AIOutputFile[],
  sessionId?: string,
  timeline?: OpenClawTimelineV2Meta,
  ledger?: OpenClawLedgerEvent
): Hub53AIOutgoingProcessStep {
  const mediaAttachments = files.map((file) => ({
    ...file,
    kind: resolveMediaKind(file.mime_type, file.file_name)
  }));
  return {
    req_id: reqId,
    action: "chat",
    status: "streaming",
    data: {
      id: `${reqId}-output-files-${Date.now()}`,
      object: "process.step",
      created: Math.floor(Date.now() / 1000),
      model: "openclaw-agent",
      status: "streaming",
      ...(sessionId ? { session_id: sessionId, conversation_id: sessionId } : {}),
      process_step: {
        step_code: "output_files",
        name: "生成文件",
        status: "completed",
        message: `生成了 ${files.length} 个文件`,
        data: {
          files,
          contract_version: "v1",
          ...(timeline ? { openclaw_timeline: timeline } : {}),
          ...(ledger ? { openclaw_ledger: ledger } : {}),
          media_attachments: mediaAttachments,
          media_contract_version: "v1"
        },
        timestamp: Math.floor(Date.now() / 1000)
      }
    }
  };
}

function extractOutputFilesFromPayload(payload: unknown): Hub53AIOutputFile[] {
  const candidates = collectOutputFileCandidates(payload);
  const seen = new Set<string>();
  const files: Hub53AIOutputFile[] = [];
  for (const candidate of candidates) {
    const file = normalizeOutputFile(candidate);
    if (!file) {
      continue;
    }
    const key = getOutputFileKeys(file)[0] ?? "";
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    files.push(file);
  }
  return files;
}

function collectOutputFileCandidates(value: unknown, depth = 0): unknown[] {
  if (value == null || depth > 4) {
    return [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((entry) => collectOutputFileCandidates(entry, depth + 1));
  }
  if (typeof value !== "object") {
    return [];
  }

  const record = value as Record<string, unknown>;
  if (looksLikeOutputFile(record)) {
    return [record];
  }

  const candidates: unknown[] = [];
  for (const key of [
    "output_files",
    "outputFiles",
    "files",
    "file",
    "attachments",
    "attachment",
    "artifacts",
    "artifact",
    "media",
    "media_attachments",
    "generated_files",
    "generatedFiles",
    "process_step",
    "processStep"
  ]) {
    const nested = record[key];
    if (nested == null) {
      continue;
    }
    if (Array.isArray(nested)) {
      candidates.push(...nested);
    } else {
      candidates.push(nested);
    }
  }

  for (const key of ["data", "result", "payload", "message", "content", "output", "process_step", "processStep"]) {
    candidates.push(...collectOutputFileCandidates(record[key], depth + 1));
  }

  return candidates;
}

function looksLikeOutputFile(record: Record<string, unknown>): boolean {
  const url = readFirstString(record, ["url", "href", "preview_url", "previewUrl", "download_url", "downloadUrl", "file_url", "fileUrl", "signed_url"]);
  const base64 = readFirstString(record, ["base64"]);
  const content = readFirstString(record, ["content", "data"]);
  const explicitFileName = readFirstString(record, ["file_name", "fileName", "filename", "path", "title"]);
  const displayName = explicitFileName || readFirstString(record, ["name"]);
  const type = readFirstString(record, ["type", "kind", "mime_type", "mimeType", "mime"]);
  const fileLikeType = ["file", "image", "audio", "video", "attachment", "artifact", "media"].includes(type) || type.includes("/");
  if (url || base64) {
    return Boolean(displayName || fileLikeType);
  }
  return Boolean(content && (explicitFileName || fileLikeType));
}

function normalizeOutputFile(value: unknown): Hub53AIOutputFile | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as Record<string, unknown>;
  const previewUrl = readFirstString(record, ["preview_url", "previewUrl"]);
  const downloadUrl = readFirstString(record, ["download_url", "downloadUrl"]);
  const signedDownloadUrl = readFirstString(record, ["signed_download_url", "signedDownloadUrl"]);
  const rawUrl = readFirstString(record, ["url", "href", "file_url", "fileUrl", "signed_url"]);
  const url = previewUrl || rawUrl || signedDownloadUrl || downloadUrl;
  const base64 = readFirstString(record, ["base64"]);
  const content = readFirstString(record, ["content", "data"]);
  if (!url && !base64 && !content) {
    return null;
  }

  const fileName =
    readFirstString(record, ["file_name", "fileName", "filename", "name", "path", "title"]) ||
    inferFileNameFromUrl(url) ||
    "file";
  const id = readFirstString(record, ["id", "artifact_id", "artifactId", "file_id", "fileId", "upload_file_id", "uploadFileId"]) || url || fileName;
  const mimeType = readFirstString(record, ["mime_type", "mimeType", "mime", "content_type", "contentType"]);
  const size = readFirstNumber(record, ["size", "file_size", "fileSize", "bytes"]);
  return {
    id,
    file_name: fileName,
    ...(url ? { url } : {}),
    ...(previewUrl ? { preview_url: previewUrl } : {}),
    ...(downloadUrl ? { download_url: downloadUrl } : {}),
    ...(signedDownloadUrl ? { signed_download_url: signedDownloadUrl } : {}),
    ...(readFirstString(record, ["preview_key", "previewKey"]) ? { preview_key: readFirstString(record, ["preview_key", "previewKey"]) } : {}),
    ...(readFirstString(record, ["artifact_id", "artifactId"]) ? { artifact_id: readFirstString(record, ["artifact_id", "artifactId"]) } : {}),
    ...(readFirstString(record, ["upload_file_id", "uploadFileId"]) ? { upload_file_id: readFirstString(record, ["upload_file_id", "uploadFileId"]) } : {}),
    ...(mimeType ? { mime_type: mimeType } : {}),
    ...(typeof size === "number" ? { size } : {}),
    ...(readFirstString(record, ["sha256", "sha_256", "content_sha256"]) ? { sha256: readFirstString(record, ["sha256", "sha_256", "content_sha256"]) } : {}),
    ...(base64 ? { base64 } : {}),
    ...(content && !base64 ? { content } : {}),
    ...(readFirstString(record, ["message_id", "messageId"]) ? { message_id: readFirstString(record, ["message_id", "messageId"]) } : {}),
    ...(readFirstString(record, ["source_kind", "sourceKind"]) ? { source_kind: readFirstString(record, ["source_kind", "sourceKind"]) } : {})
  };
}

function getOutputFileKeys(file: Hub53AIOutputFile): string[] {
  const address = file.preview_url || file.url || file.signed_download_url || file.download_url || "";
  const contentFingerprint = getOutputFileContentFingerprint(file);
  return [
    file.id ? `id:${file.id}` : "",
    contentFingerprint ? `content:${file.file_name}:${contentFingerprint}` : "",
    address || file.file_name ? `url-name:${address}|${file.file_name}` : "",
    file.file_name ? `name:${file.file_name}` : ""
  ].filter(Boolean);
}

function getOutputFilePartIdentityKey(file: Hub53AIOutputFile): string {
  const address = file.preview_url || file.url || file.signed_download_url || file.download_url || "";
  const id = typeof file.id === "string" ? file.id.trim() : "";
  if (file.file_name) {
    return `name:${file.file_name}`;
  }
  if (id.startsWith("local:")) {
    return `local-path:${id.slice("local:".length)}`;
  }
  if (address) {
    return `address:${address}|${file.file_name || ""}`;
  }
  if (id && !id.startsWith("local-") && !id.startsWith("local-history-")) {
    return `id:${id}`;
  }
  return id ? `id:${id}` : "";
}

function getOutputFileEmissionKey(file: Hub53AIOutputFile): string {
  const fields = [
    ["id", file.id || ""],
    ["artifact", file.artifact_id || ""],
    ["upload", file.upload_file_id || ""],
    ["name", file.file_name || ""],
    ["url", file.url || ""],
    ["preview", file.preview_url || ""],
    ["preview_key", file.preview_key || ""],
    ["download", file.download_url || ""],
    ["signed", file.signed_download_url || ""],
    ["mime", file.mime_type || ""],
    ["size", typeof file.size === "number" ? String(file.size) : ""],
    ["sha256", file.sha256 || ""],
    ["content", getOutputFileContentFingerprint(file)],
    ["message", file.message_id || ""],
    ["source", file.source_kind || ""]
  ];
  const hasIdentity = fields.some(([, value]) => value);
  if (!hasIdentity) {
    return "";
  }
  const parts = fields.map(([name, value]) => `${name}:${value}`);
  return `snapshot:${createHash("sha256").update(parts.join("\0")).digest("hex").slice(0, 24)}`;
}

function getOutputFileEmissionKeys(file: Hub53AIOutputFile): string[] {
  const keys = new Set<string>();
  const primaryKey = getOutputFileEmissionKey(file);
  if (primaryKey) {
    keys.add(primaryKey);
  }

  const fileName = typeof file.file_name === "string" ? file.file_name.trim() : "";
  const sha256 = typeof file.sha256 === "string" ? file.sha256.trim().toLowerCase() : "";
  const contentFingerprint = getOutputFileContentFingerprint(file);
  const size = typeof file.size === "number" && Number.isFinite(file.size) ? String(file.size) : "";
  if (fileName && sha256) {
    keys.add(`logical:${fileName}:sha256:${sha256}`);
  }
  if (fileName && contentFingerprint) {
    keys.add(`logical:${fileName}:content:${contentFingerprint}`);
  }
  if (fileName && size && (sha256 || contentFingerprint)) {
    keys.add(`logical:${fileName}:size:${size}:${sha256 || contentFingerprint}`);
  }
  return [...keys];
}

function getOutputFileContentFingerprint(file: Hub53AIOutputFile): string {
  const base64 = typeof file.base64 === "string" ? file.base64 : "";
  if (base64) {
    try {
      const bytes = Buffer.from(base64, "base64");
      if (bytes.length) {
        return createHash("sha256").update("bytes\0").update(bytes).digest("hex").slice(0, 24);
      }
    } catch {
      return createHash("sha256").update("base64\0").update(base64).digest("hex").slice(0, 24);
    }
  }
  const content = typeof file.content === "string" ? file.content : "";
  if (content) {
    return createHash("sha256").update("bytes\0").update(Buffer.from(content, "utf8")).digest("hex").slice(0, 24);
  }
  return "";
}

function readFirstString(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function readFirstNumber(record: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim()) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return undefined;
}

function inferFileNameFromUrl(url?: string): string {
  if (!url) {
    return "";
  }
  try {
    const parsed = new URL(url);
    const last = parsed.pathname.split("/").filter(Boolean).at(-1);
    return last ? decodeURIComponent(last) : "";
  } catch {
    const last = url.split("?")[0]?.split("/").filter(Boolean).at(-1);
    return last ? decodeURIComponent(last) : "";
  }
}

function resolveMediaKind(mimeType = "", fileName = ""): Hub53AIMediaAttachment["kind"] {
  const lower = mimeType.toLowerCase();
  if (lower.startsWith("image/")) return "image";
  if (lower.startsWith("audio/")) return "audio";
  if (lower.startsWith("video/")) return "video";
  if (lower.startsWith("text/")) return "text";
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  if (["png", "jpg", "jpeg", "gif", "webp", "svg"].includes(ext)) return "image";
  if (["mp3", "wav", "m4a", "ogg"].includes(ext)) return "audio";
  if (["mp4", "mov", "webm"].includes(ext)) return "video";
  if (["txt", "md", "csv", "json", "log"].includes(ext)) return "text";
  return "file";
}

function readStringMetadata(payload: unknown, key: string): string | undefined {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }
  const value = (payload as Record<string, unknown>)[key];
  return typeof value === "string" && value.trim() ? value : undefined;
}

function readBooleanMetadata(payload: unknown, key: string): boolean | undefined {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }
  const value = (payload as Record<string, unknown>)[key];
  return typeof value === "boolean" ? value : undefined;
}

function parseHeartbeat(rawPayload: string): "ping" | "pong" | null {
  try {
    const parsed = JSON.parse(rawPayload) as { action?: unknown };
    if (parsed.action === "ping" || parsed.action === "pong") {
      return parsed.action;
    }
  } catch {
    return null;
  }
  return null;
}

function isHub53AIBusinessHeartbeatMessage(message: Hub53AIIncomingMessage): boolean {
  const chatId = String(message.chatId || "").trim();
  if (chatId.endsWith(":heartbeat")) {
    return true;
  }
  if ((message.imageUrls?.length || 0) > 0 || (message.fileUrls?.length || 0) > 0 || (message.files?.length || 0) > 0) {
    return false;
  }
  const normalized = message.text.trim().replace(/\s+/g, " ").toUpperCase();
  return normalized === "HEARTBEAT_OK";
}

function extractTextFromContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((item) => {
      const record = toRecord(item);
      if (record.type === "text" && typeof record.text === "string") {
        return record.text;
      }
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

function extractImagesFromContent(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }
  return normalizeUrlList(
    undefined,
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type === "image_url") {
          return toRecord(record.image_url).url;
        }
        if (record.type === "image") {
          return record.url ?? toRecord(record.image).url;
        }
        return undefined;
      })
      .filter(Boolean)
  );
}

function extractFilesFromContent(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }
  return normalizeUrlList(
    undefined,
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type === "file") {
          return record.url ?? toRecord(record.file).url;
        }
        return undefined;
      })
      .filter(Boolean)
  );
}

function extractInputFilesFromContent(content: unknown, metadata?: unknown): Hub53AIInputFile[] {
  const fromMetadata = normalizeInputFiles(toRecord(metadata).openclaw_input_files);
  if (!Array.isArray(content)) {
    return fromMetadata;
  }
  const fromContent = normalizeInputFiles(
    content
      .map((item) => {
        const record = toRecord(item);
        if (record.type !== "file") {
          return undefined;
        }
        const nested = toRecord(record.file);
        return {
          ...nested,
          ...record
        };
      })
      .filter(Boolean)
  );
  return dedupeInputFiles([...fromMetadata, ...fromContent]);
}

function normalizeInputFiles(...sources: unknown[]): Hub53AIInputFile[] {
  const files: Hub53AIInputFile[] = [];
  for (const source of sources) {
    const list = Array.isArray(source) ? source : [];
    for (const item of list) {
      const normalized = normalizeInputFile(item);
      if (normalized) {
        files.push(normalized);
      }
    }
  }
  return dedupeInputFiles(files);
}

function normalizeInputFile(value: unknown): Hub53AIInputFile | undefined {
  if (typeof value === "string") {
    return { url: value };
  }
  const record = toRecord(value);
  const nested = toRecord(record.file);
  const merged = { ...nested, ...record };
  const url = stringOr(merged.signed_download_url, merged.download_url, merged.preview_url, merged.url);
  const id = stringOr(merged.id, merged.file_id, merged.upload_file_id, merged.content);
  const fileName = stringOr(merged.file_name, merged.filename, merged.name);
  if (!url && !id && !fileName) {
    return undefined;
  }
  return {
    ...(id ? { id, file_id: id } : {}),
    ...(fileName ? { file_name: fileName, filename: fileName, name: fileName } : {}),
    ...(url ? { url } : {}),
    ...(typeof merged.preview_url === "string" ? { preview_url: merged.preview_url } : {}),
    ...(typeof merged.download_url === "string" ? { download_url: merged.download_url } : {}),
    ...(typeof merged.signed_download_url === "string" ? { signed_download_url: merged.signed_download_url } : {}),
    ...(typeof merged.preview_key === "string" ? { preview_key: merged.preview_key } : {}),
    ...(typeof merged.mime_type === "string" ? { mime_type: merged.mime_type } : {}),
    ...(typeof merged.size === "number" ? { size: merged.size } : {})
  };
}

function dedupeInputFiles(files: Hub53AIInputFile[]): Hub53AIInputFile[] {
  const seen = new Set<string>();
  const output: Hub53AIInputFile[] = [];
  for (const file of files) {
    const key = getInputFileURL(file) || file.id || file.file_id || file.file_name || file.filename || "";
    if (key && seen.has(key)) {
      continue;
    }
    if (key) {
      seen.add(key);
    }
    output.push(file);
  }
  return output;
}

function getInputFileURL(file: Hub53AIInputFile): string {
  return stringOr(file.local_path, file.signed_download_url, file.download_url, file.preview_url, file.url);
}

function extractSkillSelection(...sources: unknown[]): Hub53AISkillSelection | undefined {
  for (const source of sources) {
    const record = toRecord(source);
    const raw = toRecord(record.openclaw_skill ?? record.skill);
    const skillName = stringOr(raw.skill_name, raw.name, record.skill_name);
    const skillID = stringOr(raw.skill_id, raw.id, record.skill_id);
    if (!skillName && !skillID) {
      continue;
    }
    return {
      ...(skillID ? { skill_id: skillID } : {}),
      ...(skillName ? { skill_name: skillName } : {}),
      ...(stringOr(raw.display_name, raw.label) ? { display_name: stringOr(raw.display_name, raw.label) } : {}),
      ensure: raw.ensure !== false
    };
  }
  return undefined;
}

function normalizeUrlList(primary: unknown, fallback: unknown): string[] {
  const source = Array.isArray(primary) ? primary : Array.isArray(fallback) ? fallback : [];
  return source
    .map((entry) => {
      if (typeof entry === "string") {
        return entry;
      }
      const record = toRecord(entry);
      return typeof record.url === "string" ? record.url : "";
    })
    .filter(Boolean);
}

function summarizeVisibleActivity(event: TimelineEvent): string | null {
  const data = toRecord(event.payload?.data);
  const name = String(
    event.payload?.name ??
      event.payload?.toolName ??
      event.payload?.skillName ??
      data.name ??
      data.toolName ??
      data.skillName ??
      ""
  ).trim();
  if (event.kind === "tool.call") {
    return name ? `Used tool ${name}` : "Used a tool";
  }
  if (event.kind === "tool.result") {
    return name ? `Tool ${name} returned a result` : "Tool returned a result";
  }
  const message = String(event.payload?.message ?? event.payload?.status ?? "").trim();
  return message || null;
}

function isSyntheticToolPlaceholderThinkingEvent(event: TimelineEvent): boolean {
  if (event.kind !== "assistant.thinking") {
    return false;
  }
  const content = String(event.payload?.content ?? "").replace(/\s+/g, " ").trim();
  if (!content) {
    return false;
  }
  const normalized = content.toLowerCase();
  if (normalized === "used a tool" || normalized === "tool returned a result") {
    return true;
  }
  return /^used tool\b/i.test(content) || /^tool .+ returned a result$/i.test(content);
}

function filterSyntheticToolPlaceholderThinkingEvents(events: TimelineEvent[]): TimelineEvent[] {
  return events.filter((event) => !isSyntheticToolPlaceholderThinkingEvent(event));
}

function validateConfig(config: Hub53AIConfig) {
  if (!config.botId) {
    throw new Error("hub53ai.botId is required when hub53ai.enabled is true");
  }
  if (!config.secret) {
    throw new Error("hub53ai.secret is required when hub53ai.enabled is true");
  }
  if (!config.wsUrl) {
    throw new Error("hub53ai.wsUrl is required when hub53ai.enabled is true");
  }
  if (!config.wsUrl.startsWith("ws://") && !config.wsUrl.startsWith("wss://")) {
    throw new Error("hub53ai.wsUrl must start with ws:// or wss://");
  }
}

function inferErrorCode(errorText: string): string {
  const lower = errorText.toLowerCase();
  if (lower.includes("timeout")) {
    return "TIMEOUT";
  }
  if (lower.includes("rate limit")) {
    return "RATE_LIMITED";
  }
  if (lower.includes("quota")) {
    return "INSUFFICIENT_QUOTA";
  }
  if (lower.includes("unauthorized") || lower.includes("access denied")) {
    return "ACCESS_DENIED";
  }
  if (lower.includes("websocket")) {
    return "WEBSOCKET_ERROR";
  }
  return "INTERNAL_ERROR";
}

function sanitizeWsUrl(wsUrl: string): string | undefined {
  if (!wsUrl) {
    return undefined;
  }
  try {
    const url = new URL(wsUrl);
    url.username = "";
    url.password = "";
    url.search = "";
    return url.toString();
  } catch {
    return wsUrl;
  }
}

function maskBotId(botId: string): string | undefined {
  if (!botId) {
    return undefined;
  }
  if (botId.length <= 4) {
    return `${botId.slice(0, 1)}***`;
  }
  return `${botId.slice(0, 2)}***${botId.slice(-2)}`;
}

function stringOr(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function dedupeStrings(values: Array<string | undefined | null>): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const value of values) {
    const trimmed = typeof value === "string" ? value.trim() : "";
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    output.push(trimmed);
  }
  return output;
}

function sanitizePathSegment(value: string): string {
  return (value || "item").replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^-+|-+$/g, "") || "item";
}

function sanitizeDownloadFileName(value: string): string {
  const cleaned = basename(value.replace(/\\/g, "/")).replace(/[\0\r\n]/g, "").trim();
  return cleaned && cleaned !== "." && cleaned !== "/" ? cleaned : `file-${Date.now()}`;
}

function identityStringOr(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return String(value);
    }
  }
  return "";
}

function numberOr(...values: unknown[]): number | undefined {
  for (const value of values) {
    const parsed = typeof value === "number" ? value : typeof value === "string" && value.trim() ? Number(value) : NaN;
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
