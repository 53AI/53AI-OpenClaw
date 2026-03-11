import {
  DEFAULT_ACCOUNT_ID,
  formatPairingApproveHint,
  type ChannelPlugin,
  type ChannelStatusIssue,
  type OpenClawConfig,
} from "openclaw/plugin-sdk";

import { getRuntime } from "./runtime.js";
import { monitorProvider } from "./monitor.js";
import { getWebSocket } from "./state-manager.js";
import type { AIHubConfig, ResolvedAccount } from "./utils.js";
import { resolveAccount } from "./utils.js";
import { CHANNEL_ID, TEXT_CHUNK_LIMIT } from "./const.js";
import { sendDirectMessage } from "./message-sender.js";
import { aiHubOnboardingAdapter } from "./onboarding.js";

const meta = {
  id: CHANNEL_ID,
  label: "53AIHub",
  selectionLabel: "53AIHub (AgentHub)",
  detailLabel: "53AIHub 智能机器人",
  docsPath: `/channels/${CHANNEL_ID}`,
  docsLabel: CHANNEL_ID,
  blurb: "53AIHub 智能机器人接入插件",
  systemImage: "message.fill",
};

export const aiHubPlugin: ChannelPlugin<ResolvedAccount> = {
  id: CHANNEL_ID,
  meta: {
    ...meta,
    quickstartAllowFrom: true,
  },
  pairing: {
    idLabel: "userId",
    normalizeAllowEntry: (entry) => entry.replace(new RegExp(`^(${CHANNEL_ID}|user):`, "i"), "").trim(),
    notifyApproval: async ({ cfg, id }) => {
      console.log(`[53aihub] Pairing approved for user: ${id}`);
    },
  },
  onboarding: aiHubOnboardingAdapter,
  capabilities: {
    chatTypes: ["direct"],
    reactions: false,
    threads: false,
    media: false,
    nativeCommands: false,
    blockStreaming: true,
  },
  reload: { configPrefixes: [`channels.${CHANNEL_ID}`] },
  config: {
    listAccountIds: () => [DEFAULT_ACCOUNT_ID],
    resolveAccount: (cfg) => resolveAccount(cfg),
    defaultAccountId: () => DEFAULT_ACCOUNT_ID,
    setAccountEnabled: ({ cfg, enabled }) => {
      const config = (cfg.channels?.[CHANNEL_ID] ?? {}) as AIHubConfig;
      return {
        ...cfg,
        channels: {
          ...cfg.channels,
          [CHANNEL_ID]: { ...config, enabled },
        },
      };
    },
    deleteAccount: ({ cfg }) => {
      const config = (cfg.channels?.[CHANNEL_ID] ?? {}) as AIHubConfig;
      const { botId, secret, token, ...rest } = config;
      return {
        ...cfg,
        channels: {
          ...cfg.channels,
          [CHANNEL_ID]: rest,
        },
      };
    },
    isConfigured: (account) =>
      Boolean((account.botId?.trim() || account.token?.trim()) || account.secret?.trim()),
    describeAccount: (account) => ({
      accountId: account.accountId,
      name: account.name,
      enabled: account.enabled,
      configured: Boolean(account.botId?.trim() || account.token?.trim()),
      botId: account.botId,
      websocketUrl: account.websocketUrl,
      accessPolicy: account.config.accessPolicy ?? "open",
    }),
    resolveAllowFrom: ({ cfg }) => {
      const account = resolveAccount(cfg);
      return (account.config.allowFrom ?? []).map((entry) => String(entry));
    },
    formatAllowFrom: ({ allowFrom }) =>
      allowFrom.map((entry) => String(entry).trim()).filter(Boolean),
  },
  security: {
    resolveDmPolicy: ({ account }) => {
      const basePath = `channels.${CHANNEL_ID}.`;
      const accessPolicy = account.config.accessPolicy ?? "open";
      return {
        policy: accessPolicy === "pairing" ? "pairing" : accessPolicy === "allowlist" ? "allowlist" : "open",
        allowFrom: account.config.allowFrom ?? [],
        policyPath: `${basePath}accessPolicy`,
        allowFromPath: basePath,
        approveHint: formatPairingApproveHint(CHANNEL_ID),
        normalizeEntry: (raw) => raw.replace(new RegExp(`^${CHANNEL_ID}:`, "i"), "").trim(),
      };
    },
    collectWarnings: ({ account }) => {
      const warnings: string[] = [];
      const accessPolicy = account.config.accessPolicy ?? "open";
      
      if (accessPolicy === "open") {
        warnings.push(
          `- 访问策略为 "open"，所有用户都可以使用机器人`
        );
      }
      
      return warnings;
    },
  },
  messaging: {
    normalizeTarget: (target) => {
      const trimmed = target.trim();
      if (!trimmed) return undefined;
      return trimmed;
    },
    targetResolver: {
      looksLikeId: (id) => Boolean(id?.trim()),
      hint: "<userId|chatId>",
    },
  },
  directory: {
    self: async () => null,
    listPeers: async () => [],
    listGroups: async () => [],
  },
  outbound: {
    deliveryMode: "direct",
    chunker: (text, limit) => getRuntime().channel.text.chunkMarkdownText(text, limit),
    textChunkLimit: TEXT_CHUNK_LIMIT,
    sendText: async ({ to, text, accountId, ...rest }) => {
      const wsClient = getWebSocket(accountId ?? DEFAULT_ACCOUNT_ID);
      if (!wsClient) {
        throw new Error(`[53aihub] WS Client not connected for account ${accountId}`);
      }
      
      const channelPrefix = new RegExp(`^${CHANNEL_ID}:`, "i");
      const targetId = to.replace(channelPrefix, "");

      await sendDirectMessage(wsClient, targetId, text);
      return {
        channel: CHANNEL_ID,
        messageId: `msg-${Date.now()}`,
        chatId: targetId,
      };
    },
    sendMedia: async ({ to, text, mediaUrl, accountId, ...rest }) => {
      const wsClient = getWebSocket(accountId ?? DEFAULT_ACCOUNT_ID);
      if (!wsClient) {
        throw new Error(`[53aihub] WS Client not connected for account ${accountId}`);
      }
      const channelPrefix = new RegExp(`^${CHANNEL_ID}:`, "i");
      const targetId = to.replace(channelPrefix, "");

      const content = `[不支持发送媒体文件]\n${text ? `${text}\n${mediaUrl}` : (mediaUrl ?? "")}`;
      await sendDirectMessage(wsClient, targetId, content);
      
      return { channel: CHANNEL_ID, messageId: `msg-${Date.now()}`, chatId: targetId };
    },
  },
  status: {
    defaultRuntime: {
      accountId: DEFAULT_ACCOUNT_ID,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    },
    collectStatusIssues: (accounts): ChannelStatusIssue[] =>
      accounts.flatMap((entry) => {
        const accountId = String(entry.accountId ?? DEFAULT_ACCOUNT_ID);
        if (!entry.enabled) return [];
        
        const issues: ChannelStatusIssue[] = [];
        if (!entry.configured) {
          issues.push({
            channel: CHANNEL_ID,
            accountId,
            kind: "config",
            message: "53AIHub 访问令牌未配置",
            fix: "Run: openclaw channels add 53aihub --token <your_token>",
          });
        }
        return issues;
      }),
    buildChannelSummary: ({ snapshot }) => ({
      configured: snapshot.configured ?? false,
      running: snapshot.running ?? false,
      lastStartAt: snapshot.lastStartAt ?? null,
      lastStopAt: snapshot.lastStopAt ?? null,
      lastError: snapshot.lastError ?? null,
    }),
    probeAccount: async () => ({ ok: true, status: 200 }),
    buildAccountSnapshot: ({ account, runtime }) => ({
      accountId: account.accountId,
      name: account.name,
      enabled: account.enabled,
      configured: Boolean(account.botId?.trim() || account.token?.trim() || account.secret?.trim()),
      running: runtime?.running ?? false,
      lastStartAt: runtime?.lastStartAt ?? null,
      lastStopAt: runtime?.lastStopAt ?? null,
      lastError: runtime?.lastError ?? null,
    }),
  },
  gateway: {
    startAccount: async (ctx) => {
      return monitorProvider({
        account: ctx.account,
        config: ctx.cfg,
        runtime: ctx.runtime,
        abortSignal: ctx.abortSignal,
      });
    },
    logoutAccount: async ({ cfg }) => {
      const nextCfg = { ...cfg } as OpenClawConfig;
      const config = (cfg.channels?.[CHANNEL_ID] ?? {}) as AIHubConfig;
      const nextConfig = { ...config };
      let cleared = false;
      let changed = false;

      if (nextConfig.botId || nextConfig.secret || nextConfig.token) {
        delete nextConfig.botId;
        delete nextConfig.secret;
        delete nextConfig.token;
        cleared = true;
        changed = true;
      }

      if (changed) {
        if (Object.keys(nextConfig).length > 0) {
          nextCfg.channels = { ...nextCfg.channels, [CHANNEL_ID]: nextConfig };
        } else {
          const nextChannels = { ...nextCfg.channels };
          delete (nextChannels as Record<string, unknown>)[CHANNEL_ID];
          nextCfg.channels = Object.keys(nextChannels).length > 0 ? nextChannels : undefined;
        }
        await getRuntime().config.writeConfigFile(nextCfg);
      }

      const resolved = resolveAccount(changed ? nextCfg : cfg);
      const loggedOut = !resolved.botId && !resolved.secret && !resolved.config.token;

      return { cleared, envToken: false, loggedOut };
    },
  },
};