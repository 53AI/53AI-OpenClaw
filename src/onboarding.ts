import {
  addWildcardAllowFrom,
  type ChannelOnboardingAdapter,
  type ChannelOnboardingDmPolicy,
  type OpenClawConfig,
  type WizardPrompter,
} from "openclaw/plugin-sdk";
import { CHANNEL_ID } from "./const.js";
import type { AIHubConfig } from "./utils.js";
import { resolveAccount, setAccount } from "./utils.js";

const channel = CHANNEL_ID;

async function noteSetupHelp(prompter: WizardPrompter): Promise<void> {
  await prompter.note(
    [
      "53AIHub 智能机器人需要以下配置信息：",
      "1. Bot ID: 机器人标识",
      "2. Secret: 机器人密钥",
      "3. WebSocket URL: WebSocket 连接地址",
    ].join("\n"),
    "53AIHub 设置",
  );
}

async function promptBotId(
  prompter: WizardPrompter,
  account: ReturnType<typeof resolveAccount> | null,
): Promise<string> {
  return String(
    await prompter.text({
      message: "53AIHub 机器人 Bot ID",
      initialValue: account?.botId ?? "",
      validate: (value) => (value?.trim() ? undefined : "必填"),
    }),
  ).trim();
}

async function promptSecret(
  prompter: WizardPrompter,
  account: ReturnType<typeof resolveAccount> | null,
): Promise<string> {
  return String(
    await prompter.text({
      message: "53AIHub 机器人 Secret",
      initialValue: account?.secret ?? "",
      validate: (value) => (value?.trim() ? undefined : "必填"),
    }),
  ).trim();
}

async function promptWebsocketUrl(
  prompter: WizardPrompter,
  account: ReturnType<typeof resolveAccount> | null,
): Promise<string> {
  return String(
    await prompter.text({
      message: "WebSocket URL (例如: ws://localhost:8080/ws)",
      initialValue: account?.websocketUrl ?? "",
      validate: (value) => {
        const trimmed = value?.trim();
        if (!trimmed) return undefined;
        if (!trimmed.startsWith("ws://") && !trimmed.startsWith("wss://")) {
          return "URL 必须以 ws:// 或 wss:// 开头";
        }
        return undefined;
      },
    }),
  ).trim();
}

function setAccessPolicy(
  cfg: OpenClawConfig,
  accessPolicy: "pairing" | "allowlist" | "open" | "disabled",
): OpenClawConfig {
  const account = resolveAccount(cfg);
  const existingAllowFrom = account.config.allowFrom ?? [];
  const allowFrom =
    accessPolicy === "open"
      ? addWildcardAllowFrom(existingAllowFrom.map((x) => String(x)))
      : existingAllowFrom.map((x) => String(x));

  return setAccount(cfg, {
    accessPolicy,
    allowFrom,
  });
}

const dmPolicy: ChannelOnboardingDmPolicy = {
  label: "53AIHub",
  channel,
  policyKey: `channels.${CHANNEL_ID}.accessPolicy`,
  allowFromKey: `channels.${CHANNEL_ID}.allowFrom`,
  getCurrent: (cfg) => {
    const account = resolveAccount(cfg);
    return account.config.accessPolicy ?? "open";
  },
  setPolicy: (cfg, policy) => {
    return setAccessPolicy(cfg, policy);
  },
  promptAllowFrom: async ({ cfg, prompter }) => {
    const account = resolveAccount(cfg);
    const existingAllowFrom = account.config.allowFrom ?? [];

    const entry = await prompter.text({
      message: "允许使用的用户ID（每行一个）",
      placeholder: "user123",
      initialValue: existingAllowFrom[0] ? String(existingAllowFrom[0]) : undefined,
    });

    const allowFrom = String(entry ?? "")
      .split(/[\n,;]+/g)
      .map((s) => s.trim())
      .filter(Boolean);

    return setAccount(cfg, { allowFrom });
  },
};

export const aiHubOnboardingAdapter: ChannelOnboardingAdapter = {
  channel,
  getStatus: async ({ cfg }) => {
    const account = resolveAccount(cfg);
    const configured = Boolean(
      account.botId?.trim() &&
      account.secret?.trim()
    );

    return {
      channel,
      configured,
      statusLines: [`53AIHub: ${configured ? "已配置" : "需要 Bot ID 和 Secret"}`],
      selectionHint: configured ? "已配置" : "需要设置",
    };
  },
  configure: async ({ cfg, prompter, forceAllowFrom }) => {
    const account = resolveAccount(cfg);

    if (!account.botId?.trim() || !account.secret?.trim()) {
      await noteSetupHelp(prompter);
    }

    const botId = await promptBotId(prompter, account);
    const secret = await promptSecret(prompter, account);
    const websocketUrl = await promptWebsocketUrl(prompter, account);

    const cfgWithAccount = setAccount(cfg, {
      botId,
      secret,
      websocketUrl: websocketUrl || undefined,
      enabled: true,
      accessPolicy: account.config.accessPolicy ?? "open",
      allowFrom: account.config.allowFrom ?? [],
      sendThinkingMessage: account.sendThinkingMessage ?? true,
    });

    return { cfg: cfgWithAccount };
  },
  dmPolicy,
  disable: (cfg) => {
    return setAccount(cfg, { enabled: false });
  },
};