import type { ChannelOnboardingAdapter } from "openclaw/plugin-sdk";
import { CHANNEL_ID } from "./const.js";

export const aiHubOnboardingAdapter: ChannelOnboardingAdapter = {
  channel: CHANNEL_ID,
  getStatus: async ({ cfg }) => {
    const config = (cfg.channels?.[CHANNEL_ID] ?? {}) as any;
    const configured = Boolean(config.botId || config.token || config.secret);
    
    return {
      channel: CHANNEL_ID,
      configured,
      statusLines: configured 
        ? ["53AIHub 智能机器人已激活"]
        : ["53AIHub 智能机器人未配置"],
      quickstartScore: configured ? 100 : 0,
    };
  },
  configure: async ({ cfg }) => {
    return { cfg };
  },
};