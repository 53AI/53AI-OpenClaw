import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";

import { aiHubPlugin } from "./src/channel.js";
import { setRuntime } from "./src/runtime.js";

const plugin = {
  id: "53aihub",
  name: "53AIHub",
  description: "53AIHub (AgentHub) OpenClaw 插件",
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi) {
    setRuntime(api.runtime);
    api.registerChannel({ plugin: aiHubPlugin });
  },
};

export default plugin;
