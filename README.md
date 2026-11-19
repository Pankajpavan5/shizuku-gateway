# Shizuku Relay

One Rust server, one Kotlin app. AI clients call MCP tools. The phone runs the
tools through Shizuku. The phone connects out, so there is no open port on the
phone and no adb host.

```
AI client --HTTP--> relay (Render) <--WebSocket-- phone app --Shizuku--> Android
```

## What is in the box

| Part        | Path          | Language | Job                                        |
| ----------- | ------------- | -------- | ------------------------------------------ |
| Gateway     | `relay/`      | Rust     | MCP over HTTP, device link, serial queue   |
| Device app  | `android/`    | Kotlin   | Shizuku shell, approval, heartbeat, retry  |
| Deploy file | `render.yaml` | -        | Render free plan, Docker, health check     |

Tools: `doctor`, `adb_shell`, `device_info`, `ui_dump`, `screenshot`, `input`,
`current_app`, `screen_state`, `observe`, `notifications`.

## 1. Deploy the relay to Render

1. Push this folder to a GitHub repo.
2. In Render: **New > Blueprint**, pick the repo. Render reads `render.yaml`.
3. Set two secrets when Render asks:

```
MCP_HTTP_TOKEN=<random 48 hex chars>
RELAY_TOKEN=<random 48 hex chars>
```

Make them with `openssl rand -hex 24`. Use different values. Both must be 16+
chars or the server exits.

4. Deploy. The image is built from `relay/Dockerfile`. The health path is
   `/healthz`.

Free plan facts: 512 MB RAM, the service sleeps after about 15 minutes with no
traffic, and it wakes on the next request in about 30 seconds. The phone link
drops when the service sleeps. The app reconnects by itself.

## 2. Build the phone app

The app needs an Android SDK. This device has none. Two ways:

- **GitHub**: push, then open **Actions > android**. The workflow builds
  `app-release.apk`. Download it from the run page or from Releases.
- **Local**: `gradle -p android assembleDebug` with a JDK 17 and Android SDK 35.

Install the APK. Install and start the Shizuku app first. Shizuku must be
started through wireless debugging or adb; Shizuku shows how.

In the app:

1. URL: `wss://<your-service>.onrender.com/relay`
2. Relay token: the `RELAY_TOKEN` value.
3. Keep **Ask before each command** on. You get a notification with
   Approve/Deny for each command. No answer in 30 s means deny.
4. Press **Start** and grant Shizuku permission.

Allow notifications for the app. Without it, approval prompts cannot show and
commands are denied. The app shows a fixed notification while it runs. It stops after 30 idle
minutes, or from the **Stop** action.

## 3. Connect an MCP client

HTTP MCP endpoint: `https://<your-service>.onrender.com/mcp`, header
`Authorization: Bearer <MCP_HTTP_TOKEN>`.

Claude Code / generic JSON config:

```json
{
  "mcpServers": {
    "android": {
      "type": "http",
      "url": "https://<your-service>.onrender.com/mcp",
      "headers": { "Authorization": "Bearer <MCP_HTTP_TOKEN>" }
    }
  }
}
```

ChatGPT and other connector UIs: pick "custom connector", paste the `/mcp`
URL, and set the Bearer header.

Then ask the model: *"Run doctor."* You should get a UID and an SDK number from
the phone.

## 4. Check by hand

```
curl -s https://<service>.onrender.com/healthz
# {"device":"connected","info":"{\"model\":\"SM-A346E\",...}","ok":true}

curl -s https://<service>.onrender.com/mcp \
  -H "Authorization: Bearer $MCP_HTTP_TOKEN" \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## Rules that the code enforces

- MCP calls need `MCP_HTTP_TOKEN`. The device link needs `RELAY_TOKEN`. Token
  compare has no early exit.
- One command runs at a time. The relay waits up to 75 s (30 s approval + 30 s shell), then fails.
- Output cap: 256 KB. Screenshot cap: 8 MB. Command cap: 4096 bytes.
- `input text` accepts ASCII letters, digits and `.,:/@_+-` only. Spaces become
  `%s`. Shell text is never passed through.
- No logs of command output. The Rust server writes one start line only.
- The phone link drops after 5 minutes of silence. The server sends a WebSocket
  ping every 30 s to check the link. The link accepts the token in the
  `Authorization: Bearer` header only.

## Limits (known, not hidden)

- **First run is the real test.** The relay passes its tests, but Shizuku paths
  are untested on hardware from here. Run `doctor` first.
- The release APK is signed with the key in the repo secrets. Updates install
  over the old version. Keep the keystore safe: a new key means a new app.
- No update checker and no in-app update flow yet.
- The free Render plan has no disk. Nothing is stored. Saved commands and logs
  are not built yet.
- `adb_shell` is a full shell. Guard `MCP_HTTP_TOKEN` like a password. Keep
  approval on.
