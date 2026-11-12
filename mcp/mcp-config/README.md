# Claude Desktop config examples

These are example entries for the MCP server in this project:

- `claude_desktop.remote.example.json` connects Claude Desktop to a deployed server over HTTPS.
- `claude_desktop.local.example.json` connects Claude Desktop to an HTTP server running on the same Mac.

Both use `mcp-remote` to bridge Claude Desktop's local stdio connection to this server's Streamable HTTP endpoint (`/mcp`). Install Node.js so `npx` is available to Claude Desktop.

## Configure the authorization header

1. Copy `headers.example` to `~/.config/mcp-machine-operation.headers`.
2. Replace the placeholder with the same `MCP_AUTH_TOKEN` configured on the MCP server:

   ```text
   Authorization: Bearer YOUR_TOKEN
   ```

3. Restrict access to the file:

   ```bash
   chmod 600 ~/.config/mcp-machine-operation.headers
   ```

The JSON examples use `/Users/USERNAME/...` as the path. Replace `USERNAME` with your macOS account name, or use the full path to the headers file you created.

## Remote server

Replace `https://DOMAIN-KAMU/mcp` in `claude_desktop.remote.example.json` with the public HTTPS endpoint of your deployed server.

## Local server

The local example assumes the MCP server is already running at `http://127.0.0.1:8000/mcp`. In this project, `main.py` runs an HTTP server; it does not start through Claude Desktop's stdio transport. Start it separately from the project directory, with a configured `.env` and the Python dependencies installed:

```bash
python main.py
```

Keep that process running while using Claude Desktop. The local example does not start PostgreSQL or the MCP server for you.

## Add to Claude Desktop

Open **Claude → Settings → Developer → Edit Config**. Copy the `mcpServers` entry from the example you want into your existing `claude_desktop_config.json`; merge it with existing entries rather than replacing them. Save the file, then restart Claude Desktop.
