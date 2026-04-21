---
title: "AI-Powered Database Migrations with Bani's MCP Server"
description: "How Bani's built-in MCP server enables AI agents like Claude to inspect databases, generate migration configs, and run migrations through natural language."
date: 2026-04-09
author: "David Mugume"
tags: ["mcp", "ai", "claude", "automation"]
---

One of Bani's most distinctive features is its built-in MCP (Model Context Protocol) server. MCP is an open standard created by Anthropic that allows AI assistants to interact with external tools and services. Bani's MCP server exposes 10 tools that let AI agents inspect databases, generate migration configurations, validate them, and run migrations -- all through natural language conversation.

This post explains what the MCP server does, how to set it up, and what an AI-driven migration workflow looks like in practice.

## What is the Model Context Protocol?

MCP is a protocol that standardizes how AI assistants communicate with external tools. Instead of building custom integrations for every AI platform, a tool can implement the MCP server protocol once and work with any MCP-compatible client.

Think of it as a USB standard for AI tools. Bani implements the MCP server; AI clients like Claude Desktop, Cursor, and others implement the MCP client. The protocol handles discovery (what tools are available?), invocation (run this tool with these parameters), and results (here is what happened).

## Bani's 10 MCP tools

### Discovery
1. **`bani_connections`** -- List all saved database connections by name
2. **`bani_connectors_list`** -- List available connector engines (postgresql, mysql, etc.)
3. **`bani_connector_info`** -- Get details about a specific connector's capabilities

### Inspection
4. **`bani_schema_inspect`** -- Introspect a database schema (tables, columns, indexes, foreign keys)

### Configuration
5. **`bani_generate_bdl`** -- Generate a BDL migration definition from source and target connections
6. **`bani_validate_bdl`** -- Validate a BDL document for correctness
7. **`bani_save_project`** -- Save a BDL project to disk

### Execution
8. **`bani_preview`** -- Preview sample data from the source before migrating
9. **`bani_run`** -- Execute a saved migration project
10. **`bani_status`** -- Check the checkpoint status of a migration

## Setting up the MCP server

Setting up Bani's MCP server with Claude Desktop takes about two minutes.

### 1. Install Bani and save your database connections

Install Bani using any method (platform installer, Docker, or pip). Then open the Web UI (`bani ui`) and add your database connections on the Connections page. Give each connection a name you will recognise, like "production-mysql" or "staging-pg".

These saved connections are what the MCP server uses -- the AI agent references them by name during conversations.

### 2. Configure Claude Desktop

Open your Claude Desktop configuration file and add the Bani MCP server:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "bani": {
      "command": "bani",
      "args": ["mcp", "serve"]
    }
  }
}
```

No environment variables or credentials are passed in this config. The MCP server reads connections from the local Bani connections registry (`~/.bani/connections.json`) that you set up via the Web UI.

### 3. Restart Claude Desktop

After saving the configuration, restart Claude Desktop. You should see Bani's tools available in the tool picker.

## The agent workflow

With the MCP server configured, you can drive an entire migration through conversation. Here is a typical workflow:

### Step 1: Discover connections

You tell the AI agent: *"What databases do I have configured in Bani?"*

The agent calls `bani_connections`, which returns the names and metadata (host, port, database) of all your saved connections. Credentials are never exposed to the agent.

### Step 2: Inspect a database

You say: *"Inspect the schema of my production-mysql connection."*

The agent calls `bani_schema_inspect` with the connection name. It returns all tables, columns, types, indexes, and foreign keys. The agent presents this in a readable format.

### Step 3: Generate a migration

You tell the agent: *"Generate a migration from production-mysql to staging-pg for all tables."*

The agent calls `bani_generate_bdl`, which produces a complete BDL configuration based on the source and target connection details. The agent shows you the configuration and explains the type mappings it chose.

### Step 4: Review and refine

You might say: *"Exclude the audit_logs table and rename the users table to app_users in the target."*

The agent modifies the BDL accordingly and calls `bani_validate_bdl` to confirm the changes are valid.

### Step 5: Save and run

When you are satisfied, you say: *"Save and run the migration."*

The agent calls `bani_save_project` to save the BDL to disk, then `bani_run` to execute it. If the client supports progress notifications, you see real-time updates on which tables are being migrated, how many rows have been processed, and the estimated time remaining.

### Step 6: Check status

If you come back later, you can ask: *"What is the status of my migration?"*

The agent calls `bani_status` to check the checkpoint and report which tables completed, failed, or are pending.

## Security model

**Saved connections, not passwords in config.** Database credentials live in Bani's local connections registry, not in the Claude Desktop config file and not in conversation. The AI agent references connections by name and never sees plaintext passwords.

**Local execution.** The MCP server runs on your local machine. No data leaves your network. The AI agent sends tool invocations to the local MCP server, which connects directly to your databases.

**Preview before execute.** The `bani_preview` tool lets you inspect sample data before committing to a full migration. The agent can explain what it sees and answer questions before you approve execution.

**Read-only inspection.** The `bani_schema_inspect` tool is a read-only operation. It reads schema metadata only -- it does not read or modify data.

## Beyond Claude Desktop

While Claude Desktop is the most common MCP client, Bani's MCP server works with any MCP-compatible tool. Cursor, Windsurf, and other AI-powered development environments that support MCP can use Bani's tools to drive migrations from within their interfaces.

The MCP protocol is also straightforward to integrate into custom applications. If you are building an AI agent that needs database migration capabilities, Bani's MCP server provides a ready-made tool set.

## Getting started

To try AI-powered migrations with Bani:

1. Install Bani and open the Web UI to save your database connections
2. Configure the MCP server in your AI client of choice
3. Start a conversation: *"What databases do I have in Bani?"*

For the complete MCP setup guide, visit [docs.bani.tools/en/latest/guides/mcp-server/](https://docs.bani.tools/en/latest/guides/mcp-server/).

For general getting-started instructions, see [docs.bani.tools/en/latest/getting-started/](https://docs.bani.tools/en/latest/getting-started/).

The source code is available on [GitHub](https://github.com/mugumedavid/bani) under the Apache-2.0 license.
