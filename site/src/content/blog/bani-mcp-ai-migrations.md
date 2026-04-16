---
title: "AI-Powered Database Migrations with Bani's MCP Server"
description: "How Bani's built-in MCP server enables AI agents like Claude to inspect databases, generate migration configs, and run migrations through natural language."
date: 2026-04-09
author: "David Mugume"
tags: ["mcp", "ai", "claude", "automation"]
---

One of Bani's most distinctive features is its built-in MCP (Model Context Protocol) server. MCP is an open standard created by Anthropic that allows AI assistants to interact with external tools and services. Bani's MCP server exposes 10 tools that let AI agents inspect databases, generate migration configurations, validate them, and run migrations — all through natural language conversation.

This post explains what the MCP server does, how to set it up, and what an AI-driven migration workflow looks like in practice.

## What is the Model Context Protocol?

MCP is a protocol that standardizes how AI assistants communicate with external tools. Instead of building custom integrations for every AI platform, a tool can implement the MCP server protocol once and work with any MCP-compatible client.

Think of it as a USB standard for AI tools. Bani implements the MCP server; AI clients like Claude Desktop, Cursor, and others implement the MCP client. The protocol handles discovery (what tools are available?), invocation (run this tool with these parameters), and results (here is what happened).

## Bani's 10 MCP tools

Bani's MCP server exposes the following tools:

### Inspection tools
1. **`inspect_source`** — Read the schema of the source database (tables, columns, types, indexes)
2. **`inspect_target`** — Read the schema of the target database
3. **`compare_schemas`** — Compare source and target schemas, highlighting differences

### Configuration tools
4. **`generate_bdl`** — Generate a BDL (Bani Definition Language) configuration based on source and target schemas
5. **`validate_bdl`** — Validate a BDL configuration for correctness and completeness
6. **`preview_migration`** — Show what a migration would do without executing it

### Execution tools
7. **`run_migration`** — Execute a migration
8. **`get_status`** — Check the status of a running migration
9. **`list_connectors`** — List available database connectors and their capabilities

### Utility tools
10. **`connector_info`** — Get detailed information about a specific connector

## Setting up the MCP server

Setting up Bani's MCP server with Claude Desktop takes about two minutes.

### 1. Install Bani

```bash
pip install bani
```

### 2. Configure Claude Desktop

Open your Claude Desktop configuration file and add the Bani MCP server:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "bani": {
      "command": "bani",
      "args": ["mcp", "serve"],
      "env": {
        "SOURCE_DSN": "postgresql://user:password@localhost:5432/mydb",
        "TARGET_DSN": "mysql://user:password@localhost:3306/mydb"
      }
    }
  }
}
```

### 3. Restart Claude Desktop

After saving the configuration, restart Claude Desktop. You should see Bani's tools available in the tool picker.

## The agent workflow

With the MCP server configured, you can drive an entire migration through conversation. Here is a typical workflow:

### Step 1: Inspect the source

You tell the AI agent: *"Inspect my source database and show me the schema."*

The agent calls `inspect_source`, which reads the database schema and returns a structured representation of all tables, columns, types, indexes, and constraints. The agent then presents this information in a readable format.

### Step 2: Inspect the target (optional)

If you are migrating to an existing database, you might ask: *"Now inspect the target and compare it to the source."*

The agent calls `inspect_target` and then `compare_schemas` to identify what already exists in the target, what needs to be created, and what differs between the two schemas.

### Step 3: Generate the configuration

You tell the agent: *"Generate a migration configuration for all tables."*

The agent calls `generate_bdl`, which produces a complete BDL configuration file based on the source and target schemas. The agent shows you the configuration and explains the type mappings it chose.

### Step 4: Review and refine

You might say: *"Exclude the audit_logs table and rename the users table to app_users in the target."*

The agent modifies the BDL configuration accordingly and calls `validate_bdl` to make sure the changes are valid.

### Step 5: Preview

Before running the migration, you ask: *"Show me a preview of what this migration will do."*

The agent calls `preview_migration`, which returns a dry-run summary: how many tables, how many rows estimated, what schema changes will be applied. No data is modified.

### Step 6: Execute

When you are satisfied, you say: *"Run the migration."*

The agent calls `run_migration` and then periodically calls `get_status` to report progress. You see real-time updates on which tables are being migrated, how many rows have been processed, and the estimated time remaining.

## Security model

Bani's MCP server is designed with security in mind:

**Environment variable references only.** Database credentials are passed as environment variables in the Claude Desktop configuration, not in conversation. The AI agent never sees plaintext passwords.

**Local execution.** The MCP server runs on your local machine. No data leaves your network. The AI agent sends tool invocations to the local MCP server, which connects directly to your databases.

**Preview before execute.** The `preview_migration` tool lets you see exactly what will happen before any data is modified. The agent can explain the preview and answer questions before you approve execution.

**Read-only inspection.** The `inspect_source` and `inspect_target` tools are read-only operations. They read schema metadata only — they do not read or modify data.

## Beyond Claude Desktop

While Claude Desktop is the most common MCP client, Bani's MCP server works with any MCP-compatible tool. Cursor, Windsurf, and other AI-powered development environments that support MCP can use Bani's tools to drive migrations from within their interfaces.

The MCP protocol is also straightforward to integrate into custom applications. If you are building an AI agent that needs database migration capabilities, Bani's MCP server provides a ready-made tool set.

## Getting started

To try AI-powered migrations with Bani:

1. Install Bani: `pip install bani`
2. Configure the MCP server in your AI client of choice
3. Start a conversation: *"Inspect my source database"*

For the complete MCP setup guide, visit [docs.bani.tools/en/latest/guides/mcp-server/](https://docs.bani.tools/en/latest/guides/mcp-server/).

For general getting-started instructions, see [docs.bani.tools/en/latest/getting-started/](https://docs.bani.tools/en/latest/getting-started/).

The source code is available on [GitHub](https://github.com/mugumedavid/bani) under the Apache-2.0 license.
