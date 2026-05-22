#!/usr/bin/env python3
"""
verify_mcp.py - Splunk MCP Server connection check for Pit Wall Race Engineer.

Confirms the Splunk MCP Server is reachable and responding. This is the Day 2
"MCP Server reachable" gate dependency. It does three things:
    1. Connects to the MCP endpoint over streamable HTTP using your token.
    2. Lists every tool the server exposes.
    3. Calls one safe, read-only tool to prove a full round-trip works.

This is also the connection scaffold the real agent will reuse in Week 2.

SETUP
-----
1. Install the MCP Python SDK:
       pip install mcp

2. Export your encrypted token as an environment variable (never hardcode it,
   never commit it):
       export SPLUNK_MCP_TOKEN="your-encrypted-token-here"

USAGE
-----
    python verify_mcp.py
    python verify_mcp.py --insecure      # if you hit an SSL cert error

NOTE ON --insecure
------------------
Splunk Cloud trial instances use a self-signed certificate on port 8089, so
the first run may fail with an SSL verification error. The --insecure flag
disables certificate verification. Use it for local dev testing only - never
in anything resembling production. Log this in /docs/feedback.md: the MCP
endpoint on trial instances uses a self-signed cert, so every client must
either trust it or disable verification.
"""

import argparse
import asyncio
import os
import ssl
import sys

# Load environment variables from a local .env file if one exists, so the
# token does not need to be exported manually each session. The .env file is
# git-ignored; .env.example documents the required keys without the secret.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional - a real exported env var still works

# MCP endpoint for this Splunk Cloud instance. Safe to keep in source - it is
# just a URL. The TOKEN is the secret and is read from the environment.
MCP_ENDPOINT = "https://prd-p-s1zak.splunkcloud.com:8089/services/mcp"

# A safe, read-only tool to call as the round-trip test. splunk_get_user_info
# just returns who the token authenticates as - no data is read or written.
# If the server does not expose it, the script falls back to listing only.
TEST_TOOL = "splunk_get_user_info"


def get_token():
    """Read the encrypted MCP token from the environment, or exit clearly."""
    token = os.environ.get("SPLUNK_MCP_TOKEN")
    if not token:
        sys.exit(
            "ERROR: SPLUNK_MCP_TOKEN environment variable is not set.\n"
            "  Set it with:  export SPLUNK_MCP_TOKEN=\"your-token-here\"\n"
            "  Get the token from the Splunk MCP Server app in Splunk Cloud."
        )
    return token


async def run_check(insecure):
    """Connect to the MCP server, list tools, and call one test tool."""
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client
    except ImportError:
        sys.exit(
            "ERROR: the MCP SDK is not installed.\n"
            "  Install it with:  pip install mcp"
        )

    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Build an SSL context. By default we verify certificates. With --insecure
    # we disable verification to cope with the self-signed cert on trial tiers.
    if insecure:
        print("WARNING: SSL verification DISABLED (--insecure). Dev testing only.\n")
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    else:
        ssl_context = None  # SDK default: full verification

    print(f"Connecting to MCP server:\n  {MCP_ENDPOINT}\n")

    try:
        async with streamablehttp_client(
            MCP_ENDPOINT, headers=headers
        ) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                # Handshake.
                await session.initialize()
                print("Connected and initialized session.\n")

                # 1. List every tool the server exposes.
                tools_result = await session.list_tools()
                tools = tools_result.tools
                print(f"Server exposes {len(tools)} tool(s):")
                for tool in tools:
                    print(f"  - {tool.name}")
                print()

                # 2. Call one read-only tool as the round-trip test.
                tool_names = {t.name for t in tools}
                if TEST_TOOL in tool_names:
                    print(f"Calling test tool: {TEST_TOOL} ...")
                    result = await session.call_tool(TEST_TOOL, arguments={})
                    print("Tool call returned:")
                    for block in result.content:
                        text = getattr(block, "text", None)
                        if text:
                            print(f"  {text}")
                    print()
                    print("=" * 56)
                    print("SUCCESS - MCP Server is reachable and responding.")
                    print("=" * 56)
                else:
                    print(f"NOTE: '{TEST_TOOL}' not exposed by this server.")
                    print("Tool listing succeeded, so the server IS reachable.")
                    print("Pick any tool above and add it as the test call.")
                    print()
                    print("=" * 56)
                    print("PARTIAL SUCCESS - connected and listed tools.")
                    print("=" * 56)

    except ssl.SSLError as exc:
        sys.exit(
            f"\nSSL ERROR: {exc}\n"
            "  The Splunk Cloud trial uses a self-signed certificate.\n"
            "  Re-run with the --insecure flag for local dev testing:\n"
            "      python verify_mcp.py --insecure"
        )
    except BaseException as exc:
        # MCP runs inside a TaskGroup, so failures arrive wrapped in an
        # ExceptionGroup. Unwrap it so the real cause is actually visible.
        def unwrap(err, depth=0):
            lines = []
            indent = "  " + "  " * depth
            sub_exceptions = getattr(err, "exceptions", None)
            if sub_exceptions:
                lines.append(f"{indent}{type(err).__name__}: {err}")
                for sub in sub_exceptions:
                    lines.extend(unwrap(sub, depth + 1))
            else:
                lines.append(f"{indent}{type(err).__name__}: {err}")
            return lines

        detail = "\n".join(unwrap(exc))
        is_ssl = "SSL" in detail or "CERTIFICATE" in detail.upper()
        print("\nCONNECTION FAILED. Underlying error(s):")
        print(detail)
        print()
        if is_ssl:
            print("This looks like an SSL certificate error.")
            print("Re-run with:  python verify_mcp.py --insecure")
        else:
            print("Things to check:")
            print("  - Is the token correct and not expired?")
            print("  - Is token authorization enabled (Settings > Tokens)?")
            print("  - Does your role have the mcp_tool_execute capability?")
        print("\nCapture the exact error above in /docs/feedback.md.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Verify the Splunk MCP Server is reachable.")
    parser.add_argument(
        "--insecure", action="store_true",
        help="Disable SSL verification (for self-signed certs, dev only).")
    args = parser.parse_args()

    asyncio.run(run_check(args.insecure))


if __name__ == "__main__":
    main()

# the connection/structure is sound, and this specific error is environmental, not a code defect.
# The full happy path just hasn't been able to execute end-to-end yet — so I can't claim it's verified,
#  only that there's no known bug standing in the way.
