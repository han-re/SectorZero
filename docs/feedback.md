# Feedback Log — Pit Wall Race Engineer

Running log of non-obvious findings, gotchas, and environment quirks discovered
during the build. Newest entries on top.

---

## 2026-05-22 — MCP Server unreachable: port 8089 blocked (NOT a token/SSL issue)

**Symptom.** `python verify_mcp.py` fails with:

```
CONNECTION FAILED. Underlying error(s):
  ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)
    ConnectTimeout:
```

The script then suggests checking the token, token authorization, and the
`mcp_tool_execute` capability. **All of those hints are misleading** for this
error. A `ConnectTimeout` means the TCP handshake never completed — we never
reached TLS or auth, so the token is irrelevant here.

**Root cause.** The MCP endpoint is on splunkd's management port:

```
https://prd-p-s1zak.splunkcloud.com:8089/services/mcp
```

On Splunk Cloud the management port (8089) is **not exposed to the public
internet by default** — it sits behind an IP allow-list. Until your current
public IP is added to that allow-list, every connection to 8089 hangs and times
out, regardless of token validity.

Network probe that pinned it down:

| Check                          | Result                          |
|--------------------------------|---------------------------------|
| DNS `prd-p-s1zak.splunkcloud.com` | resolves → 98.86.128.96      |
| TCP connect port 443           | succeeds                        |
| TCP connect port 8089          | **times out** (firewalled)      |

**Fix.** Add your public IP to the Splunk Cloud management-port (8089) IP
allow-list:
- Splunk Cloud UI: Settings → Server Settings → IP Allow List → "Splunk Platform
  REST API access (port 8089)", or
- Admin Config Service (ACS) API: add the IP to the management/REST allow-list.

Find the IP to allow-list with: `curl -s https://api.ipify.org`

`--insecure` does NOT help — it only affects TLS verification, which happens
*after* the TCP connection that is currently failing.

## 2026-05-22 — Port 443 is the Web UI tier, not the MCP/REST endpoint

Investigated whether the MCP server could be reached on 443 (which is open)
instead of the blocked 8089. It cannot.

`GET https://prd-p-s1zak.splunkcloud.com/services/mcp` returns `303 See Other`
with `Server: nginx` and `Location: .../en-US/services/mcp`. That is the Splunk
**Web** front end rewriting the path into a localized web (login) route — it does
not speak MCP JSON-RPC. A POST `initialize` JSON-RPC call gets the same 303 →
HTML redirect, not a JSON-RPC response.

Conclusion: the MCP server is served only by splunkd on 8089. There is no 443
alternative; the 8089 IP allow-list is the only path to connectivity.

## 2026-05-22 — Splunk Cloud trial uses a self-signed cert on 8089

(Carried over from verify_mcp.py docstring, for the record.) Splunk Cloud trial
instances present a self-signed certificate on port 8089, so once 8089 is
reachable the first connection may fail SSL verification. Use the `--insecure`
flag for local dev testing only — never anything resembling production.
