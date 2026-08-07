# Chat Starter — Microsoft Teams Integration Design

Paste this as your opening message in a new chat inside the "Backup Cohesity Architect" project.

---

## Paste this:

```
Read `claude/cohesity-architect-plan.md` and `claude/cohesity-architect-plan-v2.md` from this project for full context before answering.

I am designing a Microsoft Teams integration for the Cohesity intelligent automation platform. I want to work through this design in detail in this chat and save the output as a project doc at the end.

## What we need Teams to do

1. Accept natural language queries from any team member:
   - Ops: "@Claude backup failure summary today"
   - DBA: "@Claude is prod-oracle-01 being backed up"
   - App team: "@Claude check backup status for these 10 servers: [list]"

2. Claude responds in the Teams thread with structured findings (not raw data)

3. Claude also creates a Jira audit entry for every query answered

4. For alert notifications: Dynatrace creates a Jira ticket → Claude posts findings to Jira → Claude sends a Teams notification to the ops channel

## Constraints and guardrails already agreed

- Claude is read-only. Never writes to Cohesity.
- Claude never closes Jira tickets. Posts findings only.
- SNOW is out of scope completely.
- API key never in messages, prompts, or channel history.
- Every finding must cite its source (cluster, API endpoint, timestamp). No inference.

## What I need designed

1. Which Microsoft Teams connector approach fits best for our environment:
   - Teams MCP (if available via Claude Code)
   - Power Automate webhook → claude -p → Teams response
   - Azure Bot Framework bot powered by Claude API
   - Something else

2. How does the Teams message get routed to Claude and back:
   - Message format and parsing
   - How Claude knows who asked and what they asked
   - How the response gets posted back to the right thread

3. How credentials flow safely:
   - Cohesity API key never appears in Teams messages
   - Team members do not need to know the API key exists

4. How to handle multi-turn conversations in Teams:
   - "Show me cluster 7 failures" → "Now filter to just Oracle" → Claude maintains context

5. What the Teams channel setup looks like:
   - One channel for ops queries
   - One channel for DBA/App queries
   - Or one channel with routing by @mention

Start by asking me any questions you need to nail down the approach, then design it step by step. At the end, produce a Teams Integration Design doc I can save to this project.
```
