---
hide:
  - navigation
---

# BoostedTravel

**Agent-native flight search & booking. 400+ airlines, straight from the terminal.**

No browser. No scraping. No token-burning automation. Built for AI agents and developers who need travel in their workflow.

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Get Started in 5 Minutes**

    ---

    Install the CLI, grab your API key, and search your first flight.

    [:octicons-arrow-right-24: Getting Started](getting-started.md)

-   :material-api:{ .lg .middle } **API Guide**

    ---

    Error handling, search results, workflows, unlock mechanics, cost strategies.

    [:octicons-arrow-right-24: API Guide](api-guide.md)

-   :material-robot:{ .lg .middle } **AI Agent Guide**

    ---

    Architecture patterns, preference scoring, rate limits, price tracking.

    [:octicons-arrow-right-24: Agent Guide](agent-guide.md)

-   :material-console:{ .lg .middle } **CLI Reference**

    ---

    Every command, flag, and option for the `boostedtravel` CLI.

    [:octicons-arrow-right-24: CLI Reference](cli-reference.md)

</div>

---

## Why BoostedTravel?

Flight websites inflate prices with demand tracking, cookie-based pricing, and surge markup. The same flight is often **$20–$50 cheaper** through BoostedTravel — raw airline price, zero markup.

| | Google Flights / Booking.com / Expedia | **BoostedTravel** |
|---|---|---|
| Search | Free | **Free** |
| View details & price | Free (with tracking/inflation) | **Free** (no tracking) |
| Book | Ticket + hidden markup | **$1 unlock + ticket price** |
| Price goes up on repeat search? | Yes | **Never** |

## How It Works

```
Search (free) → Unlock ($1) → Book (free)
```

1. **Search** — returns offers with price, airlines, duration, stopovers, conditions. Completely free, unlimited.
2. **Unlock** — confirms live price with the airline, reserves for 30 minutes. $1 flat fee.
3. **Book** — creates real airline PNR. E-ticket sent to passenger email. Free after unlock.

## Quick Start

=== "Python CLI"

    ```bash
    pip install boostedtravel

    boostedtravel register --name my-agent --email you@example.com
    export BOOSTEDTRAVEL_API_KEY=trav_...

    boostedtravel search LHR JFK 2026-04-15
    boostedtravel unlock off_xxx
    boostedtravel book off_xxx \
      --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
      --email john.doe@example.com
    ```

=== "Python SDK"

    ```python
    from boostedtravel import BoostedTravel

    bt = BoostedTravel(api_key="trav_...")
    flights = bt.search("LHR", "JFK", "2026-04-15")
    print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")

    unlocked = bt.unlock(flights.offers[0].id)
    booking = bt.book(
        offer_id=unlocked.offer_id,
        passengers=[{"id": "pas_0", "given_name": "John", "family_name": "Doe",
                     "born_on": "1990-01-15", "gender": "m", "title": "mr"}],
        contact_email="john.doe@example.com",
    )
    print(f"Booked! PNR: {booking.booking_reference}")
    ```

=== "JavaScript SDK"

    ```typescript
    import { BoostedTravel } from 'boostedtravel';

    const bt = new BoostedTravel({ apiKey: 'trav_...' });
    const flights = await bt.search('LHR', 'JFK', '2026-04-15');
    console.log(`${flights.totalResults} offers`);
    ```

=== "MCP Server"

    ```json
    {
      "mcpServers": {
        "boostedtravel": {
          "command": "npx",
          "args": ["-y", "boostedtravel-mcp"],
          "env": {
            "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key"
          }
        }
      }
    }
    ```

## Packages

| Package | Install | Description |
|---------|---------|-------------|
| **Python SDK + CLI** | `pip install boostedtravel` | SDK + `boostedtravel` CLI |
| **JS/TS SDK + CLI** | `npm install -g boostedtravel` | SDK + `boostedtravel` CLI |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol for Claude, Cursor, Windsurf |

## Links

- [PyPI](https://pypi.org/project/boostedtravel/)
- [npm (JS SDK)](https://www.npmjs.com/package/boostedtravel)
- [npm (MCP Server)](https://www.npmjs.com/package/boostedtravel-mcp)
- [OpenAPI / Swagger](https://api.boostedchat.com/docs)
- [GitHub](https://github.com/Boosted-Chat/BoostedTravel)
