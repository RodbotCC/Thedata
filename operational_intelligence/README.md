# Operational Intelligence

This layer turns the normalized CRM library into action-oriented datasets.

## What is here
- `follow_up_queue.md`: prioritized action list for active/won leads
- `upcoming_event_watchlist.md`: event-date watchlist
- `by_owner/`: owner-specific follow-up queues

## Normalized Files
- `../normalized/lead_event_facts.csv`
- `../normalized/follow_up_queue.csv`
- `../normalized/upcoming_event_watchlist.csv`
- `../normalized/suppressed_follow_up_leads.csv`

## Notes
- Event facts rely heavily on inferred custom-field aliases from the opportunity export.
- `date_won` is only used as a fallback event-date hint when the dedicated event-date custom field is absent.
