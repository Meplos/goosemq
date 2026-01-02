# GooseMQ 

A minimal message brocker written in Go

## Design goals
- Simplicity over completeness
- At-least-once delivery
- Push-based consumption
- Clear failure modes

## Non-goals
- Clustering
- Exactly-once semantics
- High availability

## Message lifecycle
PUBLISH → QUEUED → DELIVERED → ACKED
