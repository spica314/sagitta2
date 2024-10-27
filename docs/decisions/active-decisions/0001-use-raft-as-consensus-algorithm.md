- Start Date: 2024-10-27

## Summary
[summary]: #summary

In this project, use Raft as the consensus algorithm.

## Rationale and alternatives
[rationale-and-alternatives]: #rationale-and-alternatives

### Alternatives Considered

1. Paxos
    - Pros: Proven and widely adopted; has been in use for a long time, providing a reliable foundation.
    - Cons: Complex to implement; lacks clarity compared to Raft, making maintenance difficult.

### Reasons for Choosing Raft

- Simplicity: Raft is designed to be easier to understand and implement than Paxos, making it more maintainable.
- Leader Election: Provides a clear mechanism for leader election, which simplifies the implementation of consensus.
