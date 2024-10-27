- Start Date: 2024-10-27

## Summary
[summary]: #summary

In this project, use gRPC as the RPC protocol.

## Rationale and alternatives
[rationale-and-alternatives]: #rationale-and-alternatives

### Alternatives Considered

1. RESTful
    - Pros: Simple and easy to use; widely supported by many programming languages and frameworks.
    - Cons:
        - Not as efficient as gRPC; requires more manual work to handle requests and responses.
        - Tends to focus on CRUD operations, which can make it challenging to use appropriate verbs for more complex or functional actions.

2. Custom WEB API for Each RPC (JSON over HTTP)
    - Pros: Easy to implement; can be used with any programming language.
    - Cons: Not as efficient as gRPC; requires more manual work to handle requests and responses.

### Reasons for Choosing gRPC

- Performance: gRPC is faster and more efficient than REST due to its use of HTTP/2 and Protocol Buffers.
- Interoperability: gRPC supports multiple programming languages, making it easier to build polyglot systems.

