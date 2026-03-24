# Network egress policy

## Normative requirements

1. All outbound HTTPS traffic MUST traverse the corporate egress proxy.
2. The runtime MUST NOT initiate direct internet connections to public APIs without approval.
3. Batch workloads SHOULD disable public internet egress unless an external API is explicitly allowlisted.
4. Service mesh east-west traffic MUST NOT use unencrypted HTTP on the network.
