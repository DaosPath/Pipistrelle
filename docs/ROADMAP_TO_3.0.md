# Pipistrelle — Roadmap hacia 3.0.0.0

Este documento fija el plan de evolución de Pipistrelle desde la release estable actual `2.1.2.1` hasta `3.0.0.0`.

La intención es evitar saltos de versión artificiales: cada minor de la serie `2.x` debe representar una etapa de producto claramente terminada. `3.0.0.0` queda reservado para el cambio arquitectónico mayor: Pipistrelle como plataforma MQTT distribuida, con clustering y alta disponibilidad estables.

## Reglas del roadmap

- La versión pública de Pipistrelle usa **cuatro componentes**: `MAJOR.MINOR.PATCH.BUILD`.
- Cargo mantiene SemVer normal de tres componentes internamente.
- No se avanza de etapa solo porque una feature “funcione”; cada release debe pasar sus gates de protocolo, persistencia, seguridad, integración y rendimiento.
- No se sacrifica correctness MQTT, backpressure, seguridad o durabilidad para recuperar un benchmark.
- Los fast-paths de QoS0 deben mantenerse aislados de trabajo que solo pertenece a rutas protocol-heavy.
- `3.0.0.0` no se publica mientras clustering/HA siga siendo experimental.

---

## Baseline actual — 2.1.2.1

Estado actual publicado:

- MQTT v5 QoS 0/1/2.
- Retained messages.
- Last Will + Will Delay + crash persistence.
- Persistent Sessions + Session Expiry.
- ClientID takeover con `DISCONNECT 0x8E`.
- Principal-bound Session hardening.
- PUBLISH/Will application properties.
- Message Expiry.
- UNSUBSCRIBE / UNSUBACK.
- Receive Maximum bilateral.
- Maximum Packet Size bilateral.
- Server-assigned ClientID.
- CONNECT fragmentado sobre TCP.
- Validación MQTT UTF-8 / varint más estricta.
- TLS 1.3 + perfil híbrido PQC `X25519MLKEM768`.
- Prometheus, `/health`, `/info`.
- Colas bounded, backpressure, slow-consumer policy.
- Bounded bridge queue.
- Topic Alias Client→Server con máximo configurable y reset por Network Connection.
- Fast-path ARM64/NEON de ingest QoS0 aislado del routing general.

Gates de referencia de la Orange Pi:

- QoS0 ingest full-topic: ~58 M msg/s nativo en el hot path validado. MQTT 5 Topic Alias: mediana **161.452 M msg/s** en host normal (3×~1B) y techo optimizado separado de **211.520 M msg/s** de mediana (3×~1B, todas >200M).
- QoS0 full-topic end-to-end: **33.203 M msg/s** en el gate fresco de ~50M con writer batch 1024.
- PQC híbrido operativo.
- RAM del broker alrededor de ~100–150 MiB en los gates QoS0 actuales.

---

# 2.1.3.0 — Cierre MQTT 5 / compliance hardening

Objetivo: reducir la deuda restante del protocolo antes de entrar en features de producto.

## Trabajo principal

- Enhanced AUTH state machine.
- Authentication Method / Authentication Data.
- AUTH packet y reason codes asociados.
- Request Problem Information.
- Request Response Information.
- Response Information cuando sea aplicable.
- Server Reference / redirect semantics que decidamos soportar.
- Revisión de todos los CONNECT/CONNACK singleton properties.
- Revisión de reason codes por packet family.
- Fuzzing del codec MQTT.
- Corpus de malformed packets.
- Shared subscription edge cases.
- UTF-8 MQTT exhaustive corpus.
- Packet Identifier lifecycle tests exhaustivos.
- Disconnect/error matrix por violación de protocolo.

## Gates

- Suite raw MQTT ampliada.
- Fuzzing sin crashes/panics.
- Malformed corpus reproducible.
- No regresión de los gates 20M ingest / 2M end-to-end fuera de variación normal.
- No declarar “MQTT v5 100% compliant” hasta validar contra una suite externa/interoperabilidad más amplia.

---

# 2.2.0.0 — Management REST API

Objetivo: convertir Pipistrelle de proceso broker a servicio administrable.

## API prevista

- Estado del broker.
- Lista de clientes.
- Cliente por ClientID.
- Conexiones activas.
- Sessions online/offline.
- Subscriptions.
- Retained messages.
- Pending Wills.
- QoS inflight.
- Slow consumers.
- Bridge state.
- Métricas resumidas.
- Disconnect remoto de clientes.
- Eliminar Session.
- Eliminar retained message.
- Inspección de configuración efectiva.

## Seguridad

- API separada del listener MQTT.
- Bind seguro por defecto.
- Autenticación de administración.
- RBAC básico para operaciones destructivas.
- Audit log para cambios administrativos.

## Gates

- API versionada (`/api/v1`).
- Ninguna operación destructiva sin auth.
- Tests de concurrencia con clientes conectándose/desconectándose mientras se consulta la API.
- La API no debe tomar locks globales largos que degraden routing.

---

# 2.3.0.0 — Pipistrelle Control Center

Objetivo: interfaz web operativa sobre la Management API.

## Vistas principales

- Overview.
- Throughput actual.
- CPU / RAM.
- Connections.
- Sessions.
- Subscriptions.
- Retained.
- QoS inflight.
- Wills.
- TLS / PQC handshakes.
- Slow consumers.
- Bridge status.

## Cliente individual

- ClientID.
- Usuario/principal.
- IP.
- TLS / cipher / KX.
- Session Expiry.
- Subscriptions.
- Queued messages.
- QoS inflight.
- Last Will.
- Disconnect.
- Delete Session.

## Gates

- UI usa exclusivamente API pública/estable.
- Ninguna lógica crítica vive solo en frontend.
- Funciona bien desde escritorio y móvil.
- No expone secretos de configuración.

---

# 2.4.0.0 — Persistence Engine v2

Objetivo: dejar de tratar SQLite por operación como solución definitiva para rutas QoS pesadas.

Esta etapa es especialmente importante para recuperar QoS1/QoS2 sin relajar compliance.

## Arquitectura objetivo

```text
MQTT state changes
        ↓
Persistence API
        ↓
WAL / journal
        ↓
batch writer
        ↓
storage backend
```

## Trabajo principal

- Capa de abstracción de persistence.
- WAL propio o journal append-oriented.
- Batching de commits.
- Group commit.
- Menos fsync/transaction por mensaje.
- Cola persistente ordenada.
- Recovery determinista.
- Compaction / cleanup.
- Métricas de storage latency.
- Métricas de queue depth.
- Backpressure del storage.
- Mantener compatibilidad/migración desde SQLite existente.

## Gates

- Crash recovery con SIGKILL.
- Cero pérdida de estado ya ACKeado como durable.
- QoS1/QoS2 mejoran claramente frente a `2.1.2.0`.
- Recovery de millones de registros medible y documentado.
- No introducir corrupción después de power-loss-style restart tests.

---

# 2.5.0.0 — Seguridad avanzada / Enterprise Auth

Objetivo: pasar de auth/ACL local sólido a una plataforma integrable con identidad empresarial.

## Backends previstos

- File/local credentials.
- SQL auth backend.
- JWT.
- OAuth2/OIDC validation.
- mTLS certificate identity.
- LDAP/Directory integration si aporta valor real.
- Backend custom mediante extensión/WASM cuando exista.

## Authorization

- Roles.
- Policies por topic.
- Variables como `${clientId}` / principal.
- QoS permitidos.
- retain permitido/no permitido.
- shared subscription permitido/no permitido.
- límites por principal.

## Protección

- Connection-rate limits.
- Per-client publish-rate limits.
- Subscription-rate limits.
- Auth brute-force protection.
- Certificate revocation strategy.
- Audit log.

## Gates

- Fail closed.
- Rotación de credenciales sin reinicio cuando sea posible.
- Tests de privilege escalation.
- ACL/session ownership no puede cruzar principals.

---

# 2.6.0.0 — Backup / Restore / Disaster Recovery

Objetivo: que un operador pueda proteger y reconstruir un broker en producción.

## CLI objetivo

```text
pipistrelle backup create
pipistrelle backup inspect
pipistrelle backup verify
pipistrelle backup restore
```

## Estado incluido

- Sessions.
- Subscriptions.
- Retained.
- Wills.
- QoS inflight.
- Offline queues.
- Metadata necesaria del persistence engine.

## Trabajo adicional

- Backup consistente mientras el broker sigue operativo.
- Checksums.
- Formato versionado.
- Restore dry-run.
- Backup encryption opcional.
- Retention policies.

## Gates

- Restore byte/semantic equivalent del estado.
- Restore sobre una máquina nueva.
- Prueba de desastre: borrar data dir → restore → clientes reanudan Session.

---

# 2.7.0.0 — Observabilidad y diagnóstico avanzado

Objetivo: que Pipistrelle sea fácil de operar bajo carga y fácil de depurar.

## Trabajo principal

- OpenTelemetry.
- Structured tracing.
- Per-client trace temporal.
- Per-topic statistics opcionales/sampled.
- Storage latency histograms.
- Routing latency.
- Auth latency.
- Queue depth.
- Slow-consumer explorer.
- Session churn.
- QoS handshake latency.
- Bridge latency/reconnect state.
- Diagnostic bundle.

## Ejemplo de tooling

```text
pipistrelle trace client sensor-921
pipistrelle diagnose create
```

## Gates

- Tracing disabled = impacto mínimo.
- Sampling configurable.
- Diagnostic bundle sin secretos.
- Métricas documentadas y estables.

---

# 2.8.0.0 — Extension System / WASM

Objetivo: crear un ecosistema de extensiones sin permitir que plugins arbitrarios comprometan el broker.

## Dirección preferida

**WASM sandboxed extensions**.

## Hooks iniciales

```text
on_connect
on_authenticate
on_authorize
on_publish
on_subscribe
on_unsubscribe
on_disconnect
on_will
```

## Requisitos

- Memory limit por plugin.
- CPU/fuel limit.
- Timeout.
- No acceso al filesystem/red por defecto.
- Capability-based permissions.
- Hot reload cuando sea seguro.
- Versioned SDK.
- Plugin crash no tumba Pipistrelle.

## Gates

- Plugin infinito no bloquea routing.
- Plugin panic/trap no tumba broker.
- Hooks pueden desactivarse sin overhead apreciable.
- ABI/SDK versionado.

---

# 2.9.0.0 — Clustering experimental / Beta

Objetivo: primer Pipistrelle distribuido. **Todavía no HA estable ni 3.0.**

## Fase 1 — Membership

- Node identity.
- Discovery.
- Membership.
- Heartbeats.
- Node join/leave.
- Failure detection.

## Fase 2 — Distributed routing

```text
Publisher → Node A
Subscriber → Node C
            ↓
        message arrives
```

- Topic routing entre nodos.
- Subscription propagation.
- Shared subscription ownership inicial.

## Fase 3 — Session ownership

- ClientID owner node.
- Takeover entre nodos.
- Session location lookup.
- Reconnect a nodo diferente.

## Fase 4 — Replicated state

- Retained.
- Sessions.
- Subscriptions.
- Offline queues.
- QoS1/2 state.
- Wills.
- Session Expiry.

## Estado esperado de 2.9

- Experimental/beta.
- Posibles restricciones documentadas.
- No prometer zero-downtime todavía.
- No llamar al cluster “production HA” hasta superar los gates de 3.0.

---

# 3.0.0.0 — Clustering estable + High Availability

`3.0.0.0` solo existe cuando el cluster deja de ser experimental.

Este es el cambio de significado del producto:

```text
Pipistrelle 2.x
standalone MQTT broker

        ↓

Pipistrelle 3.x
 distributed MQTT platform
```

## Arquitectura objetivo

```text
                 Load Balancer
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
  Pipistrelle A  Pipistrelle B  Pipistrelle C
        │             │             │
        └──────── cluster bus ───────┘
                      │
              replicated state
```

## Requisitos para poder llamarlo 3.0

### Alta disponibilidad

- Muerte de un nodo no detiene el servicio completo.
- Clientes pueden reconectar a otro nodo.
- Session state recuperable desde otro nodo.
- Retained sigue disponible.
- Wills se manejan correctamente en node failure.
- QoS1/QoS2 no se corrompen con failover.

### Routing distribuido

- Publisher y subscriber pueden estar en nodos distintos.
- Wildcards funcionan cross-node.
- Shared subscriptions tienen ownership/fairness bien definidos.
- No hay loops de routing.

### Estado

- Replicación con consistencia explícitamente documentada.
- No split-brain silencioso.
- Recovery tras partition.
- Node rejoin.
- Rebalance.

### Operación

- Rolling restart.
- Rolling upgrade.
- Node drain.
- Cluster health API.
- Replication lag metrics.
- Backup/restore cluster-aware.

### Plataforma

- Management API cluster-aware.
- Control Center cluster-aware.
- Security/policies consistentes en todos los nodos.
- Extensions con modelo claro en cluster.

## Gate definitivo de 3.0

Prueba mínima obligatoria:

```text
Subscriber → Node C
Publisher  → Node A

mensaje A→C funciona

SIGKILL Node A

cluster sigue operativo
publisher reconecta a Node B
subscriber conserva Session
retained sigue disponible
QoS state continúa correctamente

Node A vuelve
rejoin + sync
sin split-brain
```

Además:

- Stress multi-node prolongado.
- Network partition tests.
- Packet loss/reordering en cluster bus.
- Node crash loops.
- Recovery de persistence.
- Upgrade desde 2.9.x.
- Benchmark distribuido documentado.

Solo después de estos gates se publica `3.0.0.0`.

---

# Lo que NO debemos hacer antes de tiempo

- No llamar `3.0` a un cluster que apenas conecta dos nodos.
- No meter UI antes de tener Management API estable.
- No meter Data Policies complejas antes de persistence/observability/cluster foundations.
- No sacrificar MQTT correctness por benchmarks.
- No sustituir bounded queues por unbounded queues para subir throughput.
- No ocultar regresiones QoS1/QoS2 detrás de números QoS0.
- No afirmar “MQTT v5 100% conforme” sin validación externa suficiente.

---

# Ideas posteriores o paralelas, no bloqueantes para 3.0

Estas pueden entrar cuando su base correspondiente esté madura, pero no deben distraer de la secuencia principal:

- JSON Schema validation.
- Protobuf schema validation.
- Data Policies / policy engine.
- Dead-letter routing.
- Rule engine.
- MQTT ↔ Kafka/NATS connectors.
- Cloud bridge packs.
- Helm chart.
- Kubernetes Operator.
- Multi-arch release automation.
- Optional post-quantum signatures cuando exista interoperabilidad práctica suficiente.

---

# Resumen de versiones

| Versión | Objetivo principal |
|---|---|
| `2.1.2.0` | MQTT flow-control/compliance sólido |
| `2.1.2.1` | Baseline actual: Topic Alias inbound + >200M/s optimized ingest ceiling |
| `2.1.3.0` | Cierre restante de MQTT 5 + fuzzing/compliance |
| `2.2.0.0` | Management REST API |
| `2.3.0.0` | Control Center / Web UI |
| `2.4.0.0` | Persistence Engine v2 / WAL / batching |
| `2.5.0.0` | Seguridad avanzada / enterprise auth / quotas |
| `2.6.0.0` | Backup, restore y disaster recovery |
| `2.7.0.0` | Observabilidad, tracing y diagnóstico |
| `2.8.0.0` | Extensiones WASM sandboxed |
| `2.9.0.0` | Clustering experimental / beta |
| `3.0.0.0` | Clustering estable + High Availability |

---

Este roadmap es deliberadamente secuencial. Las versiones pueden recibir builds/patches intermedios (`2.4.0.1`, `2.4.0.2`, etc.) sin consumir un nuevo minor mientras una etapa todavía se está endureciendo.
