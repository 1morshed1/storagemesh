# 08 — Running StorageMesh

## Option A: unit + integration tests (no infra)

The whole system — chunking, ring, replication, quorum, dedup, failover,
multipart, presign, workers — is exercised in-process with no external services:

```bash
go test ./...
```

`internal/gateway/gateway_test.go` and `internal/worker/worker_test.go` spin up
real storage nodes in-process against a `MemStore`. Start here to understand the
system; the tests are runnable documentation.

## Option B: full local cluster with docker-compose

Brings up Postgres, 3 storage nodes, the gateway, and the worker:

```bash
docker compose -f deploy/docker-compose.yml up --build
# add the event stream too:
docker compose -f deploy/docker-compose.yml --profile kafka up --build
```

Then run the end-to-end smoke test (create bucket, upload, download+verify,
dedup, pre-signed URL, list, delete):

```bash
./deploy/smoke-test.sh
```

### Trying it by hand with curl

```bash
GW=http://localhost:8080
KEY='-H X-Access-Key:dev-access-key'

curl -sS $KEY -X PUT  $GW/photos                              # create bucket
curl -sS $KEY -X PUT  --data-binary @big.jpg $GW/photos/big.jpg   # upload
curl -sS $KEY        $GW/photos/big.jpg -o out.jpg            # download
curl -sS $KEY        "$GW/photos?prefix="                     # list
# mint a pre-signed URL, then use it WITHOUT credentials:
curl -sS $KEY "$GW/photos/big.jpg?presign=GET&expiry=300"
curl -sS -X DELETE $KEY $GW/photos/big.jpg                    # delete
```

### Watching it work

- **Dedup:** upload the same file under two keys, then
  `docker compose exec postgres psql -U mesh -d storagemesh -c 'select hash, ref_count, ref_count from chunks;'`
  — shared chunks show `ref_count = 2`.
- **Failover:** `docker compose stop storage-1`, then GET an object — it still
  serves from a surviving replica (gateway logs show the node dropped from the ring).
- **Repair/rebalance:** after restarting `storage-1`, the worker's rebalancer
  refills the replicas it missed within `REBALANCE_INTERVAL`.

## Option C: Kubernetes

Manifests live in `deploy/k8s/` (applied in filename order):

```bash
# build the image and load it into your cluster (kind/minikube shown):
docker build -t storagemesh:latest -f deploy/Dockerfile .
kind load docker-image storagemesh:latest        # or: minikube image load

kubectl apply -f deploy/k8s/
kubectl -n storagemesh get pods
```

What each manifest sets up and why:

| File | Resource | Why |
|------|----------|-----|
| `00-namespace-and-postgres.yaml` | Namespace, Secret, Postgres | metadata store (demo-grade) |
| `10-storage-nodes.yaml` | **StatefulSet** + headless Service | stable identity + per-pod volume for stateful chunk data |
| `20-gateway.yaml` | Deployment + Service + **HPA** | stateless, horizontally autoscaled |
| `30-worker.yaml` | Deployment (1 replica, Recreate) | cluster-wide sweeps; never run two at once |

The key Kubernetes lesson is the **StatefulSet vs Deployment** split: storage
nodes are stateful (unique durable disks, stable DNS) → StatefulSet; the gateway
is stateless → Deployment + HPA. Mapping that distinction correctly is exactly
what the architecture is teaching.
