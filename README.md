# persist-stitch

Persists Stitch Stream Encoded (SSE) data from stdin to the Stitch Import API

## Build

```bash
lein uberjar
```

## Use

```bash
STITCH_TOKEN=secrettoken STITCH_CLIENT_ID=1 STITCH_NAMESPACE=my_namespace cat sse-encoded-data.out | java -cp target/persist-stitch-standalone.jar com.stitchdata.persist.stitch.core
```
