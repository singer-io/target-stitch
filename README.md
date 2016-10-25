# persist-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Build

```bash
lein uberjar
```

## Use

```bash
› export STITCH_TOKEN=secrettoken
› export STITCH_CLIENT_ID=1
› export STITCH_NAMESPACE=my_namespace
› cat encoded-data.out | java -cp target/persist-stitch-standalone.jar com.stitchdata.persist.stitch.core
```
