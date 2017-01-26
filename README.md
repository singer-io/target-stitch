# persist-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Install

Requires Python 3

```bash
› pip install persist-stitch
```

## Use

Create config file:

```json
{
  "client_id" : YOUR_STITCH_CLIENT_ID,
  "token" : YOUR_STITCH_ACCESS_TOKEN
}
```

```bash
› persist_stitch.py sync --config config.json
```

