# persist-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Install

Requires Python 3

```bash
› pip install persist-stitch
```

## Use

persist_stitch.py takes two types of input:

1. A config file containing your client id and access token
2. A stream of stitchstream-formatted data on stdin

Create config file to contain your client id and token:

```json
{
  "client_id" : YOUR_STITCH_CLIENT_ID,
  "token" : YOUR_STITCH_ACCESS_TOKEN
}
```

```bash
› some_streamer | persist_stitch.py sync --config config.json
```

where `some_streamer` is a program that writes stitchstream-formatted data to stdout.
