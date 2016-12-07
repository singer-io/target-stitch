# persist-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Install

Requires Python 3

```bash
› pip install persist-stitch
```

## Use

```bash
› STITCH_TOKEN=abcd STITCH_CLIENT_ID=1234 persist-stitch command [arg...]
```

Anything on the command line after COMMAND is passed as arguments to
COMMAND. Example:

```bash
› STITCH_TOKEN=abcd STITCH_CLIENT_ID=1234 persist-stitch python stream_github.py
```