# target-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Install

Requires Python 3

```bash
› pip install target-stitch
```

## Use

target-stitch takes two types of input:

1. A config file containing your client id and access token
2. A stream of stitchstream-formatted data on stdin

Create config file to contain your client id and token:

```json
{
  "client_id" : 1234,
  "token" : "asdkjqbawsdciobasdpkjnqweobdclakjsdbcakbdsac"
}
```

```bash
› tap-some-api | target-stitch --config config.json
```

where `tap-some-api` is a program that writes stitchstream-formatted data to stdout.

---

Copyright &copy; 2017 Stitch
