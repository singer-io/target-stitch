# persist-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Build

Requires Python 3

```bash
python setup.py install
```

## Use

```bash
› cat encoded-data.out | persist-stitch -T <token> -C <client-id>
```
