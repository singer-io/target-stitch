# persist-stitch

Persists stitchstream formatted data from stdin to the Stitch Import API

## Install

Requires Python 3

```bash
› pip install stitchclient
› python setup.py install
```

## Use

```bash
› cat encoded-data.out | persist-stitch -T <token> -C <client-id>
```
