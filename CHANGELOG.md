# Changelog

## 1.7.3
  * Update to singer-python==5.0.15 to use the change to `RecordMessage.asdict` for serialization of `time_extracted`

## 1.7.2
  * Updates serialize to format `time_extracted` in a cross platform way, using `singer.utils.strftime`

## 1.7.1
  * Allows the push to the Stitch API to bypass SSL verification if an env variable is set [#45](https://github.com/singer-io/target-stitch/pull/45)
  * Updates error message to clarify when a message is too large for the Stitch API [#47](https://github.com/singer-io/target-stitch/pull/47)
