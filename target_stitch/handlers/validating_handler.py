class ValidatingHandler: # pylint: disable=too-few-public-methods
    '''Validates input messages against their schema.'''

    def handle_batch(self, messages, buffer_size_bytes, schema, key_names, bookmark_names=None): # pylint: disable=no-self-use,unused-argument
        '''Handles messages by validating them against schema.'''
        schema = ensure_multipleof_is_decimal(schema)
        validator = Draft4Validator(schema, format_checker=FormatChecker())
        for i, message in enumerate(messages):
            if isinstance(message, singer.RecordMessage):
                data = marshall_decimals(schema, message.record)
                try:
                    validator.validate(data)
                    if key_names:
                        for k in key_names:
                            if k not in data:
                                raise Exception('Message {} is missing key property {}'.format(i, k))

                except Exception as e:
                    raise TargetStitchException(
                        'Record does not pass schema validation: {}'.format(e))

        LOGGER.info('%s (%s): Batch is valid', message.stream, len(messages))
