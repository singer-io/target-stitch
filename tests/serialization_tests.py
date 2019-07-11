import unittest
import singer
import simplejson

class SerializationTests(unittest.TestCase):

    def create_raw_record(self, value):
        return '{"value": ' + value + '}'

    def create_raw_record_message(self,raw_record):
        return '{"type": "RECORD", "stream": "test", "record": ' + raw_record + '}'
        
        
    def test_deserialize_and_serialize(self):
        decimal_strs = [
            '-9999999999999999.9999999999999999999999',
            '0',
            '9999999999999999.9999999999999999999999',
            '-7187498962233394.3739812942138415666763',
            '9273972760690975.2044306442955715221042',
            '29515565286974.1188802122612813004366',
            '9176089101347578.2596296292040288441238',
            '-8416853039392703.306423225471199148379',
            '1285266411314091.3002668125515694162268',
            '6051872750342125.3812886238958681227336',
            '-1132031605459408.5571559429308939781468',
            '-6387836755056303.0038029604189860431045',
            '4526059300505414'
        ]
        for decimal_str in decimal_strs:
            record_str = self.create_raw_record(decimal_str)
            record_message_str = self.create_raw_record_message(record_str)
            deserialized_record = singer.parse_message(record_message_str).record
            serialized_record_str = simplejson.dumps(deserialized_record)
            self.assertEqual(record_str, serialized_record_str)




if __name__ == '__main__':
    unittest.main()
