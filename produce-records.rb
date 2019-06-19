require 'json'

$TABLE_NAME='postgres_full_table_replication_test'
records = File.open('records.json', 'w')

schema = {"stream" =>  $TABLE_NAME, 
	  "bookmark_properties" =>  [], 
	  "key_properties" =>  ["id"], 
	  "schema" =>  {	"selected" =>  true, 
			"type" =>  "object", 
			"properties" =>  {
					  "our_real" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "number"]}, 
					  "our_smallint" =>  {"selected" =>  true, "minimum" =>  -32768, "maximum" =>  32767, "inclusion" =>  "available", "type" =>  ["null", "integer"]}, 
					  "OUR DATE" =>  {"selected" =>  true, "type" =>  ["null", "string"], "inclusion" =>  "available", "format" =>  "date-time"}, 
					  "id" =>  {"selected" =>  true, "minimum" =>  -2147483648, "maximum" =>  2147483647, "inclusion" =>  "automatic", "type" =>  ["integer"]}, 
					  "our_bigint" =>  {"selected" =>  true, "minimum" =>  -9223372036854775808, "maximum" =>  9223372036854775807, "inclusion" =>  "available", "type" =>  ["null", "integer"]}, 
					  "our_integer" =>  {"selected" =>  true, "minimum" =>  -2147483648, "maximum" =>  2147483647, "inclusion" =>  "available", "type" =>  ["null", "integer"]}, 
					  "our_boolean" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "boolean"]}, 
					  "our_double" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "number"]}, 
					  "our_json" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "string"]}, 
					  "our_store" =>  {"selected" =>  true, "type" =>  ["null", "object"], "inclusion" =>  "available", "properties" =>  {}},
				 	  "our_decimal" =>  {"exclusiveMinimum" =>  true, "minimum" =>  -10000000000, "exclusiveMaximum" =>  true, "inclusion" =>  "available", "selected" =>  true, 
					  		"multipleOf" =>  0.01, "maximum" =>  10000000000, "type" =>  ["null", "number"]}, 
					  "our_text" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "string"]},


					  "our_real2" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "number"]}, 
					  "our_smallint2" =>  {"selected" =>  true, "minimum" =>  -32768, "maximum" =>  32767, "inclusion" =>  "available", "type" =>  ["null", "integer"]}, 
					  "OUR DATE2" =>  {"selected" =>  true, "type" =>  ["null", "string"], "inclusion" =>  "available", "format" =>  "date-time"}, 
					  "our_bigint2" =>  {"selected" =>  true, "minimum" =>  -9223372036854775808, "maximum" =>  9223372036854775807, "inclusion" =>  "available", "type" =>  ["null", "integer"]}, 
					  "our_integer2" =>  {"selected" =>  true, "minimum" =>  -2147483648, "maximum" =>  2147483647, "inclusion" =>  "available", "type" =>  ["null", "integer"]}, 
					  "our_boolean2" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "boolean"]}, 
					  "our_double2" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "number"]}, 
					  "our_json2" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "string"]}, 
					  "our_store2" =>  {"selected" =>  true, "type" =>  ["null", "object"], "inclusion" =>  "available", "properties" =>  {}},
				 	  "our_decimal2" =>  {"exclusiveMinimum" =>  true, "minimum" =>  -10000000000, "exclusiveMaximum" =>  true, "inclusion" =>  "available", "selected" =>  true, 
					  		"multipleOf" =>  0.01, "maximum" =>  10000000000, "type" =>  ["null", "number"]}, 
					  "our_text2" =>  {"selected" =>  true, "inclusion" =>  "available", "type" =>  ["null", "string"]}
					  }}, 
	"type" =>  "SCHEMA"}

records.puts( schema.to_json() )

50000.times do |i|
	records.puts( {	"stream" =>  $TABLE_NAME, 
			"record" =>  {	
					"our_real" =>  1.2, 
					"our_smallint" =>  100,
					"OUR DATE" =>  "1998-03-04T00:00:00+00:00",
					"id" =>  i,
					"our_bigint" =>  1000000, 
					"our_integer" =>  44100, 
					"our_boolean" =>  true, 
					"our_double" =>  1.1, 
					"our_json" =>  "{\"secret\" =>  55}",
					"our_store" => {"name" =>  "betty", "dances" => "floor"},
					"our_decimal" =>  0.01,
					"our_text" =>  "some text",

					"our_real2" =>  1.2, 
					"our_smallint2" =>  100,
					"OUR DATE2" =>  "1998-03-04T00:00:00+00:00",
					"our_bigint2" =>  1000000, 
					"our_integer2" =>  44100, 
					"our_boolean2" =>  true, 
					"our_double2" =>  1.1, 
					"our_json2" =>  "{\"secret\" =>  55}",
					"our_store2" => {"name" =>  "betty", "dances" => "floor"},
					"our_decimal2" =>  0.01,
					"our_text2" =>  " I've seen things you people wouldn't believe. Attack ships on fire off the shoulder of Orion. I watched c-beams glitter in the dark near the Tannhauser Gate. All those moments will be lost in time, like tears in rain. Time to die." }, 
			"time_extracted" =>  "2019-06-18T17:10:05.878611Z", 
			"version" =>  1560877805878, "type" =>  "RECORD"}.to_json() )
end



