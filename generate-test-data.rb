require 'bigdecimal'
require 'json'
OUR_SCHEMA={"type"=>"SCHEMA",
            "stream"=>"fat",
            "key_properties"=>["id"],
            "schema"=>{"type"=>"object",
                      "properties"=>{"id"=>{"inclusion"=>"automatic","minimum"=>-2147483648,"maximum"=>2147483647,"type"=>["null","integer"]},
                                    "name"=>{"type"=>["null","string"]},
                                    "height"=>{"type"=>["number"]},
                                    "criminal"=>{"type"=>["null","boolean"]},
                                    "money"=>{"type"=>["null","number"],"multipleOf"=>0.01},
                                    "birthday"=>{"type"=>["null","string"],"format"=>"date-time"},
                                    "int_array"=>{"type"=>["null","array"],"items"=>{"type"=>"integer"}}}}}


File.open('fat.singer.out', 'w') do |f|
  f.write(OUR_SCHEMA.to_json())
  f.write("\n")

  for i in 1..100000 do
    new_rec = {"type"=>"RECORD",
               "stream"=>"fat",
               "record"=>{"id"=> i,
                         "name"=>(0...800).map { (65 + rand(26)).chr }.join,
                         "height"=>rand(300),
                         "criminal"=>true,
                         "money"=>BigDecimal("#{rand(100)}.#{rand(100)}").to_f(),
                         "birthday"=>"2018-11-14T14:00:17.000000Z",
                         "int_array"=> (0...10).map { rand(26)}}}
    f.write(new_rec.to_json())
    f.write("\n")
  end

end
