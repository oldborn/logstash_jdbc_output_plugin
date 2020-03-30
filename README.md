# How to Install

Download logstash codebase from https://github.com/elastic/logstash, 
and compile using `gradle assemble`.
Configure `gradle.properties` file point to your newly built logstash-core directory. 

Execute `gradle gem` command to build the plugin.
Inside your logstash (pre-built) execute: 

`bin/logstash-plugin install --no-verify --local /path/to/logstash-output-logstash_jdbc_output_plugin-1.0.0.gem`  
