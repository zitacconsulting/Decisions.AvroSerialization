# What is this?
This is a project for adding steps in Decisions, with the purpose of "decoding" (Kafka) Avro messages.

## Usage
Open SLN and select Build -> Publish Selection. 
The files you want are:
- /bin/Release/net6.0/Avro.dll
- /bin/Release/net6.0/Confluent.SchemaRegistry.dll
- /bin/Release/net6.0/Confluent.SchemaRegistry.Serdes.Avro.dll
- /bin/Release/net6.0/Zitac.Decisions.AvroSerialization.dll

## Usage in Decisions
After implementation in Decisions you will have new steps under Integration > Message Queues > Zitac for converting Avro messages to Json format