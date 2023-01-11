# What is this?
This is a project for adding steps in Decisions, with the purpose of "decoding" (Kafka) Avro messages.

## Usage
Open SLN and build/publish/whatever.

## Usage in Decisions
After implementation in Decisions you will have new steps under Integration > Message Queues > Zitac for converting Avro messages to Json format

## Notes
- When uploading the Zitac.Decisions.AvroSerialization.dll to Decisions you currently also must add the Avro.dll found from the NuGet package "Confluent.SchemaRegistry.Serdes.Avro". 