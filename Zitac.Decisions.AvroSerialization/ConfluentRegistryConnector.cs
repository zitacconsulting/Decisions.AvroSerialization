using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;

using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;

namespace Zitac.Decisions.AvroSerialization
{
    [AutoRegisterStep("Schema Registry Connection", "Integration", "Message Queues", "Zitac")]
    [Writable]
    public class SchemaRegistryConnection : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer
    {
        public override OutcomeScenarioData[] OutcomeScenarios
        {
            get
            {
                return new[] {
                    new OutcomeScenarioData("Done", new DataDescription(typeof(object), "Confluence Registry Connection")),
                    new OutcomeScenarioData("Error")
                };
            }
        }

        public DataDescription[] InputData
        {
            get
            { 
                List<DataDescription> inputList = new List<DataDescription>();
                inputList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Schema Registry Url"));
                inputList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(Credentials)), "Credentials"));
                return inputList.ToArray();
            }
        }

        public ResultData Run(StepStartData data)
        {
            try
            {
                string url = data["Schema Registry Url"] as string;
                Credentials InputCredentials = data.Data["Credentials"] as Credentials;

                var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
                {
                    Url = url,
                    BasicAuthUserInfo = InputCredentials.Username + ":" + InputCredentials.Password
                });

                IDeserializer<GenericRecord> deserializer = new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync();

                Dictionary<string, object> dictionary = new Dictionary<string, object>();
                dictionary.Add("Confluence Registry Connection", (object)deserializer);                

                return new ResultData("Done", (IDictionary<string, object>)dictionary);

            } 
            catch (Exception e)
            {
                return new ResultData("Error", (IDictionary<string, object>)new Dictionary<string, object>()
                {
                {
                    "Error Message",
                    (object) e.Message
                }
                });
            }
            
        }
    }
}