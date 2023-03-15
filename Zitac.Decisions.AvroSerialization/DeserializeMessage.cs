using DecisionsFramework.Design.Flow;

using Avro.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;
using System.Security.Cryptography.X509Certificates;
using System.Runtime.Serialization;

namespace Zitac.Decisions.AvroSerialization
{
    [AutoRegisterStep("Deserialize Message", "Integration", "Message Queues", "Zitac")]
    public class DeserializeMessage : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer
    {
        public DataDescription[] InputData
        {
            get
            {
                List<DataDescription> inputList = new List<DataDescription>();
                inputList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(byte)), "Byte Array Message", true, false, false));
                inputList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(object)), "Registry Connection"));

                return inputList.ToArray();
            }
        }

        public override OutcomeScenarioData[] OutcomeScenarios
        {
            get
            {
                return new[] {
                    new OutcomeScenarioData("Done", new DataDescription(typeof(string), "JSON Message")),
                    new OutcomeScenarioData("Error")
                };
            }
        }

        public ResultData Run(StepStartData data)
        {
            try
            {
                object connector = data["Registry Connection"] as object;
                byte[] message = data["Byte Array Message"] as byte[];

                IDeserializer<GenericRecord> deserializer = (IDeserializer<GenericRecord>)connector;
                var deserializedMessage = deserializer.Deserialize(message, false, new SerializationContext());
                var contents = ExtractContents(deserializedMessage);

                Dictionary<string, object> dictionary = new Dictionary<string, object>();
                dictionary.Add("JSON Message", (string)JsonConvert.SerializeObject(contents));

                return new ResultData("Done", (IDictionary<string, object>)dictionary);

            }
            catch(Exception e)
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

        private static object ExtractContents(object obj)
        {
            if (obj is GenericRecord record)
            {
                var contents = new Dictionary<string, object>();

                foreach (var field in record.Schema.Fields)
                {
                    var value = ExtractContents(record[field.Name]);

                    if (value != null)
                    {
                        contents[field.Name] = value;
                    }
                }

                return contents;
            }
            else if (obj is ICollection<object> collection)
            {
                var contents = new List<object>();

                foreach (var item in collection)
                {
                    var value = ExtractContents(item);

                    if (value != null)
                    {
                        contents.Add(value);
                    }
                }

                return contents;
            }
            else
            {
                return obj;
            }
        }
    }
}
