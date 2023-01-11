using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.Design.Properties;

namespace Zitac.Decisions.AvroSerialization
{

    [AutoRegisterStep("Avro Message", "Integration", "Message Queues", "Zitac")]
    [Writable]
    public class AvroMessageFromByteArray : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer
    {
        [WritableValue]
        private bool getSchemaFromHost;
        //[PropertyClassification(new string[] { "Get Schema from Host" })]
        public bool GetSchemaFromHost
        {
            get { return getSchemaFromHost; }
            set
            {
                getSchemaFromHost = value;
                this.OnPropertyChanged("InputData");
            }
        }

        [WritableValue]
        public bool SkipSchemaInResult;

        public DataDescription[] InputData
        {
            get
            {
                List<DataDescription> dataDescriptionList = new List<DataDescription>();
                dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(byte[])), "Byte Array Message"));

                if (!GetSchemaFromHost)
                {
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Schema JSON String"));
                }
                else
                {
                    //dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(SchemaFromHost)), "SchemaFromHost"));
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Host"));
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Topic"));
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Version ('latest')"));
                }
                return dataDescriptionList.ToArray();
            }
        }

        public override OutcomeScenarioData[] OutcomeScenarios
        {
            get
            {
                return new[] {
                    new OutcomeScenarioData("Done", new DataDescription(typeof(string), "Result")),
                    new OutcomeScenarioData("Error", new DataDescription(typeof(string), "Error Message")),
                };
            }
        }

        public ResultData Run(StepStartData data)
        {
            try
            {
                Dictionary<string, object> resultData = new Dictionary<string, object>();
                byte[] ByteArray = data.Data["Byte Array Message"] as byte[];

                SchemaFromHost SchemaFromHost = new SchemaFromHost();

                string result = "";
                if (GetSchemaFromHost)
                {
                    AvroDeserializer message = new AvroDeserializer(
                        data.Data["Topic"] as string,
                        data.Data["Host"] as string,
                        data.Data["Version ('latest')"] as string, 
                        this.SkipSchemaInResult
                    );
                    result = message.GetFromBytes(ByteArray);
                }
                else
                {
                    AvroDeserializer message = new AvroDeserializer(
                        "", 
                        "", 
                        "", 
                        this.SkipSchemaInResult,
                        data.Data["Schema JSON String"] as string
                    );
                    result = message.GetFromBytes(ByteArray);
                }
                
                return new ResultData("Done", (IDictionary<string, object>)new Dictionary<string, object>()
                {
                    {
                        "Result",
                        result
                    }
                });
            }
            catch (Exception e)
            {
                string ExceptionMessage = e.ToString();
                return new ResultData("Error", (IDictionary<string, object>)new Dictionary<string, object>()
                {
                    {
                        "Error Message",
                        (object) ExceptionMessage
                    }
                });
            }
        }

    }
}