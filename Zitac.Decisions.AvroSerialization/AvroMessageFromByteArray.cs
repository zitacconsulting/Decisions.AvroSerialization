using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Flow;
using DecisionsFramework.Design.Flow.CoreSteps;
using DecisionsFramework.Design.Flow.Mapping;
using DecisionsFramework.Design.Properties;
using System.Runtime.Serialization;

namespace Zitac.Decisions.AvroSerialization
{
    [AutoRegisterStep("Avro Message", "Integration", "Message Queues", "Zitac")]
    [Writable]
    public class AvroMessageFromByteArray : BaseFlowAwareStep, ISyncStep, IDataConsumer, IDataProducer
    {
        [WritableValue]
        private string messageType = "Byte[]";

        [WritableValue]
        [DataMember]
        [PropertyClassification(10, "Message type", new string[] { "[Settings]" })]
        [SelectStringEditor("MessageTypeSelect")]
        public string MessageType
        {
            get { return messageType; }
            set {
                messageType = value;
                this.OnPropertyChanged("InputData");
            }
        }

        //private string MessageTypeSelected;

        [PropertyHidden]
        public string[] MessageTypeSelect
        {
            get
            {
                return new[] {
                    "Byte[]", 
                    "Base64 String"
                };
            }
            set {
                return; 
            }
        }

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
                if(MessageType == "Byte[]")
                { 
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(byte)), "Byte Array Message", true, false, false));
                }
                else
                {
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Base64 Message"));
                }
                

                if (!GetSchemaFromHost)
                {
                    dataDescriptionList.Add(new DataDescription((DecisionsType)new DecisionsNativeType(typeof(string)), "Schema JSON String"));
                }
                else
                {
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
                

                //SchemaFromHost SchemaFromHost = new SchemaFromHost();

                string result = "";
                if (GetSchemaFromHost)
                {
                    AvroDeserializer message = new AvroDeserializer(
                        data.Data["Topic"] as string,
                        data.Data["Host"] as string,
                        data.Data["Version ('latest')"] as string,
                        this.SkipSchemaInResult
                    );
                    if (MessageType == "Byte[]") 
                    {
                        byte[] ByteArray = data.Data["Byte Array Message"] as byte[];
                        result = message.GetFromBytes(ByteArray); 
                    }
                    else
                    {
                        string Messagestring = data.Data["Base64 Message"] as string;
                        result = message.GetFromBase64(Messagestring);
                    }
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
                    if (MessageType == "Byte[]")
                    {
                        byte[] ByteArray = data.Data["Byte Array Message"] as byte[];
                        result = message.GetFromBytes(ByteArray);
                    }
                    else
                    {
                        string Messagestring = data.Data["Base64 Message"] as string;
                        result = message.GetFromBase64(Messagestring);
                    }
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