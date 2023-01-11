using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Avro;
using Avro.Generic;

namespace Zitac.Decisions.AvroSerialization
{
    internal class AvroDeserializer
    {
        private string kafkaTopic, schemaUrl, schemaVersion;
        private bool skipSchema = true; 

        public AvroDeserializer(
            string kafkaTopic,
            string schemaUrl,
            string schemaVersion,
            bool skipSchema = true
            )
        {
            this.kafkaTopic = kafkaTopic;
            this.schemaUrl = schemaUrl;
            this.schemaVersion = schemaVersion;
            this.skipSchema = skipSchema;

        }

        public string GetFromBase64(string message)
        {
            return DoMessage(Convert.FromBase64String(message));
        }

        public string GetFromBytes(byte[] message)
        {
            return DoMessage(message);
        }

        public string GetFromBlob(ConsumeResult<Ignore, byte[]> message)
        {
            return DoMessage(message.Value);
        }

        private string DoMessage(byte[] message)
        {
            var reader = new Avro.IO.BinaryDecoder(new MemoryStream(message));
            var avro = Avro.Schema.Parse(SchemaAsString());
            var deserialized = new GenericDatumReader<GenericRecord>(avro, avro);
            var data = deserialized.Read(null, reader);

            var objString = JsonConvert.SerializeObject(data, new AvroSchemaConverter(avro));

            if (this.skipSchema)
            {
                JObject obj = JObject.Parse(objString);
                obj.Remove("schema");
                objString = JsonConvert.SerializeObject(obj);
            }

            return objString;
        }

        private string SchemaAsString()
        {
            var schemaRegistryUrl = this.schemaUrl + "/subjects/" + this.kafkaTopic + "-value/versions/" + this.schemaVersion;
            //var schemaRegistryUrl = "http://localhost:8081/subjects/linustest15-value/versions/latest";
            var httpClient = new HttpClient();
            var response = httpClient.GetAsync(schemaRegistryUrl).Result;
            var schema = JObject.Parse(response.Content.ReadAsStringAsync().Result);
            return schema["schema"].ToString();
        }

        private class AvroSchemaConverter : JsonConverter
        {
            public Schema AvroSchema { get; set; }
            public AvroSchemaConverter(Schema schema)
            {
                this.AvroSchema = schema;
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(GenericRecord);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                throw new NotImplementedException();
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var record = (GenericRecord)value;
                var jsonObject = new JObject();
                if (AvroSchema is RecordSchema recordSchema)
                {
                    for (int i = 0; i < recordSchema.Fields.Count; i++)
                    {
                        jsonObject.Add(recordSchema.Fields[i].Name, JToken.FromObject(record[recordSchema.Fields[i].Name]));
                    }
                }
                jsonObject.Add("schema", JToken.FromObject(AvroSchema.ToString()));
                jsonObject.WriteTo(writer);
            }
        }
    }
}
