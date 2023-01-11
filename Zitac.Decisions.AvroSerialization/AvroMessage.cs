using DecisionsFramework.Design.Flow;

namespace Zitac.Decisions.AvroSerialization;

[AutoRegisterMethodsOnClass(true, "Integration", "Message Queues", "Zitac")]
internal class AvroMessage
{
    public static string ConvertBase64AvroToJson(
        string base64string,
        string kafkaTopic,
        string schemaUrl,
        string schemaVersion,
        bool skipSchema = true)
    {
        AvroDeserializer AvDe = new AvroDeserializer(kafkaTopic, schemaUrl, schemaVersion, skipSchema);
        return AvDe.GetFromBase64(base64string);
    }

    public static string ConvertBinaryAvroToJson(
        byte[] bytes,
        string kafkaTopic,
        string schemaUrl,
        string schemaVersion,
        bool skipSchema = true)
    {
        AvroDeserializer AvDe = new AvroDeserializer(kafkaTopic, schemaUrl, schemaVersion, skipSchema);
        return AvDe.GetFromBytes(bytes);
    }
}
