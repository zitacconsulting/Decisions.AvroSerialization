using DecisionsFramework.Design.ConfigurationStorage.Attributes;
using DecisionsFramework.Design.Flow.Service.Debugging.DebugData;
using DecisionsFramework.Design.Properties.Attributes;

namespace Zitac.Decisions.AvroSerialization
{
    internal class Credentials : IDebuggerJsonProvider
    {
        [WritableValue]
        public string Username { get; set; }

        [WritableValue]
        [PasswordText]
        public string Password { get; set; }

        public object GetJsonDebugView()
        {
            return new
            {
                Username = this.Username,
                Password = "********"
            };
        }
    }
}
