using Newtonsoft.Json.Linq;
using Raven.ManagedStorage.Degenerate.Commands;

namespace Raven.ManagedStorage.Degenerate
{
    public class Command
    {
        public JToken Key { get; set; }
        public CommandType Type { get; set; }
        public long Position { get; set; }
    }
}