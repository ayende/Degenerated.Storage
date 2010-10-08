using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class Optimize : SimpleFileTest
    {
        [Fact]
        public void AfterManyModificationsFileSizeWillGoDownOnCommit()
        {
            var txId = Guid.NewGuid();
            for (int i = 0; i < 16; i++)
            {
                persistentDictionary.Add(JToken.FromObject(i), new byte[512], txId);
            }

            for (int i = 0; i < 16; i++)
            {
                persistentDictionary.Remove(JToken.FromObject(i), txId);
            }

            var oldSize = persistentSource.Data.Length;

            Commit(txId);

            Compact();

            Assert.True( oldSize > persistentSource.Data.Length);
        }

        [Fact]
        public void AfterOptimizeCommittedDataIsStillThere()
        {
            var txId = Guid.NewGuid();
            for (int i = 0; i < 16; i++)
            {
                persistentDictionary.Add(JToken.FromObject(i), new byte[512], txId);
            }

            for (int i = 0; i < 16; i++)
            {
                persistentDictionary.Remove(JToken.FromObject(i), txId);
            }

            persistentDictionary.Add(JToken.FromObject("a"), new byte[512], txId);
           
            var oldSize = persistentSource.Data.Length;

            Commit(txId);
            Compact();

            Assert.True(oldSize > persistentSource.Data.Length);

            Assert.NotNull(
                persistentDictionary.Read(JToken.FromObject("a"), Guid.NewGuid())
                );
        }


        [Fact]
        public void AfterOptimizeUnCommittedDataIsStillThere()
        {
            var txId = Guid.NewGuid();
            for (int i = 0; i < 16; i++)
            {
                persistentDictionary.Add(JToken.FromObject(i), new byte[512], txId);
            }

            for (int i = 0; i < 16; i++)
            {
                persistentDictionary.Remove(JToken.FromObject(i), txId);
            }

            var txId2 = Guid.NewGuid();
            persistentDictionary.Add(JToken.FromObject("a"), new byte[512], txId2);

            var oldSize = persistentSource.Data.Length;

            Commit(txId);
            Compact();

            Assert.True(oldSize > persistentSource.Data.Length);

            Assert.NotNull(
                persistentDictionary.Read(JToken.FromObject("a"), txId2)
                );
        }
    }
}