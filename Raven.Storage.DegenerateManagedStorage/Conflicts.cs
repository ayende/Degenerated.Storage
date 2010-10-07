using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class Conflicts : SimpleFileTest
    {
        [Fact]
        public void TwoTxCannotAddSameDataBeforeCmmmit()
        {
            Assert.True(persistentDictionary.Add(JToken.FromObject("a"), new byte[] { 1 }, Guid.NewGuid()));

            Assert.False(persistentDictionary.Add(JToken.FromObject("a"), new byte[] { 1 }, Guid.NewGuid()));
        }

        [Fact]
        public void OneTxCannotDeleteTxThatAnotherTxAddedBeforeCommit()
        {
            Assert.True(persistentDictionary.Add(JToken.FromObject("a"), new byte[] { 1 }, Guid.NewGuid()));

            Assert.False(persistentDictionary.Remove(JToken.FromObject("a"), Guid.NewGuid()));
        }


        [Fact]
        public void TwoTxCanAddSameDataAfterCmmmit()
        {
            var txId = Guid.NewGuid();
            Assert.True(persistentDictionary.Add(JToken.FromObject("a"), new byte[] { 1 }, txId));

            persistentDictionary.Commit(txId);

            Assert.True(persistentDictionary.Add(JToken.FromObject("a"), new byte[] { 1 }, Guid.NewGuid()));
        }

        [Fact]
        public void OneTxCanDeleteTxThatAnotherTxAddedAfterCommit()
        {
            var txId = Guid.NewGuid();
            Assert.True(persistentDictionary.Add(JToken.FromObject("a"), new byte[] { 1 }, txId));

            persistentDictionary.Commit(txId);

            Assert.True(persistentDictionary.Remove(JToken.FromObject("a"), Guid.NewGuid()));
        }
    }
}