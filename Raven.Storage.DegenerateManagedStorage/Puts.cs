using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class Puts : SimpleFileTest
    {
        [Fact]
        public void CanStartAndStopPersistentDictionary()
        {
            // all work happens in ctor & dispose
        }

        [Fact]
        public void CanAddAndGetDataSameTx()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Add(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            var data = persistentDictionary.Read(JToken.FromObject("123"), txId);
            
            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data);
        }

        [Fact]
        public void AfterAddInDifferentTxValueDoesNotExists()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Add(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            var data = persistentDictionary.Read(JToken.FromObject("123"), Guid.NewGuid());
            Assert.Null(data);
        }

        [Fact]
        public void AfterCommitValueIsVisibleToAllTx()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Add(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            persistentDictionary.Commit(txId);

            var data = persistentDictionary.Read(JToken.FromObject("123"), Guid.NewGuid());
       
            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data);
        }


        [Fact]
        public void AfterRollbackValueIsGoneToAllTx()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Add(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            persistentDictionary.Rollback(txId);

            Assert.Null(persistentDictionary.Read(JToken.FromObject("123"), txId));
            Assert.Null(persistentDictionary.Read(JToken.FromObject("123"), Guid.NewGuid()));
        }

        [Fact]
        public void AddReadAndThenAddWillNotCorruptData()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Add(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            Assert.True(persistentDictionary.Add(JToken.FromObject("789"), new byte[] { 3, 1, 4, 5 }, txId));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, persistentDictionary.Read(JToken.FromObject("123"), txId));

            Assert.True(persistentDictionary.Add(JToken.FromObject("456"), new byte[] { 4, 5 }, txId));

            Assert.Equal(new byte[] { 3, 1, 4, 5 }, persistentDictionary.Read(JToken.FromObject("789"), txId));
        }
    }
}