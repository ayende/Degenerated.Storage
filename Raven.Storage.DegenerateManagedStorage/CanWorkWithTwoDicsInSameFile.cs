using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class CanWorkWithTwoDicsInSameFile : MultiDicInSingleFile
    {
        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValues()
        {
            var txId = Guid.NewGuid();

            persistentDictionaryOne.Add(JToken.FromObject(1), new byte[] { 1, 2 }, txId);
            persistentDictionaryTwo.Add(JToken.FromObject(1), new byte[] { 2, 3 }, txId);

            Assert.Equal(new byte[] { 1, 2, }, persistentDictionaryOne.Read(JToken.FromObject(1), txId));
            Assert.Equal(new byte[] { 2, 3 }, persistentDictionaryTwo.Read(JToken.FromObject(1), txId));
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommit()
        {
            var txId = Guid.NewGuid();

            persistentDictionaryOne.Add(JToken.FromObject(1), new byte[] { 1, 2 }, txId);
            persistentDictionaryTwo.Add(JToken.FromObject(1), new byte[] { 2, 3 }, txId);

            Commit(txId);
            Commit(txId);

            Assert.Equal(new byte[] { 1, 2, }, persistentDictionaryOne.Read(JToken.FromObject(1), txId));
            Assert.Equal(new byte[] { 2, 3 }, persistentDictionaryTwo.Read(JToken.FromObject(1), txId));
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommitAndReopen()
        {
            var txId = Guid.NewGuid();

            persistentDictionaryOne.Add(JToken.FromObject(1), new byte[] { 1, 2 }, txId);
            persistentDictionaryTwo.Add(JToken.FromObject(1), new byte[] { 2, 3 }, txId);

            aggregateDictionary.Commit(txId);

            Reopen();

            Assert.Equal(new byte[] { 1, 2, }, persistentDictionaryOne.Read(JToken.FromObject(1), txId));
            Assert.Equal(new byte[] { 2, 3 }, persistentDictionaryTwo.Read(JToken.FromObject(1), txId));
        }
    }
}