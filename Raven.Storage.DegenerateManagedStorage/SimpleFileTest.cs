using System;
using System.IO;
using Raven.ManagedStorage.Degenerate;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class SimpleFileTest : IDisposable
    {
        protected PersistentDictionary persistentDictionary;
        protected FileBasedPersistentSource persistentSource;
        private readonly string tempPath;
        private AggregateDictionary aggregateDictionary;

        public SimpleFileTest()
        {
            tempPath = Path.GetTempPath();
            OpenDictionary();
        }

        protected void Reopen()
        {
            persistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(tempPath, "test_");
            aggregateDictionary = new AggregateDictionary(persistentSource, 1);
            persistentDictionary = aggregateDictionary[0];
        }

        protected void Compact()
        {
            aggregateDictionary.Compact();
        }

        protected void Commit(Guid txId)
        {
            aggregateDictionary.Commit(txId);
        }

        public void Dispose()
        {
            persistentSource.Dispose();
            persistentSource.Delete();
        }
    }
}