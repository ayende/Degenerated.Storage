using System;
using System.IO;
using Raven.ManagedStorage.Degenerate;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class MultiDicInSingleFile : IDisposable
    {
        protected PersistentDictionary persistentDictionaryOne;
        protected FileBasedPersistentSource persistentSource;
        protected PersistentDictionary persistentDictionaryTwo;
        protected AggregateDictionary aggregateDictionary;

        public MultiDicInSingleFile()
        {
            OpenDictionary();
        }

        #region IDisposable Members

        public void Dispose()
        {
            persistentSource.Dispose();
            persistentSource.Delete();
        }

        #endregion

        protected void Commit(Guid txId)
        {
            aggregateDictionary.Commit(txId);
        }

        protected void Reopen()
        {
            persistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(Path.GetTempPath(), "test_");
            aggregateDictionary = new AggregateDictionary(persistentSource, 2);
            persistentDictionaryOne = aggregateDictionary[0];
            persistentDictionaryTwo = aggregateDictionary[1];
        }
    }
}