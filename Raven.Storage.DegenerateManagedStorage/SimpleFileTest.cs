using System;
using System.IO;
using Raven.ManagedStorage.Degenerate;

namespace Raven.Storage.DegenerateManagedStorage
{
    public class SimpleFileTest : IDisposable
    {
        protected PersistentDictionary persistentDictionary;
        protected FileBasedPersistentSource persistentSource;

        public SimpleFileTest()
        {
            OpenDictionary();
        }

        protected void Reopen()
        {
            Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(Path.GetTempPath(), "test_");
            persistentDictionary = new PersistentDictionary(persistentSource);
        }


        public void Dispose()
        {
            persistentDictionary.Dispose();
            persistentSource.Delete();
        }
    }
}