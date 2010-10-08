using System;
using System.IO;

namespace Raven.ManagedStorage.Degenerate
{
    public interface IPersistentSource : IDisposable
    {
        object SyncLock { get; }
        Stream Data { get; }
        Stream Log { get; }

        void ReplaceAtomically(Stream data, Stream log);

        Stream CreateTemporaryStream();

        void FlushData();
        void FlushLog();
    }
}