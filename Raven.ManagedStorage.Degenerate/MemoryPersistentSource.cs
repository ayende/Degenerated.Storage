﻿using System;
using System.IO;

namespace Raven.ManagedStorage.Degenerate
{
    public class MemoryPersistentSource : IPersistentSource
    {
        public MemoryPersistentSource()
        {
            Data = new MemoryStream();
            Log = new MemoryStream();
        }

        public Stream Data
        {
            get; set;
        }

        public Stream Log
        {
            get; set;
        }

        public void ReplaceAtomically(Stream data, Stream log)
        {
            Data = data;
            Log = log;
        }

        public Stream CreateTemporaryStream()
        {
            return new MemoryStream();
        }

        public void FlushData()
        {
        }

        public void FlushLog()
        {
        }

        public void Dispose()
        {
        }
    }
}