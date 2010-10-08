using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Caching;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.ManagedStorage.Degenerate.Commands;

namespace Raven.ManagedStorage.Degenerate
{
    public class PersistentDictionary 
    {
        private class PositionInFile
        {
            public long Position { get; set; }
            public int Size { get; set; }
        }

        private readonly ConcurrentDictionary<JToken, PositionInFile> index =
            new ConcurrentDictionary<JToken, PositionInFile>(JTokenComparer.Instance);

        private readonly ConcurrentDictionary<JToken, Guid> keysModifiedInTx = new ConcurrentDictionary<JToken, Guid>();

        private readonly ConcurrentDictionary<Guid, List<Command>> operationsInTransactions =
            new ConcurrentDictionary<Guid, List<Command>>();

        private readonly IPersistentSource persistentSource;

        private readonly ObjectCache cache = new MemoryCache(Guid.NewGuid().ToString());
        public int DictionaryId { get; set; }

        public PersistentDictionary(IPersistentSource persistentSource)
        {
            this.persistentSource = persistentSource;
        }

        public int WasteCount { get; private set; }

        internal void ApplyCommands(IEnumerable<Command> cmds)
        {
            foreach (Command command in cmds)
            {
                switch (command.Type)
                {
                    case CommandType.Put:
                        AddInteral(command.Key, new PositionInFile
                        {
                            Position = command.Position,
                            Size = command.Size
                        });
                        break;
                    case CommandType.Delete:
                        RemoveInternal(command.Key);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public bool Add(JToken key, byte[] value, Guid txId)
        {

            Guid existing;
            if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                return false;

            long position;
            lock (persistentSource.SyncLock)
            {
                // we *always* write to the end
                position = persistentSource.Data.Position = persistentSource.Data.Length;
                persistentSource.Data.Write(value, 0, value.Length);
            }
            operationsInTransactions.GetOrAdd(txId, new List<Command>())
                .Add(new Command
                {
                    Key = key,
                    Position = position,
                    Size = value.Length,
                    DictionaryId = DictionaryId,
                    Type = CommandType.Put
                });

            if (existing != txId) // otherwise we are already there
                keysModifiedInTx.TryAdd(key, txId);

            return true;
        }

        public byte[] Read(JToken key, Guid txId)
        {
            Guid mofiedByTx;
            if (keysModifiedInTx.TryGetValue(key, out mofiedByTx) && mofiedByTx == txId)
            {
                Command command = operationsInTransactions.GetOrAdd(txId, new List<Command>()).LastOrDefault(
                    x => JTokenComparer.Instance.Equals(x.Key, key));

                if (command != null)
                {
                    switch (command.Type)
                    {
                        case CommandType.Put:
                            return ReadData(command.Position, command.Size);
                        case CommandType.Delete:
                            return null;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            PositionInFile pos;
            if (index.TryGetValue(key, out pos) == false)
                return null;

            return ReadData(pos.Position, pos.Size);
        }

        private byte[] ReadData(long pos, int size)
        {
            var cacheKey = pos.ToString();
            var cached = cache.Get(cacheKey);
            if (cached != null)
                return (byte[]) cached;

            byte[] buf;

            lock (persistentSource.SyncLock)
            {
                cached = cache.Get(cacheKey);
                if (cached != null)
                    return (byte[])cached;

                buf = ReadDataNoCaching(pos, size);
            }

            cache[cacheKey] = buf;

            return buf;
        }

        private byte[] ReadDataNoCaching(long pos, int size)
        {
            persistentSource.Data.Position = pos;

            var read = 0;
            var buf = new byte[size];
            do
            {
                int dataRead = persistentSource.Data.Read(buf, read, buf.Length - read);
                if (dataRead == 0) // nothing read, EOF, probably truncated write, 
                {
                    throw new InvalidDataException("Could not read complete data, the data file is corrupt");
                }
                read += dataRead;
            } while (read < buf.Length);
            return buf;
        }

        internal List<Command> GetCommandsToCommit(Guid txId)
        {
            List<Command> cmds;
            if (operationsInTransactions.TryGetValue(txId, out cmds) == false)
                return null;

            return cmds;
        }

        internal void CompleteCommit(Guid txId)
        {
            List<Command> cmds;
            if (operationsInTransactions.TryGetValue(txId, out cmds) == false)
                return;

            ApplyCommands(cmds);
            ClearTransactionInMemoryData(txId);
        }

        public void Rollback(Guid txId)
        {
            ClearTransactionInMemoryData(txId);
        }

        private void ClearTransactionInMemoryData(Guid txId)
        {
            List<Command> commands;
            if (operationsInTransactions.TryRemove(txId, out commands) == false)
                return;

            foreach (Command command in commands)
            {
                Guid _;
                keysModifiedInTx.TryRemove(command.Key, out _);
            }
        }

        public bool Remove(JToken key, Guid txId)
        {
            Guid existing;
            if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                return false;

            operationsInTransactions.GetOrAdd(txId, new List<Command>())
                .Add(new Command
                {
                    Key = key,
                    DictionaryId = DictionaryId,
                    Type = CommandType.Delete
                });

            if (existing != txId) // otherwise we are already there
                keysModifiedInTx.TryAdd(key, txId);

            return true;
        }

        private void AddInteral(JToken key, PositionInFile position)
        {
            index.AddOrUpdate(key, position, (token, oldPos) =>
            {
                WasteCount += 1;
                return position;
            });
        }

        private void RemoveInternal(JToken key)
        {
            PositionInFile _;
            index.TryRemove(key, out _);
            WasteCount += 1;
        }

        internal void CopyCommittedData(Stream tempData, List<Command> cmds)
        {
            foreach (var kvp in index) // copy committed data
            {
                long pos = tempData.Position;
                byte[] data = ReadData(kvp.Value.Position, kvp.Value.Size);

                byte[] lenInBytes = BitConverter.GetBytes(data.Length);
                tempData.Write(lenInBytes, 0, lenInBytes.Length);
                tempData.Write(data, 0, data.Length);

                cmds.Add(new Command
                {
                    Key = kvp.Key,
                    Position = pos,
                    DictionaryId = DictionaryId,
                    Size = kvp.Value.Size,
                    Type = CommandType.Put
                });

                kvp.Value.Position = pos;
            }
        }

        public void CopyUncommitedData(Stream tempData)
        {
            // copy uncommitted data
            foreach (Command uncommitted in operationsInTransactions
                .SelectMany(x => x.Value)
                .Where(x => x.Type == CommandType.Put))
            {
                long pos = tempData.Position;
                byte[] data = ReadData(uncommitted.Position, uncommitted.Size);

                byte[] lenInBytes = BitConverter.GetBytes(data.Length);
                tempData.Write(lenInBytes, 0, lenInBytes.Length);
                tempData.Write(data, 0, data.Length);

                uncommitted.Position = pos;
            }
        }
    }
}