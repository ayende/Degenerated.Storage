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
    public class PersistentDictionary : IDisposable
    {
        private readonly ConcurrentDictionary<JToken, long> index =
            new ConcurrentDictionary<JToken, long>(JTokenComparer.Instance);

        private readonly ConcurrentDictionary<JToken, Guid> keysModifiedInTx = new ConcurrentDictionary<JToken, Guid>();

        private readonly ConcurrentDictionary<Guid, List<Command>> operationsInTransactions =
            new ConcurrentDictionary<Guid, List<Command>>();

        private readonly IPersistentSource persistentSource;
        private readonly object fileLock = new object();

        private ObjectCache cache = new MemoryCache(Guid.NewGuid().ToString());

        public PersistentDictionary(IPersistentSource persistentSource)
        {
            this.persistentSource = persistentSource;
            Initialze();
        }

        public int WasteCount { get; private set; }

        #region IDisposable Members

        public void Dispose()
        {
            lock (fileLock)
            {
                persistentSource.Dispose();

                operationsInTransactions.Clear();
                keysModifiedInTx.Clear();
            }
        }

        #endregion

        private void Initialze()
        {
            while (true)
            {
                long lastGoodPosition = persistentSource.Log.Position;
                int cmdCount;
                if (ReadInt32(lastGoodPosition, out cmdCount) == false)
                    return;

                var cmds = new Command[cmdCount];
                for (int i = 0; i < cmdCount; i++)
                {
                    cmds[i] = ReadCommand(lastGoodPosition);
                    if (cmds[i] == null)
                        return;
                }

                // now we know we loaded all the operations in the tx, we can apply them.

                ApplyCommands(cmds);
            }
        }

        private void ApplyCommands(IEnumerable<Command> cmds)
        {
            foreach (Command command in cmds)
            {
                switch (command.Type)
                {
                    case CommandType.Put:
                        AddInteral(command.Key, command.Position);
                        break;
                    case CommandType.Delete:
                        RemoveInternal(command.Key);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        private Command ReadCommand(long lastGoodPosition)
        {
            int cmdTypeAsByte = persistentSource.Log.ReadByte();
            if (cmdTypeAsByte == -1) // truncated data?
            {
                persistentSource.Log.SetLength(lastGoodPosition); // truncate the file to remove this
                return null;
            }

            JToken key;
            try
            {
                key = JToken.ReadFrom(new BsonReader(persistentSource.Log));
            }
            catch (Exception)
            {
                persistentSource.Log.SetLength(lastGoodPosition); // truncate the file to remove this
                return null;
            }

            long position = 0;
            var commandType = (CommandType) cmdTypeAsByte;
            if (commandType == CommandType.Put)
            {
                if (ReadInt64(lastGoodPosition, out position) == false)
                    return null;
            }
            return new Command
            {
                Key = key,
                Position = position,
                Type = commandType
            };
        }

        private bool ReadInt32(long lastGoodPosition, out int value)
        {
            int read = 0;
            value = -1;
            var buf = new byte[sizeof (int)];
            do
            {
                int dataRead = persistentSource.Log.Read(buf, read, buf.Length - read);
                if (dataRead == 0) // nothing read, EOF, probably truncated write, 
                {
                    persistentSource.Log.SetLength(lastGoodPosition); // truncate the file to remove this
                    return false;
                }
                read += dataRead;
            } while (read < buf.Length);
            value = BitConverter.ToInt32(buf, 0);
            return true;
        }


        private bool ReadInt64(long lastGoodPosition, out long value)
        {
            int read = 0;
            value = -1;
            var buf = new byte[sizeof (long)];
            do
            {
                int dataRead = persistentSource.Log.Read(buf, read, buf.Length - read);
                if (dataRead == 0) // nothing read, EOF, probably truncated write, 
                {
                    persistentSource.Log.SetLength(lastGoodPosition); // truncate the file to remove this
                    return false;
                }
                read += dataRead;
            } while (read < buf.Length);
            value = BitConverter.ToInt64(buf, 0);
            return true;
        }

        public bool Add(JToken key, byte[] value, Guid txId)
        {
            lock (fileLock)
            {
                Guid existing;
                if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                    return false;

                long position = persistentSource.Data.Position = persistentSource.Data.Length;
                    // we *always* write to the end
                byte[] lenInBytes = BitConverter.GetBytes(value.Length);
                persistentSource.Data.Write(lenInBytes, 0, lenInBytes.Length);
                persistentSource.Data.Write(value, 0, value.Length);

                operationsInTransactions.GetOrAdd(txId, new List<Command>())
                    .Add(new Command
                    {
                        Key = key,
                        Position = position,
                        Type = CommandType.Put
                    });

                if (existing != txId) // otherwise we are already there
                    keysModifiedInTx.TryAdd(key, txId);

                return true;
            }
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
                            return ReadData(command.Position);
                        case CommandType.Delete:
                            return null;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            long pos;
            if (index.TryGetValue(key, out pos) == false)
                return null;

            return ReadData(pos);
        }

        private byte[] ReadData(long pos)
        {
            var cacheKey = pos.ToString();
            var cached = cache.Get(cacheKey);
            if (cached != null)
                return (byte[]) cached;

            byte[] buf;

            lock (fileLock)
            {
                cached = cache.Get(cacheKey);
                if (cached != null)
                    return (byte[])cached;

                buf = ReadDataNoCaching(pos);
            }

            cache[cacheKey] = buf;

            return buf;
        }

        private byte[] ReadDataNoCaching(long pos)
        {
            persistentSource.Data.Position = pos;

            int read = 0;
            var buf = new byte[sizeof(int)];
            do
            {
                int dataRead = persistentSource.Data.Read(buf, read, buf.Length - read);
                if (dataRead == 0) // nothing read, EOF, probably truncated write, 
                {
                    throw new InvalidDataException("Could not read data length, the data file is corrupt");
                }
                read += dataRead;
            } while (read < buf.Length);

            int len = BitConverter.ToInt32(buf, 0);

            read = 0;
            buf = new byte[len];
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

        public void Commit(Guid txId)
        {
            List<Command> cmds;
            if (operationsInTransactions.TryGetValue(txId, out cmds) == false)
                return;

            lock (fileLock)
            {
                persistentSource.FlushData(); // sync the data to disk before doing anything else

                byte[] count = BitConverter.GetBytes(cmds.Count);
                persistentSource.Log.Write(count, 0, count.Length);
                foreach (Command command in cmds)
                {
                    WriteCommand(command, persistentSource.Log);
                }

                persistentSource.FlushLog(); // flush all the index changes to disk

                ApplyCommands(cmds);

                ClearTransactionInMemoryData(txId);

                if (RequiresOptimization())
                    Optimize();
            }
        }

        private void Optimize()
        {
            lock (fileLock)
            {
                Stream tempLog = persistentSource.CreateTemporaryStream();
                Stream tempData = persistentSource.CreateTemporaryStream();

                foreach (var kvp in index) // copy committed data
                {
                    long pos = tempData.Position;
                    byte[] data = ReadData(kvp.Value);

                    byte[] lenInBytes = BitConverter.GetBytes(data.Length);
                    tempData.Write(lenInBytes, 0, lenInBytes.Length);
                    tempData.Write(data, 0, data.Length);

                    WriteCommand(new Command
                    {
                        Key = kvp.Key,
                        Position = pos,
                        Type = CommandType.Put
                    }, tempLog);

                    index.TryUpdate(kvp.Key, pos, kvp.Value);
                }

                // copy uncommitted data
                foreach (
                    Command uncommitted in
                        operationsInTransactions.SelectMany(x => x.Value).Where(x => x.Type == CommandType.Put))
                {
                    long pos = tempData.Position;
                    byte[] data = ReadData(uncommitted.Position);

                    byte[] lenInBytes = BitConverter.GetBytes(data.Length);
                    tempData.Write(lenInBytes, 0, lenInBytes.Length);
                    tempData.Write(data, 0, data.Length);

                    uncommitted.Position = pos;
                }

                persistentSource.ReplaceAtomically(tempData, tempLog);
            }
        }

        private bool RequiresOptimization()
        {
            if (index.Count < 10000) // for small data sizes, we cleanup on 100% waste
                return WasteCount > index.Count;
            if (index.Count < 100000) // for meduim data sizes, we cleanup on 50% waste
                return WasteCount > (index.Count/2);
            return WasteCount > (index.Count/10); // on large data size, we cleanup on 10% waste
        }

        private static void WriteCommand(Command command, Stream log)
        {
            log.WriteByte((byte) command.Type);
            command.Key.WriteTo(new BsonWriter(log));
            if (command.Type != CommandType.Put)
                return;

            byte[] bytes = BitConverter.GetBytes(command.Position);
            log.Write(bytes, 0, bytes.Length);
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
            lock (fileLock)
            {
                Guid existing;
                if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                    return false;

                operationsInTransactions.GetOrAdd(txId, new List<Command>())
                    .Add(new Command
                    {
                        Key = key,
                        Type = CommandType.Delete
                    });

                if (existing != txId) // otherwise we are already there
                    keysModifiedInTx.TryAdd(key, txId);

                return true;
            }
        }

        public void AddInteral(JToken key, long position)
        {
            index.AddOrUpdate(key, position, (token, oldPos) =>
            {
                WasteCount += 1;
                return position;
            });
        }

        public void RemoveInternal(JToken key)
        {
            long _;
            index.TryRemove(key, out _);
            WasteCount += 1;
        }
    }
}