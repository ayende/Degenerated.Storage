﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.ManagedStorage.Degenerate.Commands;

namespace Raven.ManagedStorage.Degenerate
{
    public class AggregateDictionary
    {
        private readonly IPersistentSource persistentSource;
        private readonly PersistentDictionary[] dictionaries;

        public AggregateDictionary(IPersistentSource persistentSource, int countOfDictionaries)
        {
            this.persistentSource = persistentSource;
            dictionaries = new PersistentDictionary[countOfDictionaries];

            for (var i = 0; i < countOfDictionaries; i++)
            {
                dictionaries[i] = new PersistentDictionary(persistentSource)
                {
                    DictionaryId = i
                };
            }

            Initialze();
        }

        private void Initialze()
        {
            while (true)
            {
                long lastGoodPosition = persistentSource.Log.Position;

                if (persistentSource.Log.Position == persistentSource.Log.Length)
                    break;// EOF

                var cmds = ReadCommands(lastGoodPosition);
                if (cmds == null)
                    break;

                foreach (var commandForDictionary in cmds.GroupBy(x => x.DictionaryId))
                {
                    dictionaries[commandForDictionary.Key].ApplyCommands(commandForDictionary);
                }
            }
        }

        private IEnumerable<Command> ReadCommands(long lastGoodPosition)
        {
            try
            {
                var cmds = JToken.ReadFrom(new BsonReader(persistentSource.Log));
                return cmds.Values().Select(cmd => new Command
                {
                    Key = cmd.Value<JToken>("key"),
                    Position = cmd.Value<long>("position"),
                    Size = cmd.Value<int>("size"),
                    Type = (CommandType)cmd.Value<byte>("type"),
                    DictionaryId = cmd.Value<int>("dicId")
                }).ToArray();
            }
            catch (Exception)
            {
                persistentSource.Log.SetLength(lastGoodPosition);//truncate log to last known good position
                return null;
            }
        }

        public PersistentDictionary this[int i]
        {
            get { return dictionaries[i]; }
        }

        public void Commit(Guid txId)
        {
            lock (persistentSource.SyncLock)
            {
                var cmds = new List<Command>();
                foreach (var persistentDictionary in dictionaries)
                {
                    var commandsToCommit = persistentDictionary.GetCommandsToCommit(txId);
                    if(commandsToCommit == null)
                        continue;
                    cmds.AddRange(commandsToCommit);
                }

                WriteCommands(cmds, persistentSource.Log);

                persistentSource.FlushData(); // sync the data to disk before doing anything else
                WriteCommands(cmds, persistentSource.Log);
                persistentSource.FlushLog(); // flush all the index changes to disk

                foreach (var persistentDictionary in dictionaries)
                {
                    persistentDictionary.CompleteCommit(txId);
                }
            }
        }

        private static void WriteCommands(IEnumerable<Command> cmds, Stream log)
        {
            var array = new JArray();
            foreach (var command in cmds)
            {
                var cmd = new JObject
                {
                    {"type", (byte) command.Type},
                    {"key", command.Key},
                    {"dicId", command.DictionaryId}
                };

                if (command.Type == CommandType.Put)
                {
                    cmd.Add("position", command.Position);
                    cmd.Add("size", command.Size);
                }

                array.Add(cmd);
            }

            array.WriteTo(new BsonWriter(log));
        }


        public void Compact()
        {
            lock (persistentSource.SyncLock)
            {
                Stream tempLog = persistentSource.CreateTemporaryStream();
                Stream tempData = persistentSource.CreateTemporaryStream();

                var cmds = new List<Command>();
                foreach (var persistentDictionary in dictionaries)
                {
                    persistentDictionary.CopyCommittedData(tempData, cmds);
                    persistentDictionary.CopyUncommitedData(tempData);
                }

                WriteCommands(cmds, tempLog);

                persistentSource.ReplaceAtomically(tempData, tempLog);
            }
        }


        /// <summary>
        /// This method should be called when the application is idle
        /// It is used for book keeping tasks such as compacting the data storage file.
        /// </summary>
        public void PerformIdleTasks()
        {
            if (CompactionRequired() == false) 
                return;

            Compact();
        }


        private bool CompactionRequired()
        {
            var itemsCount = dictionaries.Sum(x => x.ItemCount);
            var wasteCount = dictionaries.Sum(x => x.WasteCount);

            if (itemsCount < 10000) // for small data sizes, we cleanup on 100% waste
                return wasteCount > itemsCount;
            if (itemsCount < 100000) // for meduim data sizes, we cleanup on 50% waste
                return wasteCount > (itemsCount / 2);
            return wasteCount > (itemsCount / 10); // on large data size, we cleanup on 10% waste
        }
    }
}