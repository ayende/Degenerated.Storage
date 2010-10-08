﻿using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.ManagedStorage.Degenerate
{
    public class JTokenComparer : IComparer<JToken>, IEqualityComparer<JToken>
    {
        public static JTokenComparer Instance = new JTokenComparer();

        public int Compare(JToken x, JToken y)
        {
            // null handling
            if (x == null && y == null)
                return 0;
            if (x == null)
                return -1;
            if (y == null)
                return 1;

            if (x.Type != y.Type)
                return (x.Type - y.Type); // this just to force consistent equality

            switch (x.Type)
            {
                case JTokenType.None:
                case JTokenType.Undefined:
                case JTokenType.Null:
                    return 0;//both are nil
                case JTokenType.Object:
                    // that here we only compare _x_ properties, that is intentional and allows to create partial searches
                    // we compare based on _y_ properties order, because that is more stable in our usage
                    var xObj = (JObject)x;
                    var yObj = (JObject)y;
                    foreach (var prop in yObj)
                    {
                        JToken value;
                        if (xObj.TryGetValue(prop.Key, out value) == false)
                            continue;
                        var compare = Compare(prop.Value, value);
                        if (compare != 0)
                            return compare;
                    }
                    if (xObj.Count > yObj.Count)// yObj has missing properties
                        return -1;
                    return 0;
                case JTokenType.Array:
                    var xArray = (JArray)x;
                    var yArray = (JArray)y;

                    for (int i = 0; i < xArray.Count && i < yArray.Count; i++)
                    {
                        var compare = Compare(xArray[i], yArray[i]);
                        if (compare == 0)
                            continue;
                        return compare;
                    }
                    return xArray.Count - yArray.Count;
                case JTokenType.Property:
                    var xProp = ((JProperty)x);
                    var yProp = ((JProperty)y);
                    var compareTo = xProp.Name.CompareTo(yProp.Name);
                    if (compareTo != 0)
                        return compareTo;
                    return Compare(xProp.Value, yProp.Value);
                case JTokenType.Integer:
                    return x.Value<long>().CompareTo(y.Value<long>());
                case JTokenType.Float:
                    return (x.Value<double>()).CompareTo(y.Value<double>());
                case JTokenType.String:
                    return x.Value<string>().CompareTo(y.Value<string>());
                case JTokenType.Boolean:
                    return x.Value<bool>().CompareTo(y.Value<bool>());
                case JTokenType.Date:
                    return x.Value<DateTime>().CompareTo(y.Value<DateTime>());
                case JTokenType.Bytes:
                    var xBytes = x.Value<byte[]>();
                    var yBytes = y.Value<byte[]>();
                    if (xBytes.Length == 16 && yBytes.Length == 16)
                    {
                        return new Guid(xBytes).CompareTo(new Guid(yBytes));
                    }
                    for (int i = 0; i < xBytes.Length && i < yBytes.Length; i++)
                    {
                        if (xBytes[i] != yBytes[i])
                            return xBytes[i] - yBytes[i];
                    }
                    return xBytes.Length - yBytes.Length;
                case JTokenType.Raw:
                case JTokenType.Comment:
                case JTokenType.Constructor:
                    throw new ArgumentOutOfRangeException();
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public bool Equals(JToken x, JToken y)
        {
            return Compare(x, y) == 0;
        }

        public int GetHashCode(JToken obj)
        {
            switch (obj.Type)
            {
                case JTokenType.None:
                case JTokenType.Undefined:
                case JTokenType.Null:
                    return 0;
                case JTokenType.Object:
                case JTokenType.Bytes:
                case JTokenType.Array:
                    return obj.Aggregate(0, (current, val) => (current * 397) ^ GetHashCode(val));
                case JTokenType.Property:
                    var prop = ((JProperty)obj);
                    return (prop.Name.GetHashCode() * 397) ^ GetHashCode(prop.Value);
                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.String:
                case JTokenType.Boolean:
                case JTokenType.Date:
                    return ((JValue)obj).Value.GetHashCode();
                case JTokenType.Raw:
                case JTokenType.Comment:
                case JTokenType.Constructor:
                    throw new ArgumentOutOfRangeException();
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}