using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Dapper
{
    public static partial class SimpleCRUD
    {
        public class CachingNameResolver : IColumnNameResolver, ITableNameResolver
        {
            private class Mapping
            {
                public string TableName { get; set; }

                public Dictionary<PropertyInfo, string> ColumnName { get; } = new Dictionary<PropertyInfo, string>();
            }

            private readonly ConcurrentDictionary<Type, Mapping> _mappings = new ConcurrentDictionary<Type, Mapping>();
            private readonly ColumnNameResolver _columnNameResolver;
            private readonly TableNameResolver _tableNameResolver;

            public CachingNameResolver(INameFormatter nameFormatter)
            {
                _columnNameResolver = new ColumnNameResolver(nameFormatter);
                _tableNameResolver = new TableNameResolver(nameFormatter);
            }

            private Mapping Get(Type type)
            {
                lock (_mappings)
                {
                    if (_mappings.TryGetValue(type, out var mapping))
                    {
                        return mapping;
                    }

                    mapping = new Mapping
                    {
                        TableName = _tableNameResolver.ResolveTableName(type)
                    };
                    foreach (var p in GetScaffoldableProperties(type, true))
                    {
                        mapping.ColumnName.Add(p, _columnNameResolver.ResolveColumnName(p));
                    }

                    _mappings.AddOrUpdate(type, t => mapping, (t, m2) => mapping);

                    return mapping;
                }
            }

            public string ResolveColumnName(PropertyInfo propertyInfo)
            {
                return Get(propertyInfo.DeclaringType).ColumnName[propertyInfo];
            }

            public virtual string ResolveTableName(Type type)
            {
                return Get(type).TableName;
            }
        }
    }
}
