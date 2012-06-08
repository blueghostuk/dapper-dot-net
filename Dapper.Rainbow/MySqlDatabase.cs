using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;

namespace Dapper.Rainbow
{
    public abstract class MySqlDatabase<TDatabase> : Database<TDatabase> where TDatabase : Database<TDatabase>, new()
    {
        public static readonly TransactionOptions DefaultTransactionOptions = new TransactionOptions()
        {
            IsolationLevel = IsolationLevel.RepeatableRead
        };

        public class MySqlTable<T> : Database<TDatabase>.Table<T>
        {
            private readonly string _ignore = " IGNORE";

            public MySqlTable(Database<TDatabase> database, string likelyTableName)
                : base(database, likelyTableName, "`", "`") { }

            public override long? Insert(dynamic data)
            {
                return Insert(data, false);
            }

            public void SetKeyFieldName(string keyFieldName)
            {
                KeyFieldName = keyFieldName;
            }

            public virtual long? Insert(dynamic data, bool ignoreIfExists)
            {
                var o = (object)data;
                List<string> paramNames = GetParamNames(o);
                string ignore = ignoreIfExists ? _ignore : string.Empty;

                string cols = string.Join(",", paramNames);
                string cols_params = string.Join(",", paramNames.Select(p => "@" + p));
                var sql = "INSERT" + ignore + " INTO " + TableName + " (" + cols + ") values (" + cols_params + "); SELECT LAST_INSERT_ID();";

                return database.Query<long?>(sql, o).Single();
            }

            public override T First()
            {
                return database.Query<T>("SELECT * FROM " + NamePrefix + TableName + NameSuffix + " LIMIT 1").FirstOrDefault();
            }
        }

        internal override Action<TDatabase> CreateTableConstructorForTable()
        {
            return CreateTableConstructor(typeof(MySqlTable<>));
        }
    }
}
