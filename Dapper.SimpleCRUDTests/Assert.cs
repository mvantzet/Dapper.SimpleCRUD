using System;
using System.Collections.Generic;
using System.Linq;

namespace Dapper.SimpleCRUDTests
{
    /// <summary>
    /// Assert extensions borrowed from Sam's code in DapperTests
    /// </summary>
    static class Assert
    {
        public static void IsEqualTo<T>(this T obj, T other)
        {
            if (!obj.Equals(other))
            {
                throw new ApplicationException(string.Format("{0} should be equal to {1}", obj, other));
            }
        }

        public static void IsSequenceEqualTo<T>(this IEnumerable<T> obj, IEnumerable<T> other)
        {
            if (!obj.SequenceEqual(other))
            {
                throw new ApplicationException(string.Format("{0} should be equal to {1}", obj, other));
            }
        }

        public static void IsFalse(this bool b)
        {
            if (b)
            {
                throw new ApplicationException("Expected false");
            }
        }

        public static void IsTrue(this bool b)
        {
            if (!b)
            {
                throw new ApplicationException("Expected true");
            }
        }

        public static void IsNull(this object obj)
        {
            if (obj != null)
            {
                throw new ApplicationException("Expected null");
            }
        }

        public static void Throws<T>(Action a) where T : Exception
        {
            try
            {
                a();
                throw new ApplicationException($"Expected exception type '{typeof(T).Name}'");
            }
            catch (Exception e)
            {
                if (e is T)
                {
                    return;
                }
                throw new ApplicationException($"Expected exception type '{typeof(T).Name}' but was '{e.GetType().Name}'");
            }
        }
    }
}