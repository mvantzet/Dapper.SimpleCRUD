using System;
using System.Collections.Generic;
using System.Text;

namespace Dapper.SimpleCRUDTests
{
    public class PgTests : Tests
    {
        public PgTests() : base(SimpleCRUD.Dialect.PostgreSQL)
        {
        }

        /// <summary>
        /// Test lowercase formatter
        /// </summary>
        public class User2
        {
            public int Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public EnumString<DayOfWeek> ScheduledDayOff { get; set; }
        }

        public void TestLowerCaseNames()
        {
            var mapper = new SimpleCRUD.CachingNameResolver(new SimpleCRUD.LowercaseFormatter());
            SimpleCRUD.SetColumnNameResolver(mapper);
            SimpleCRUD.SetTableNameResolver(mapper);

            int userId = 0;
            using (var c = GetOpenConnection())
            {
                userId = (int)c.Insert(new User2 {FirstName = "Jane", LastName = "Doe", ScheduledDayOff = DayOfWeek.Monday});
            }

            using (var c2 = GetOpenConnection())
            {
                var user = c2.Get<User2>(userId);
                Assert.IsNotNull(user);
                user.FirstName.IsEqualTo("Jane");
                user.LastName.IsEqualTo("Doe");
                Assert.IsEqualTo((DayOfWeek)user.ScheduledDayOff, DayOfWeek.Monday);
            }

        }
    }
}
