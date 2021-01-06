using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
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

        public class Post2
        {
            public int Id { get; set; }
            public string Text { get; set; }
            [Column("author_id")]
            public int? AuthorId => Author?.Id;
            public User2 Author { get; set; }
        }

        public void TestLowerCaseNames()
        {
            var mapper = new SimpleCRUD.CachingNameResolver(new SimpleCRUD.LowercaseFormatter());
            SimpleCRUD.SetColumnNameResolver(mapper);
            SimpleCRUD.SetTableNameResolver(mapper);

            int userId;
            using (var c = GetOpenConnection())
            {
                var user = new User2 {FirstName = "Jane", LastName = "Doe", ScheduledDayOff = DayOfWeek.Monday};
                c.Insert(user);
                userId = user.Id;
                var post = new Post2 {Text = "My first post", Author = user};
                c.Insert(post);
            }

            using (var c2 = GetOpenConnection())
            {
                var user = c2.Get<User2>(userId);
                Assert.IsNotNull(user);
                user.FirstName.IsEqualTo("Jane");
                user.LastName.IsEqualTo("Doe");
                Assert.IsEqualTo((DayOfWeek)user.ScheduledDayOff, DayOfWeek.Monday);

                var posts = c2.MultiQuery<Post2, User2, Post2>("select p.* ||| u.* " +
                                                              "from post2 p, user2 u " +
                                                              "where p.author_id = u.id", (p, u) =>
                {
                    p.Author = u;
                    return p;
                });
                Assert.IsTrue(posts.Any());
            }

        }
    }
}
