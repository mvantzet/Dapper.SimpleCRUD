using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.SimpleCRUDTests
{
    internal class NonDbTests
    {
        public void LowercaseUnderscoreFormatterTest()
        {
            var tests = new[] {"", "", "a", "a", "A", "a", "AB", "a_b", "A_Test", "a_test", "A__Test", "a_test", "a_test", "a_test", "a__test", "a_test", "LowercaseUnderscoreFormatter", "lowercase_underscore_formatter"};
            var f = new SimpleCRUD.LowercaseUnderscoreFormatter();
            for (var i = 0; i < tests.Length; i += 2)
            {
                tests[i + 1].IsEqualTo(f.Format(tests[i]));
            }
        }

        public void LowercaseFormatterTest()
        {
            var tests = new[] {"", "", "a", "a", "A", "a", "AB", "ab", "A_Test", "a_test", "A__Test", "a__test", "a_test", "a_test", "a__test", "a__test", "LowercaseFormatter", "lowercaseformatter"};
            var f = new SimpleCRUD.LowercaseFormatter();
            for (var i = 0; i < tests.Length; i += 2)
            {
                var actual = f.Format(tests[i]);
                var expected = tests[i + 1];
                actual.IsEqualTo(expected);
            }
        }

        public void GetScaffoldablePropertiesTest()
        {
            var u = new User();
            var expected = new[] {"ScheduledDayOff", "CreatedDate", "Id", "Name", "Age"};
            var props = SimpleCRUD.GetScaffoldableProperties(u.GetType());
            expected.Length.IsEqualTo(props.Length);
            expected.All(e => props.Any(p => p.Name == e)).IsTrue();
        }

        public void TemplateEngineTest_ExistingProp()
        {
            var sql = "a @test?{something} b";
            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(sql, new {test = false});
            Assert.IsEqualTo(sql2, "a  b");

            var sql3 = SimpleCRUD.TemplateEngine.Evaluate(sql, new {test = true});
            Assert.IsEqualTo(sql3, "a something b");
        }

        public void TemplateEngineTest_NonExistentProp()
        {
            var sql = "a @test?{something} b";
            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(sql, null);
            Assert.IsEqualTo(sql2, "a  b");
        }

        public void TemplateEngineTest_StringProp()
        {
            var sql = "@test?{x = @test}";
            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(sql, new {test = "OK"});
            Assert.IsEqualTo(sql2, "x = @test");

            var sql3 = SimpleCRUD.TemplateEngine.Evaluate(sql, new {test = ""});
            Assert.IsEqualTo(sql3, "x = @test");

            var sql4 = SimpleCRUD.TemplateEngine.Evaluate(sql, new {test = (string)null});
            Assert.IsEqualTo(sql4, "");
        }

        public void TemplateEngineTest_EscapeChar()
        {
            var sql = @"@test?{char > '\}'}";
            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(sql, new {test = true});
            Assert.IsEqualTo(sql2, @"char > '}'");
        }

        public void TemplateEngineTest_EmptyString()
        {
            var sql = @"";
            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(sql, null);
            Assert.IsEqualTo(sql2, @"");
        }

        public void TemplateEngineTest_LiteralInserts()
        {
            var sql = @"select * from user @OrderBy?{order by @@OrderBy}";

            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(sql, null);
            Assert.IsEqualTo(sql2, @"select * from user ");

            var sql3 = SimpleCRUD.TemplateEngine.Evaluate(sql, new
            {
                OrderBy = "id desc"
            });
            Assert.IsEqualTo(sql3, @"select * from user order by id desc");

            var sql4 = SimpleCRUD.TemplateEngine.Evaluate(@"select * from user order by @@OrderBy",
                new {OrderBy = "id desc"});
            Assert.IsEqualTo(sql4, @"select * from user order by id desc");
        }

        public void TemplateEngineTest_WhereAndOrderBy() {
            var sql = SimpleCRUD.TemplateEngine.Evaluate(@"select * from user where id > 10 @Where?{and @@Where} @OrderBy?{order by @@OrderBy}",
                new
                {
                    Where = "firstname like @FirstName || '%'", 
                    FirstName = "A",
                    OrderBy = "lastname, firstname"
                });
            Assert.IsEqualTo(sql, @"select * from user where id > 10 and firstname like @FirstName || '%' order by lastname, firstname");
        }

        public void TemplateEngineTest_TestElse() {
            var sql = SimpleCRUD.TemplateEngine.Evaluate(@"select * from user order by @OrderBy?{@@OrderBy}:{id}",
                new
                {
                    OrderBy = "lastname, firstname"
                });
            Assert.IsEqualTo(sql, @"select * from user order by lastname, firstname");

            var sql2 = SimpleCRUD.TemplateEngine.Evaluate(@"select * from user order by @OrderBy?{@@OrderBy}:{id}", null);
            Assert.IsEqualTo(sql2, @"select * from user order by id");
        }

        public void TemplateEngineTest_UnclosedBrace_ThrowsException()
        {
            Assert.Throws<FormatException>(() =>
                SimpleCRUD.TemplateEngine.Evaluate(@"select * from user order by @OrderBy?{@@OrderBy", null));
        }
    }
}
