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
    }
}
