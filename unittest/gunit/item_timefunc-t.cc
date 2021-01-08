/* Copyright (c) 2011, 2021, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <gtest/gtest.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <limits>
#include <sstream>
#include <string>

#include "decimal.h"
#include "m_ctype.h"
#include "m_string.h"
#include "my_time.h"
#include "mysql_time.h"
#include "sql/item.h"
#include "sql/item_timefunc.h"
#include "sql/my_decimal.h"
#include "sql/parse_location.h"
#include "sql/parse_tree_node_base.h"
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "sql/sql_lex.h"
#include "sql/system_variables.h"
#include "sql_string.h"
#include "unittest/gunit/test_utils.h"

namespace item_timefunc_unittest {

using my_testing::Server_initializer;

class ItemTimeFuncTest : public ::testing::Test {
 protected:
  void SetUp() override { initializer.SetUp(); }

  void TearDown() override { initializer.TearDown(); }

  THD *thd() { return initializer.thd(); }

  Server_initializer initializer;
};

TEST_F(ItemTimeFuncTest, dateAddInterval) {
  Item_int *arg0 = new Item_int(20130122145221LL);  // 2013-01-22 14:52:21
  Item_decimal *arg1 = new Item_decimal(0.1234567);
  Item *item = new Item_date_add_interval(POS(), arg0, arg1,
                                          INTERVAL_SECOND_MICROSECOND, false);
  Parse_context pc(thd(), thd()->lex->current_query_block());
  EXPECT_FALSE(item->itemize(&pc, &item));
  EXPECT_FALSE(item->fix_fields(thd(), nullptr));

  // The below result is not correct, see Bug#16198372
  EXPECT_DOUBLE_EQ(20130122145222.234567, item->val_real());
}

// Checks that the metadata and result are consistent for a time function that
// returns an integer.
static void CheckMetadataConsistency(THD *thd, Item *item) {
  SCOPED_TRACE(ItemToString(item));

  Item *ref[] = {item};
  ASSERT_FALSE(item->fix_fields(thd, ref));
  ASSERT_EQ(item, ref[0]);

  // Expect a signed integer return type.
  EXPECT_FALSE(item->unsigned_flag);
  EXPECT_EQ(0, item->decimals);

  const int64_t int_result = item->val_int();
  if (item->null_value) {
    EXPECT_TRUE(item->is_nullable());
    return;
  }

  // int result and string result should match.
  const std::string expected_string_result = std::to_string(int_result);
  StringBuffer<STRING_BUFFER_USUAL_SIZE> buffer;
  EXPECT_EQ(expected_string_result, to_string(*item->val_str(&buffer)));

  // Check that the metadata matches what was actually returned.
  const size_t result_char_length = expected_string_result.size();
  const size_t digits = result_char_length - (int_result < 0 ? 1 : 0);
  EXPECT_LE(result_char_length, item->max_char_length());
  EXPECT_LE(digits, item->decimal_precision());
  EXPECT_LE(digits, item->decimal_int_part());
}

// Checks that the metadata and result are consistent for a time function that
// returns an integer. Additionally, check that the expected result is returned.
static void CheckMetadataAndResult(THD *thd, Item *item,
                                   int64_t expected_result) {
  CheckMetadataConsistency(thd, item);
  EXPECT_EQ(expected_result, item->val_int());
  EXPECT_FALSE(item->null_value);
}

// Verifies that the results returned by the PERIOD_ADD function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, PeriodAddMetadata) {
  // PERIOD_ADD returns values on the form YYYYMM, but it's not limited to
  // four-digit year.
  CheckMetadataAndResult(
      thd(),
      new Item_func_period_add(POS(), new Item_int(999912), new Item_int(1)),
      1000001);

  // Maximum return value.
  CheckMetadataAndResult(
      thd(),
      new Item_func_period_add(POS(), new Item_int(9223372036854775806LL),
                               new Item_int(1)),
      std::numeric_limits<int64_t>::max());

  // Overflow makes the result wrap around.
  CheckMetadataAndResult(
      thd(),
      new Item_func_period_add(POS(), new Item_int(9223372036854775806LL),
                               new Item_int(2)),
      std::numeric_limits<int64_t>::min());
}

// Verifies that the results returned by the PERIOD_DIFF function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, PeriodDiffMetadata) {
  CheckMetadataAndResult(thd(),
                         new Item_func_period_diff(POS(), new Item_int(202101),
                                                   new Item_int(202101)),
                         0);

  CheckMetadataAndResult(thd(),
                         new Item_func_period_diff(POS(), new Item_int(202101),
                                                   new Item_int(201912)),
                         13);

  CheckMetadataAndResult(thd(),
                         new Item_func_period_diff(POS(), new Item_int(201912),
                                                   new Item_int(202101)),
                         -13);

  CheckMetadataAndResult(
      thd(),
      new Item_func_period_diff(POS(), new Item_int(9223372036854775807LL),
                                new Item_int(1)),
      1106804644422549102LL);

  CheckMetadataAndResult(
      thd(),
      new Item_func_period_diff(POS(), new Item_int(1),
                                new Item_int(9223372036854775807LL)),
      -1106804644422549102LL);
}

// Verifies that the results returned by the TO_DAYS function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, ToDaysMetadata) {
  auto arg = new Item_string(STRING_WITH_LEN("9999-12-31"),
                             &my_charset_utf8mb4_0900_ai_ci);
  auto to_days = new Item_func_to_days(POS(), arg);
  CheckMetadataAndResult(thd(), to_days, 3652424);
}

// Verifies that the results returned by the TO_SECONDS function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, ToSecondsMetadata) {
  auto arg = new Item_string(STRING_WITH_LEN("9999-12-31 23:59:59"),
                             &my_charset_utf8mb4_0900_ai_ci);
  auto to_seconds = new Item_func_to_seconds(POS(), arg);
  CheckMetadataAndResult(thd(), to_seconds, 315569519999);
}

// Verifies that the results returned by the DAYOFMONTH function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, DayOfMonthMetadata) {
  for (int i = 20210100; i <= 20210131; ++i) {
    CheckMetadataAndResult(
        thd(), new Item_func_dayofmonth(POS(), new Item_int(i)), i % 100);
  }
}

// Verifies that the results returned by the DAYOFYEAR function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, DayOfYearMetadata) {
  CheckMetadataAndResult(
      thd(), new Item_func_dayofyear(POS(), new Item_int(20200101)), 1);

  CheckMetadataAndResult(
      thd(), new Item_func_dayofyear(POS(), new Item_int(20201231)), 366);
}

// Verifies that the results returned by the HOUR function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, HourMetadata) {
  static_assert(TIME_MAX_HOUR == 838,
                "TIME_MAX_HOUR has changed. Update the test case to test the "
                "new maximum value.");
  CheckMetadataAndResult(thd(),
                         new Item_func_hour(POS(), new Item_int(8380000)), 838);

  CheckMetadataAndResult(
      thd(), new Item_func_hour(POS(), new Item_int(-8380000)), 838);
}

// Verifies that the results returned by the MINUTE function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, MinuteMetadata) {
  for (int i = 0; i < 60; ++i) {
    CheckMetadataAndResult(
        thd(), new Item_func_minute(POS(), new Item_int(i * 100)), i);
  }
}

// Verifies that the results returned by the QUARTER function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, QuarterMetadata) {
  for (int month = 1; month <= 12; ++month) {
    CheckMetadataAndResult(
        thd(),
        new Item_func_quarter(POS(), new Item_int(20000001 + month * 100)),
        (month + 2) / 3);
  }
}

// Verifies that the results returned by the SECOND function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, SecondMetadata) {
  for (int i = 0; i < 60; ++i) {
    CheckMetadataAndResult(thd(), new Item_func_second(POS(), new Item_int(i)),
                           i);
  }
}

// Verifies that the results returned by the WEEK function are consistent
// with the metadata.
TEST_F(ItemTimeFuncTest, WeekMetadata) {
  auto mode = new Item_int(int{WEEK_MONDAY_FIRST | WEEK_YEAR});

  CheckMetadataAndResult(
      thd(), new Item_func_week(POS(), new Item_int(20200101), mode), 1);

  CheckMetadataAndResult(
      thd(), new Item_func_week(POS(), new Item_int(20210101), mode), 53);
}

struct test_data {
  const char *secs;
  unsigned int hour;
  unsigned int minute;
  unsigned int second;
  unsigned long second_part;
};

::std::ostream &operator<<(::std::ostream &os, const struct test_data &data) {
  return os << data.secs;
}

class ItemTimeFuncTestP : public ::testing::TestWithParam<test_data> {
 protected:
  void SetUp() override {
    initializer.SetUp();
    m_t = GetParam();
  }

  void TearDown() override { initializer.TearDown(); }

  THD *thd() { return initializer.thd(); }

  Server_initializer initializer;
  test_data m_t;
};

const test_data test_values[] = {{"0.1234564", 0, 0, 0, 123456},
                                 {"0.1234567", 0, 0, 0, 123457},
                                 {"0.1234", 0, 0, 0, 123400},
                                 {"12.1234567", 0, 0, 12, 123457},
                                 {"123", 0, 2, 3, 0},
                                 {"2378.3422349", 0, 39, 38, 342235},
                                 {"3020398.999999999", 838, 59, 59, 0},
                                 {"3020399", 838, 59, 59, 0},
                                 {"99999999.99999999", 838, 59, 59, 0}};

INSTANTIATE_TEST_SUITE_P(a, ItemTimeFuncTestP,
                         ::testing::ValuesIn(test_values));

/**
  Test member function of @c Item_time_func

  @param item     item of a sub-class of @c Item_time_func
  @param ltime    time structure that contains the expected result
  @param decimals number of significant decimals in the expected result
*/
void testItemTimeFunctions(Item_time_func *item, MYSQL_TIME *ltime,
                           int decimals) {
  long long int mysql_time =
      10000 * ltime->hour + 100 * ltime->minute + ltime->second;
  EXPECT_EQ(mysql_time, item->val_int());

  long long int packed = TIME_to_longlong_packed(*ltime);
  EXPECT_EQ(packed, item->val_time_temporal());

  double d = mysql_time + ltime->second_part / 1000000.0;
  EXPECT_DOUBLE_EQ(d, item->val_real());

  my_decimal decval1, decval2;
  my_decimal *dec = item->val_decimal(&decval1);
  double2decimal(d, &decval2);
  EXPECT_EQ(0, my_decimal_cmp(dec, &decval2));

  char s[20];
  sprintf(s, "%02d:%02d:%02d", ltime->hour, ltime->minute, ltime->second);
  if (ltime->second_part > 0) {  // Avoid trailing zeroes
    int decs = ltime->second_part;
    while (decs % 10 == 0) decs /= 10;
    sprintf(s + strlen(s), ".%d", decs);
  } else if (decimals > 0)
    // There were decimals, but they have disappeared due to overflow
    sprintf(s + strlen(s), ".000000");
  String timeStr(20);
  EXPECT_STREQ(s, item->val_str(&timeStr)->c_ptr());

  MYSQL_TIME ldate;
  //> Second argument of Item_func_time::get_date is not used for anything
  item->get_date(&ldate, 0);
  // Todo: Should check that year, month, and day is relative to current date
  EXPECT_EQ(ltime->hour % 24, ldate.hour);
  EXPECT_EQ(ltime->minute, ldate.minute);
  EXPECT_EQ(ltime->second, ldate.second);
  EXPECT_EQ(ltime->second_part, ldate.second_part);

  // Todo: Item_time_func::save_in_field is not tested
}

TEST_P(ItemTimeFuncTestP, secToTime) {
  Item_decimal *sec = new Item_decimal(POS(), m_t.secs, strlen(m_t.secs),
                                       &my_charset_latin1_bin);
  Item_func_sec_to_time *time = new Item_func_sec_to_time(POS(), sec);

  Parse_context pc(thd(), thd()->lex->current_query_block());
  Item *item;
  EXPECT_FALSE(time->itemize(&pc, &item));
  EXPECT_EQ(time, item);
  EXPECT_FALSE(time->fix_fields(thd(), nullptr));

  MYSQL_TIME ltime;
  time->get_time(&ltime);
  EXPECT_EQ(0U, ltime.year);
  EXPECT_EQ(0U, ltime.month);
  EXPECT_EQ(0U, ltime.day);
  EXPECT_EQ(m_t.hour, ltime.hour);
  EXPECT_EQ(m_t.minute, ltime.minute);
  EXPECT_EQ(m_t.second, ltime.second);
  EXPECT_EQ(m_t.second_part, ltime.second_part);

  testItemTimeFunctions(time, &ltime, sec->decimals);
}

// Test for MODE_TIME_TRUNCATE_FRACTIONAL.
class ItemTimeFuncTruncFracTestP : public ::testing::TestWithParam<test_data> {
 protected:
  void SetUp() override {
    initializer.SetUp();
    m_t = GetParam();
    save_mode = thd()->variables.sql_mode;
    thd()->variables.sql_mode |= MODE_TIME_TRUNCATE_FRACTIONAL;
  }

  void TearDown() override {
    thd()->variables.sql_mode = save_mode;
    initializer.TearDown();
  }

  THD *thd() { return initializer.thd(); }

  Server_initializer initializer;
  test_data m_t;
  sql_mode_t save_mode;
};

const test_data test_values_trunc_frac[] = {
    {"0.1234564", 0, 0, 0, 123456},
    {"0.1234567", 0, 0, 0, 123456},
    {"0.1234", 0, 0, 0, 123400},
    {"12.1234567", 0, 0, 12, 123456},
    {"123", 0, 2, 3, 0},
    {"2378.3422349", 0, 39, 38, 342234},
    {"3020398.999999999", 838, 59, 58, 999999},
    {"3020399", 838, 59, 59, 0},
    {"99999999.99999999", 838, 59, 59, 0}};

INSTANTIATE_TEST_SUITE_P(a, ItemTimeFuncTruncFracTestP,
                         ::testing::ValuesIn(test_values_trunc_frac));

TEST_P(ItemTimeFuncTruncFracTestP, secToTime) {
  Item_decimal *sec = new Item_decimal(POS(), m_t.secs, strlen(m_t.secs),
                                       &my_charset_latin1_bin);
  Item_func_sec_to_time *time = new Item_func_sec_to_time(POS(), sec);

  Parse_context pc(thd(), thd()->lex->current_query_block());
  Item *item;
  EXPECT_FALSE(time->itemize(&pc, &item));
  EXPECT_EQ(time, item);
  EXPECT_FALSE(time->fix_fields(thd(), nullptr));

  MYSQL_TIME ltime;
  time->get_time(&ltime);
  EXPECT_EQ(0U, ltime.year);
  EXPECT_EQ(0U, ltime.month);
  EXPECT_EQ(0U, ltime.day);
  EXPECT_EQ(m_t.hour, ltime.hour);
  EXPECT_EQ(m_t.minute, ltime.minute);
  EXPECT_EQ(m_t.second, ltime.second);
  EXPECT_EQ(m_t.second_part, ltime.second_part);

  testItemTimeFunctions(time, &ltime, sec->decimals);
}
}  // namespace item_timefunc_unittest
