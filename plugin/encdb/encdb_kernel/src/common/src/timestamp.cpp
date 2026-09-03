#include "timestamp.h"
#include <stdexcept>
#include "encdb_common_exception.h"

// format 'YYYY-MM-DD hh:mm:ss'
Timestamp::Timestamp(const std::string &t, bool check_value) {
  // format check
  if (t.length() != 19 || t[4] != '-' || t[7] != '-' || t[10] != ' ' ||
      t[13] != ':' || t[16] != ':') {
    throw encdb::InvalidTimestampException("Invalid timestamp " + t);
  }
  std::string str_year, str_mon, str_day, str_hour, str_min, str_sec;
  str_year = t.substr(0, 4);
  str_mon = t.substr(5, 2);
  str_day = t.substr(8, 2);
  str_hour = t.substr(11, 2);
  str_min = t.substr(14, 2);
  str_sec = t.substr(17, 2);

  try {
    this->year = std::stoi(str_year);
    this->month = std::stoi(str_mon);
    this->day = std::stoi(str_day);
    this->hour = std::stoi(str_hour);
    this->minute = std::stoi(str_min);
    this->second = std::stoi(str_sec);
  } catch (const std::invalid_argument &e) {
    throw encdb::InvalidTimestampException("Invalid timestamp " + t);
  }
  // Check if time is in range 1000-01-01 00:00:00 and 9999-12-31 23:59:59
  if (check_value && (*this > MYSQL_MAX_TIME || MYSQL_MIN_TIME > *this)) {
    throw encdb::InvalidTimestampException("Invalid timestamp " + t);
  }
}

bool Timestamp::operator>(const Timestamp &t) const {
  return this->year > t.year ||
         (this->year == t.year && this->month > t.month) ||
         (this->year == t.year && this->month == t.month &&
          this->day > t.day) ||
         (this->year == t.year && this->month == t.month &&
          this->day == t.day && this->hour > t.hour) ||
         (this->year == t.year && this->month == t.month &&
          this->day == t.day && this->hour == t.hour &&
          this->minute > t.minute) ||
         (this->year == t.year && this->month == t.month &&
          this->day == t.day && this->hour == t.hour &&
          this->minute == t.minute && this->second > t.second);
}

bool Timestamp::operator!=(const Timestamp &t) const {
  return this->year != t.year || this->day != t.day || this->month != t.month ||
         this->hour != t.hour || this->minute != t.minute ||
         this->second != t.second;
}

bool Timestamp::operator==(const Timestamp &t) const { return !(*this != t); }

std::string Timestamp::to_string() const {
  std::string ret;
  ret += std::to_string(this->year);
  ret += "-";
  if (this->month < 10) {
    ret += "0";
    ret += std::to_string(this->month);
  } else {
    ret += std::to_string(this->month);
  }
  ret += "-";
  if (this->day < 10) {
    ret += "0";
    ret += std::to_string(this->day);
  } else {
    ret += std::to_string(this->day);
  }
  ret += " ";
  if (this->hour < 10) {
    ret += "0";
    ret += std::to_string(this->hour);
  } else {
    ret += std::to_string(this->hour);
  }
  ret += ":";
  if (this->minute < 10) {
    ret += "0";
    ret += std::to_string(this->minute);
  } else {
    ret += std::to_string(this->minute);
  }
  ret += ":";
  if (this->second < 10) {
    ret += "0";
    ret += std::to_string(this->second);
  } else {
    ret += std::to_string(this->second);
  }
  return ret;
}
