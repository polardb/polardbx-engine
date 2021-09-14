show variables like 'join_buffer_size';

let $query=
SELECT city.Name, country.Name FROM city,country
  WHERE city.Country=country.Code AND city.Population > 3000000;

--replace_column 10 # 11 #
--eval EXPLAIN $query
--sorted_result
--eval $query

let $query=
SELECT city.Name, country.Name FROM city,country
  WHERE city.Country=country.Code AND 
        country.Name LIKE 'L%' AND city.Population > 100000;

--replace_column 10 # 11 #
--eval EXPLAIN $query
--sorted_result
--eval $query

let $query=
SELECT city.Name, country.Name, countrylanguage.Language
  FROM city,country,countrylanguage
  WHERE city.Country=country.Code AND
        countrylanguage.Country=country.Code AND
        city.Name LIKE 'L%' AND country.Population > 3000000 AND
        countrylanguage.Percentage > 50;

--replace_column 10 # 11 #
--eval EXPLAIN $query
--sorted_result
--eval $query

let $query=
SELECT Name FROM city
  WHERE city.country IN (SELECT Code FROM country WHERE country.Name LIKE 'L%') AND
        city.Population > 100000;

--replace_column 10 # 11 #
--eval EXPLAIN $query
--sorted_result
--eval $query


set join_buffer_size=default;
