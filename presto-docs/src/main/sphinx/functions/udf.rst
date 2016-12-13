======================
User Defined Functions
======================

.. function:: distance(double, double, double, double) -> double

    Returns the distance(in kilometers) between two points, assuming earth is spherical.

.. function:: distance(double, double, double, double, radiusOfCurvature) -> double

    Returns the distance(in kilometers) between two points, assuming earth is spherical.

.. function:: vincenty_distance(double, double, double, double) -> double

    Returns the distance (in kilometers) between two points using the Vincenty formula.

.. function:: within_circle(double, double, double, double, double) -> boolean

    Determines whether a point (lat,lon) is within a circle of radius d kilometers centered at a given point (lat0,lon0).

.. function:: initcap(varchar) -> varchar

    Capitalizes first letter of each alphanumeric word and puts the rest in lowercase.

.. function:: regexp_substr(varchar, varchar) -> varchar

    Returns the substring that matches a regular expression within a string. If no matches are found, this function returns NULL.

.. function:: regexp_substr(varchar, varchar, bigint) -> varchar

    Returns the substring that matches a regular expression within a string. If no matches are found, this function returns NULL.

.. function:: regexp_substr(varchar, varchar, bigint, bigint) -> varchar

    Returns the substring that matches a regular expression within a string. If no matches are found, this function returns NULL.

.. function:: regexp_substr(varchar, varchar, bigint, bigint, varchar) -> varchar

    Returns the substring that matches a regular expression within a string. If no matches are found, this function returns NULL.

.. function:: regexp_substr(varchar, varchar, bigint, bigint, varchar, bigint) -> varchar

    Returns the substring that matches a regular expression within a string. If no matches are found, this function returns NULL.

.. function:: regexp_count(varchar, varchar, bigint, varchar) -> bigint

    Returns the number times a regular expression matches a string.

.. function:: regexp_count(varchar, varchar, bigint) -> bigint

    Returns the number times a regular expression matches a string.

.. function:: regexp_count(varchar, varchar) -> bigint

    Returns the number times a regular expression matches a string.

.. function:: indexOf(varchar, varchar) -> bigint

    Returns index of first occurrence of a substring (or 0 if not found) in string.

.. function:: days(timestamp) -> bigint

    Returns number of seconds that have elapsed since the epoch.

.. function:: timestamp_round(timestamp, varchar) -> timestamp

    Rounds a TIMESTAMP to a specified format.

.. function:: to_timestamp_tz(timestamp, varchar) -> varchar

    Converts a TIMESTAMP value between time zones.

.. function:: uber_asgeojson(double, double) -> varchar

    Returns the JSON representation of a lng/lat pair.

.. function:: translate(varchar, varchar, varchar) -> varchar

    Replaces individual characters in string with other characters.


