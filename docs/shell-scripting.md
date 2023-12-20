Lollypop offers developers the opportunity to use a Scala/SQL-hybrid scripting language for writing
shell-scripts. And because your scripts will be running within the JVM you can leverage Maven Central
and the myriads of libraries available to it.

#### A new approach to shell-scripting

Lollypop provides native variants of the following UNIX-like commands:
* <a href="#cat">cat</a> - Retrieves the contents of a file.
* <a href="#cd">cd</a> - Changes the current directory.
* <a href="#cp">cp</a> - Copies a source file or directory to a target.
* <a href="#echo">echo</a> - Prints text to the standard output.
* <a href="#find">find</a> - Returns a dataframe containing a recursive list of files matching any specified criterion.
* <a href="#ls">ls</a> - Returns a dataframe containing a list of files matching any specified criterion.
* <a href="#md5">md5</a> - Returns an MD5 digest of a file or byte-encode-able value.
* <a href="#mkdir">mkdir</a> - Creates a new directory.
* <a href="#mv">mv</a> - Renames a file or moves the file to a directory.
* <a href="#pwd">pwd</a> - Retrieves the contents of a file.
* <a href="#rm">rm</a> - Removes a file or a collection of files via pattern-matching.
* <a href="#rmdir">rmdir</a> - Removes a specific directory.
* <a href="#rmr">rmr</a> - Recursively removes files or collections of files via pattern-matching.
* <a href="#touch">touch</a> - Creates or updates the last modified time of a file. Return true if successful.
* <a href="#wc">wc</a> - Returns the count of lines of a text file.
* <a href="#www">www</a> - A non-interactive HTTP client

#### Let's try something simple

* What if I ask you to write some Bash code to retrieve the top 5 largest files by size in descending order?

Could you write it without consulting a search engine or manual pages? Off the cuff, here's what I came up with to do it. It's crude, and it only works on the Mac...

```bash
ls -lS ./app/examples/ | grep -v ^total | head -n 5
```
##### produced the following:
```text
-rw-r--r--@ 1 ldaniels  staff  4990190 Nov 11 23:50 stocks-100k.csv
-rw-r--r--@ 1 ldaniels  staff   336324 Nov 11 23:50 stocks.csv
-rw-r--r--@ 1 ldaniels  staff   249566 Nov 11 23:50 stocks-5k.csv
drwxr-xr-x@ 6 ldaniels  staff      192 Jul  5 14:57 target
drwxr-xr-x@ 5 ldaniels  staff      160 Jul  5 14:52 src
```

And here's the equivalent in Lollypop:

```sql
ls app/examples where not isHidden order by length desc limit 5
```
##### produced the following:
```sql
|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| name            | canonicalPath                                                | lastModified             | length  | isDirectory | isFile | isHidden |
|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| stocks-100k.csv | /Users/ldaniels/GitHub/lollypop/app/examples/stocks-100k.csv | 2023-11-12T07:50:27.490Z | 4990190 | false       | true   | false    |
| stocks-5k.csv   | /Users/ldaniels/GitHub/lollypop/app/examples/stocks-5k.csv   | 2023-11-12T07:50:27.491Z |  249566 | false       | true   | false    |
| .DS_Store       | /Users/ldaniels/GitHub/lollypop/app/examples/.DS_Store       | 2023-11-12T05:53:49.722Z |    6148 | false       | true   | true     |
| target          | /Users/ldaniels/GitHub/lollypop/app/examples/target          | 2023-07-05T21:57:46.435Z |     192 | true        | false  | false    |
| companylist     | /Users/ldaniels/GitHub/lollypop/app/examples/companylist     | 2023-11-12T07:50:27.476Z |     128 | true        | false  | false    |
|-------------------------------------------------------------------------------------------------------------------------------------------------------|
```

* What if I ask for the same as above except find files recursively?

```sql
find './app/examples/' where not isHidden order by length desc limit 5
```
##### produced the following:
```sql
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name            | canonicalPath                                                                                             | lastModified             | length  | isDirectory | isFile | isHidden |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| stocks-100k.csv | /Users/ldaniels/GitHub/lollypop/app/examples/stocks-100k.csv                                              | 2023-11-12T07:50:27.490Z | 4990190 | false       | true   | false    |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
```

#### Let's try something practical

* What if you want a list of the top 5 CPU hungry processes?

```sql
def ps() := TableArray((? ps aux ?).map(x =>
  x.trim()
   .replaceAll("  ", " ")
   .split("[ ]")
   .filter(x => not x.isEmpty())
))
select PID, `%CPU`, `%MEM`, STARTED, TIME, VSZ, RSS
from (ps())
order by `%CPU` desc
limit 5
```
##### produced the following:
```sql
|--------------------------------------------------------------------|
| PID   | %CPU | %MEM | STARTED | TIME       | VSZ        | RSS      |
|--------------------------------------------------------------------|
|  1542 | 83.6 |  0.2 | 13Dec23 | 1657:48.74 |   36408744 |   231972 |
| 25369 | 19.1 | 10.7 | Thu09AM | 728:56.39  |  438577760 | 10803088 |
| 21272 | 12.4 |  3.2 | 9:39AM  | 0:25.86    |  437902704 |  3271536 |
| 28637 |  8.2 |  0.6 | Wed04PM | 414:47.08  | 1594687744 |   612032 |
|   360 |  8.1 |  0.8 | 13Dec23 | 669:52.88  |  416009088 |   790640 |
|--------------------------------------------------------------------|
```

#### Let's try something cool

The system command `iostat 1 5` yielded the following text output:
```text
              disk0               disk4               disk5       cpu    load average
    KB/t  tps  MB/s     KB/t  tps  MB/s     KB/t  tps  MB/s  us sy id   1m   5m   15m
   37.13  105  3.80   127.34   11  1.31  1017.11    6  5.97  10  4 86  2.79 2.33 2.18
   22.67    3  0.07     0.00    0  0.00     0.00    0  0.00   5  3 92  2.79 2.33 2.18
    0.00    0  0.00     0.00    0  0.00     0.00    0  0.00   6  3 91  2.65 2.31 2.17
    0.00    0  0.00     0.00    0  0.00     0.00    0  0.00   7  3 90  2.65 2.31 2.17
    0.00    0  0.00     0.00    0  0.00     0.00    0  0.00   7  3 90  2.65 2.31 2.17
```

We could use system evaluation tags `(?` and `?)` to easily capture then parse them into a dataframe:
```sql
def iostat(n, m) := TableArray((? iostat $n $m ?).drop(1).map(x =>
  x.trim()
   .replaceAll("  ", " ")
   .split("[ ]")
   .filter(x => not x.isEmpty())
))
iostat(1, 5)
```
##### produced the following:
```sql
|--------------------------------------------------------------------------------------------------|
| KB/t  | tps | MB/s | KB/t  | tps | MB/s | KB/t  | tps | MB/s | us | sy | id | 1m   | 5m   | 15m  |
|--------------------------------------------------------------------------------------------------|
| 37.19 | 105 | 3.81 | 37.19 | 105 | 3.81 | 37.19 | 105 | 3.81 | 10 |  4 | 86 | 2.62 | 2.44 | 2.43 |
|  5.62 | 338 | 1.85 |  5.62 | 338 | 1.85 |  5.62 | 338 | 1.85 | 18 |  5 | 78 | 2.62 | 2.44 | 2.43 |
| 22.67 |   3 | 0.07 | 22.67 |   3 | 0.07 | 22.67 |   3 | 0.07 | 11 |  4 | 85 | 2.62 | 2.44 | 2.43 |
|   0.0 |   0 |  0.0 |   0.0 |   0 |  0.0 |   0.0 |   0 |  0.0 | 11 |  4 | 85 | 2.62 | 2.44 | 2.43 |
|   0.0 |   0 |  0.0 |   0.0 |   0 |  0.0 |   0.0 |   0 |  0.0 | 10 |  4 | 85 | 2.62 | 2.44 | 2.43 |
|--------------------------------------------------------------------------------------------------|
```
Now we have a dataframe (table) containing the metrics outputted by `iostat`

