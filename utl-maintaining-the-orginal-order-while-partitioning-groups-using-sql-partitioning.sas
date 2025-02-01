%let pgm=utl-maintaining-the-orginal-order-while-partitioning-groups-using-sql-partitioning;

%stop_submission;

Maintaining the orginal data order while partitioning groups using sql partitioning

github
https://tinyurl.com/4c5ypfpv
https://github.com/rogerjdeangelis/utl-maintaining-the-orginal-order-while-partitioning-groups-using-sql-partitioning

This is an enhancement to the previous sqlpartitioning macro

SOAPBOX ON

SQL Patitioning is very usefull when

  1 transposing within groups
  2 top and bottom n per group
  3 lag by group
  4 first and last per group

NOTE: SAS transpose and macros transpose and untranspose are faster and more direct solutions.
However SQL provides a bridge amoung languages

SOAPBOX OFF


   SOLUTIONS

      1 old macro sqlpartition supports sql arrays
      2 new macro sqlpartitionx (data order preserved also supports sqlarrays)
      3 r python excel sql (no need for partition macro. Gen code with array and doover macro)
        for python and excel see https://tinyurl.com/2s92n76c
      4 macro sqlpartitionx
      5 related repos


macros library
https://tinyurl.com/y9nfugth
https://github.com/rogerjdeangelis/utl-macros-used-in-many-of-rogerjdeangelis-repositories



/**************************************************************************************************************************/
/*   _                                _                  _   _ _   _                                                      */
/*  (_)___ ___ _   _  ___   ___  __ _| |_ __   __ _ _ __| |_(_) |_(_) ___  _ __                                           */
/*  | / __/ __| | | |/ _ \ / __|/ _` | | `_ \ / _` | `__| __| | __| |/ _ \| `_ \                                          */
/*  | \__ \__ \ |_| |  __/ \__ \ (_| | | |_) | (_| | |  | |_| | |_| | (_) | | | |                                         */
/*  |_|___/___/\__,_|\___| |___/\__, |_| .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|                                         */
/*                                 |_| |_|                                                                                */
/*                                                                                                                        */
/*------------------------------------------------------------------------------------------------------------------------*/
/*            |                                                                  |                                        */
/*     INPUT  |                              PROCESS                             |               OUTPUT                   */
/*            |                                                                  |                                        */
/*TEAM  SCORE |  proc sql;                                                       |                                        */
/*            |    create                                                        | ISSSUE (ARE NOT IN THE DATA ORDER)     */
/* cubs    4  |      table oldXpo as                                             |                                        */
/* cubs    2  |    select                                                        | ISSUE                                  */
/* cubs    6  |      team                                                        |                                        */
/* cubs    5  |      ,max(case when partition=1 then score else . end) as score1 | TEAM  SCORE1 SCORE2 SCORE3 SCORE4      */
/* reds    7  |      ,max(case when partition=2 then score else . end) as score2 |                                        */
/* reds    9  |      ,max(case when partition=3 then score else . end) as score3 | cubs     6      2      4      5        */
/* reds    8  |      ,max(case when partition=4 then score else . end) as score4 | reds     8      9      7      .        */
/*            |    from                                                          |                                        */
/* data       |       %sqlpartition(sd1.have,by=team)                            | SHOULD BE                              */
/*  sd1.have; |    group                                                         |                                        */
/* input      |       by team                                                    | TEAM  SCORE1 SCORE2 SCORE3 SCORE4      */
/*   team$    |  ;quit;                                                          |                                        */
/*   score;   |                                                                  | cubs     4      2      6      5        */
/* cards4;    |                                                                  | reds     7      9      8      .        */
/* cubs 4     |  WHAT SQLPARTITION DOES                                          |                                        */
/* cubs 2     |                                                                  |                                        */
/* cubs 6     |  NOTE: SCORES ARE OUT OF ORDER                                   |                                        */
/* cubs 5     |                                                                  |                                        */
/* reds 7     |   TEAM  SCORE  PARTITION                                         |                                        */
/* reds 9     |  -------------------------                                       |                                        */
/* reds 8     |   cubs      6 was 4    1                                         |                                        */
/* ;;;;       |   cubs      2          2                                         |                                        */
/* run;quit;  |   cubs      4 was 6    3                                         |                                        */
/*            |   cubs      5          4                                         |                                        */
/*            |                                                                  |                                        */
/*            |   reds      8 was 7    1                                         |                                        */
/*            |   reds      9          2                                         |                                        */
/*            |   reds      7 was 8    3                                         |                                        */
/*            |                                                                  |                                        */
/* -----------------------------------------------------------------------------------------------------------------------*/
/*   __ _                 _                  _   _ _   _                                                                  */
/*  / _(_)_  _  ___  __ _| |_ __   __ _ _ __| |_(_) |_(_) ___  _ __ __  __                                                */
/* | |_| \ \// / __|/ _` | | `_ \ / _` | `__| __| | __| |/ _ \| `_ \\ \/ /                                                */
/* |  _| |>    \__ \ (_| | | |_) | (_| | |  | |_| | |_| | (_) | | | |>  <                                                 */
/* |_| |_/_/\\ |___/\__, |_| .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_/_/\_\                                                */
/*                     |_| |_|                                                                                            */
/*                                                                                                                        */
/*------------------------------------------------------------------------------------------------------------------------*/
/*            |                                                                  |                                        */
/*            |  proc sql;                                                       | PIVOT IN DATA ORDER                    */
/*            |    create                                                        |                                        */
/*            |      table newXpo as                                             | TEAM SCORE1 SCORE2 SCORE3 SCORE4       */
/*            |    select                                                        |                                        */
/*            |      team                                                        | cubs    4      2      6      5         */
/*            |      ,max(case when partition=1 then score else . end) as score1 | reds    7      9      8      .         */
/*            |      ,max(case when partition=2 then score else . end) as score2 |                                        */
/*            |      ,max(case when partition=3 then score else . end) as score3 |                                        */
/*            |      ,max(case when partition=4 then score else . end) as score4 |                                        */
/*            |    from                                                          |                                        */
/*            |       %sqlpartitionx(sd1.have,by=team)                           |                                        */
/*            |    group                                                         |                                        */
/*            |       by team                                                    |                                        */
/*            |  ;quit;                                                          |                                        */
/*            |                                                                  |                                        */
/*            |  WHAT SQLPARTIONINGX MACRO DOES                                  |                                        */
/*            |                                                                  |                                        */
/*            |                                                                  |                                        */
/*            |  TEAM SCORE  PARTITION                                           |                                        */
/*            |  ----------------------                                          |                                        */
/*            |  cubs     4          1                                           |                                        */
/*            |  cubs     2          2                                           |                                        */
/*            |  cubs     6          3                                           |                                        */
/*            |  cubs     5          4                                           |                                        */
/*            |                                                                  |                                        */
/*            |  reds     7          1                                           |                                        */
/*            |  reds     9          2                                           |                                        */
/*            |  reds     8          3                                           |                                        */
/*            |                                                                  |                                        */
/*------------------------------------------------------------------------------------------------------------------------*/
/*                     _   _                                     _                                                        */
/*  _ __   _ __  _   _| |_| |__   ___  _ __     _____  _____ ___| |                                                       */
/* | `__| | `_ \| | | | __| `_ \ / _ \| `_ \   / _ \ \/ / __/ _ \ |                                                       */
/* | |    | |_) | |_| | |_| | | | (_) | | | | |  __/>  < (_|  __/ |                                                       */
/* |_|    | .__/ \__, |\__|_| |_|\___/|_| |_|  \___/_/\_\___\___|_|                                                       */
/*        |_|    |___/                                                                                                    */
/* -----------------------------------------------------------------------------------------------------------------------*/
/*            |                                                                  |                                        */
/*            | select                                                           |                                        */
/*            |   team                                                           |                                        */
/*            |  ,max(case when partition=1 then score else NULL end) as score1  |                                        */
/*            |  ,max(case when partition=2 then score else NULL end) as score2  |                                        */
/*            |  ,max(case when partition=3 then score else NULL end) as score3  |                                        */
/*            |  ,max(case when partition=4 then score else NULL end) as score4  |                                        */
/*            | from                                                             |                                        */
/*            |   (select team                                                   |                                        */
/*            |      ,score                                                      |                                        */
/*            |      ,row_number() OVER (PARTITION BY team) as partition         |                                        */
/*            |   from have)                                                     |                                        */
/*            | group                                                            |                                        */
/*            |   by team;                                                       |                                        */
/*            |                                                                  |                                        */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

data
 sd1.have;
input
  team$
  score;
cards4;
cubs 4
cubs 2
cubs 6
cubs 5
reds 7
reds 9
reds 8
;;;;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/*  TEAM    SCORE                                                                                                         */
/*                                                                                                                        */
/*  cubs      4                                                                                                           */
/*  cubs      2                                                                                                           */
/*  cubs      6                                                                                                           */
/*  cubs      5                                                                                                           */
/*  reds      7                                                                                                           */
/*  reds      9                                                                                                           */
/*  reds      8                                                                                                           */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*         _     _                                             _                  _   _ _   _
/ |   ___ | | __| |  _ __ ___   __ _  ___ _ __ ___   ___  __ _| |_ __   __ _ _ __| |_(_) |_(_) ___  _ __ __  __
| |  / _ \| |/ _` | | `_ ` _ \ / _` |/ __| `__/ _ \ / __|/ _` | | `_ \ / _` | `__| __| | __| |/ _ \| `_ \\ \/ /
| | | (_) | | (_| | | | | | | | (_| | (__| | | (_) |\__ \ (_| | | |_) | (_| | |  | |_| | |_| | (_) | | | |>  <
|_|  \___/|_|\__,_| |_| |_| |_|\__,_|\___|_|  \___/ |___/\__, |_| .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_/_/\_\
                                                            |_| |_|
*/

proc sql;
  create
    table oldXpo as
  select
    team
    ,max(case when partition=1 then score else . end) as score1
    ,max(case when partition=2 then score else . end) as score2
    ,max(case when partition=3 then score else . end) as score3
    ,max(case when partition=4 then score else . end) as score4
  from
     %sqlpartition(sd1.have,by=team)
  group
     by team
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  TEAM    SCORE1    SCORE2    SCORE3    SCORE4                                                                          */
/*                                                                                                                        */
/*  cubs       6         2         4         5                                                                            */
/*  reds       8         9         7         .                                                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                                                    _                  _   _ _   _
|___ \   _ __   _____      __  _ __ ___   __ _  ___ _ __ ___   ___  __ _| |_ __   __ _ _ __| |_(_) |_(_) ___  _ __ __  __
  __) | | `_ \ / _ \ \ /\ / / | `_ ` _ \ / _` |/ __| `__/ _ \ / __|/ _` | | `_ \ / _` | `__| __| | __| |/ _ \| `_ \\ \/ /
 / __/  | | | |  __/\ V  V /  | | | | | | (_| | (__| | | (_) |\__ \ (_| | | |_) | (_| | |  | |_| | |_| | (_) | | | |>  <
|_____| |_| |_|\___| \_/\_/   |_| |_| |_|\__,_|\___|_|  \___/ |___/\__, |_| .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_/_/\_\
                                                                      |_| |_|
*/

proc sql;
  create
    table newXpo as
  select
    team
    ,max(case when partition=1 then score else . end) as score1
    ,max(case when partition=2 then score else . end) as score2
    ,max(case when partition=3 then score else . end) as score3
    ,max(case when partition=4 then score else . end) as score4
  from
     %sqlpartitionx(sd1.have,by=team)
  group
     by team
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* TEAM    SCORE1    SCORE2    SCORE3    SCORE4                                                                           */
/*                                                                                                                        */
/* cubs       4         2         6         5                                                                             */
/* reds       7         9         8         .                                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                      _   _                                    _             _
|___ /   _ __   _ __  _   _| |_| |__   ___  _ __    _____  _____ ___| |  ___  __ _| |
  |_ \  | `__| | `_ \| | | | __| `_ \ / _ \| `_ \  / _ \ \/ / __/ _ \ | / __|/ _` | |
 ___) | | |    | |_) | |_| | |_| | | | (_) | | | ||  __/>  < (_|  __/ | \__ \ (_| | |
|____/  |_|    | .__/ \__, |\__|_| |_|\___/|_| |_| \___/_/\_\___\___|_| |___/\__, |_|
               |_|    |___/                                                     |_|
*/

/*---- for python and excel see https://tinyurl.com/2s92n76c ----*/

proc datasets lib=sd1 nolist nodetails;
 delete want;
run;quit;

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
want<-sqldf("
 select
    team
   ,max(case when partition=1 then score else NULL end) as score1
   ,max(case when partition=2 then score else NULL end) as score2
   ,max(case when partition=3 then score else NULL end) as score3
   ,max(case when partition=4 then score else NULL end) as score4
 from
   (select team
      ,score
      ,row_number() OVER (PARTITION BY team) as partition
   from have)
 group
   by team;
 ");
want;
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="want"
     )
;;;;
%utl_rendx;

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                      |                                                                                 */
/* > want;                              |  SAS                                                                            */
/*                                      |                                                                                 */
/*   team score1 score2 score3 score4   |  ROWNAMES    TEAM    SCORE1    SCORE2    SCORE3    SCORE4                       */
/*                                      |                                                                                 */
/* 1 cubs      4      2      6      5   |      1       cubs       4         2         6         5                         */
/* 2 reds      7      9      8     NA   |      2       reds       7         9         8         .                         */
/*                                      |                                                                                 */
/**************************************************************************************************************************/

/*  _                                              _                  _   _ _   _ _
| || |   _ __ ___   __ _  ___ _ __ ___   ___  __ _| |_ __   __ _ _ __| |_(_) |_(_|_) ___  _ __ __  __
| || |_ | `_ ` _ \ / _` |/ __| `__/ _ \ / __|/ _` | | `_ \ / _` | `__| __| | __| | |/ _ \| `_ \\ \/ /
|__   _|| | | | | | (_| | (__| | | (_) |\__ \ (_| | | |_) | (_| | |  | |_| | |_| | | (_) | | | |>  <
   |_|  |_| |_| |_|\__,_|\___|_|  \___/ |___/\__, |_| .__/ \__,_|_|   \__|_|\__|_|_|\___/|_| |_/_/\_\
                                                |_| |_|
*/

/*---- sas does not appear to support sql 'order by' in a subquery? ----*/

filename ft15f001 "c:/oto/sqlpartitionx.sas";
parmcards4;
%macro sqlpartitionx(dsn,by=team)/
   des="Improved sqlpartition that maintains data order";

 ( select
     *
     ,max(seq) as seq
   from
     (select
        *
       ,seq-min(seq) + 1 as partition
     from
       (select *, monotonic() as seq from sd1.have)
     group
       by team )
   group
       by team, seq
   having
       1=1)

%mend sqlpartitionx;
;;;;
run;quit;

/*___             _       _           _
| ___|   _ __ ___| | __ _| |_ ___  __| |  _ __ ___ _ __   ___  ___
|___ \  | `__/ _ \ |/ _` | __/ _ \/ _` | | `__/ _ \ `_ \ / _ \/ __|
 ___) | | | |  __/ | (_| | ||  __/ (_| | | | |  __/ |_) | (_) \__ \
|____/  |_|  \___|_|\__,_|\__\___|\__,_| |_|  \___| .__/ \___/|___/
                                                  |_|
*/

REPO
---------------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-adding-sequence-numbers-and-partitions-in-SAS-sql-without-using-monotonic
https://github.com/rogerjdeangelis/utl-create-equally-spaced-values-using-partitioning-in-sql-wps-r-python
https://github.com/rogerjdeangelis/utl-create-primary-key-for-duplicated-records-using-sql-partitionaling-and-pivot-wide-sas-python-r
https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-flag-second-duplicate-using-base-sas-and-sql-sas-python-and-r-partitioning-multi-language
https://github.com/rogerjdeangelis/utl-incrementing-by-one-for-each-new-group-of-records-sas-r-python-sql-partitioning
https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot
https://github.com/rogerjdeangelis/utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python
https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning
https://github.com/rogerjdeangelis/utl-sql-partitioning-increase-in-investment-when-interest-rates-change-over-time-compound-interest
https://github.com/rogerjdeangelis/utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-transposing-words-into-sentences-using-sql-partitioning-in-r-and-python
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning


/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
