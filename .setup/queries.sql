--Monthly Users
select lm.c as Last_Month, llm.c - lm.c as Prev_Month, lllm.c - llm.c as 2_Months_Ago from (SELECT count(distinct vid) as c FROM events where created > DATE_SUB(Now(), 30)) as lm cross join (SELECT count(distinct vid) as c FROM events where created > DATE_SUB(Now(), 60)) as llm cross join (SELECT count(distinct vid) as c FROM events where created > DATE_SUB(Now(), 90)) as lllm
