--Monthly Users
select lm.c as Last_Month, llm.c - lm.c as Prev_Month, lllm.c - llm.c as 2_Months_Ago from (SELECT count(distinct vid) as c FROM events where created > DATE_SUB(Now(), 30)) as lm cross join (SELECT count(distinct vid) as c FROM events where created > DATE_SUB(Now(), 60)) as llm cross join (SELECT count(distinct vid) as c FROM events where created > DATE_SUB(Now(), 90)) as lllm
--insert into logs (id) values (efbe604e-bcf8-11eb-a01a-3af9d39c7fb0);
--insert into logs (id) values (d9e97860-bcff-11eb-8353-3af9d39c7fb0);
--insert into logs (id) values (db4283d2-bcff-11eb-8354-3af9d39c7fb0);