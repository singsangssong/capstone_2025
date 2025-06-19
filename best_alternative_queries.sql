WITH default_plans (query_path, walltime) AS (
    SELECT q.query_path,
           AVG(walltime)
    FROM queries q,
         query_optimizer_configs qoc,
         measurements m
    WHERE q.id = qoc.query_id
      AND qoc.id = m.query_optimizer_config_id
      AND qoc.num_disabled_rules = 0
      AND qoc.disabled_rules = 'None'
    GROUP BY q.query_path,
             qoc.num_disabled_rules,
             qoc.disabled_rules
),
results(query_path, num_disabled_rules, runtime, runtime_baseline, savings, disabled_rules, rank) AS (
    SELECT q.query_path,
           qoc.num_disabled_rules,
           AVG(m.walltime),
           dp.walltime,
           (dp.walltime * 1.0 - AVG(m.walltime)) / dp.walltime AS savings,
           qoc.disabled_rules,
           dense_rank() OVER (
               PARTITION BY q.query_path
               ORDER BY (dp.walltime - AVG(m.walltime)) / dp.walltime DESC
           ) AS ranki
    FROM queries q,
         query_optimizer_configs qoc,
         measurements m,
         default_plans dp
    WHERE q.id = qoc.query_id
      AND qoc.id = m.query_optimizer_config_id
      AND dp.query_path = q.query_path
      AND qoc.num_disabled_rules > 0
    GROUP BY q.query_path,
             qoc.num_disabled_rules,
             qoc.disabled_rules
)
SELECT * FROM results
ORDER BY savings DESC;