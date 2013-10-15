{application, epgsql_pool,
 [{description, "PostgreSQL Connection Pool"},
  {vsn, "0.1.0"},
  {modules, [epgsql_pool, pgsql_pool]},
  {registered, [epgsql_pool]},
  {mod, {epgsql_pool, []}},
  {applications, [kernel, stdlib, epgsql]},
  {included_applications, []},
  {env, [{pools, []}]}]}.
