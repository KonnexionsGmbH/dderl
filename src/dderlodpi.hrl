-ifndef(DDERLODPI_HRL).
-define(DDERLODPI_HRL, true).

-include("dderl.hrl").
-include_lib("imem/include/imem_sql.hrl").

-define(InitTimeout, 3600000).
-define(ExecTimeout, 3600000).

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

-record(odpi_conn, {node, context, connection}).

-endif. % DDERLODPI_HRL
