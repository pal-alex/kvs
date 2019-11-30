-ifndef(API_HRL).
-define(API_HRL, true).
-define(API,[start/0,stop/0,leave/0,leave/1,
             join/0,join/1,join/2,modules/0,cursors/0,get/2,get/3,put/1,put/2,index/3,delete/2,
             seq/0, seq/2, seq_gen/0, dump/0, metainfo/0, table/1, tables/0, fields/1, has_field/2, get_field/2,
             dir/0, initialize/2, all/1, all/2, count/1, ver/0, get_value/2, get_value/3, fetch/2, fetch/3,
             fold/6, fold/7, head/1, head/2]).
-include("metainfo.hrl").
-spec seq(atom() | [], integer() | []) -> term().
-spec seq() -> term().
-spec count(atom()) -> integer().
-spec dir() -> list({'table',atom()}).
-spec ver() -> {'version',string()}.
-spec leave() -> ok.
-spec join() -> ok | {error,any()}.
-spec join(Node :: string()) -> [{atom(),any()}].
-spec modules() -> list(atom()).
-spec cursors() -> list({atom(),list(atom())}).
-spec tables() -> list(#table{}).
-spec table(Tab :: atom()) -> #table{}.
-endif.
