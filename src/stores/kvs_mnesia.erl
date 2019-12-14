-module(kvs_mnesia).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include("cursors.hrl").
% -include("stream.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/qlc.hrl").
% -export(?BACKEND).
% -export([info/1,exec/1,dump/1,seq/0]).
-compile(export_all).

start()    -> mnesia:start().
stop()     -> mnesia:stop().
destroy()  -> [mnesia:delete_table(T)||{_,T}<-kvs:dir()], mnesia:delete_schema([node()]), ok.
version()  -> {version,"KVS MNESIA"}.
dir()      -> [{table,T}||T<-mnesia:system_info(local_tables)].
join([])   -> mnesia:start(), mnesia:change_table_copy_type(schema, node(), disc_copies), initialize();
join(Node) ->
    mnesia:start(),
    mnesia:change_config(extra_db_nodes, [Node]),
    mnesia:change_table_copy_type(schema, node(), disc_copies),
    [{Tb, mnesia:add_table_copy(Tb, node(), Type)}
     || {Tb, [{N, Type}]} <- [{T, mnesia:table_info(T, where_to_commit)}
                               || T <- mnesia:system_info(tables)], Node==N].

initialize() ->
    mnesia:create_schema([node()]),
    Res = [ kvs:initialize(kvs_mnesia,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity),
    Res.

index(Tab,Key,Value) ->
    Table = kvs:table(Tab),
    Index = string:str(Table#table.fields,[Key]),
    lists:flatten(many(fun() -> mnesia:index_read(Tab,Value,Index+1) end)).

get(Tab, Key) when is_atom(Tab) -> just_one(fun() -> mnesia:read(Tab, Key) end);
get(Feed, Key) -> case get(writer, Feed) of
                    {ok, W} -> First = W#writer.first, 
                                case is_tuple(First) of
                                    true -> get(element(1, First), Key);
                                    false -> {error, no_first}
                                end;
                    {error,_} -> {error, no_writer}
                  end.

put(Records) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record) -> put([Record]).
delete(Tab, Key) when is_atom(Tab) ->
    case mnesia:activity(context(),fun()-> mnesia:delete({Tab, Key}) end) of
        {aborted,Reason} -> {error,Reason};
        {atomic,_Result} -> ok;
        _ -> ok end;
delete(Feed, Key) -> case get(writer, Feed) of
                        {ok, W} -> First = W#writer.first, 
                                    case is_tuple(First) of
                                        true -> delete(element(1, First), Key);
                                        false -> {error, no_first}
                                    end;
                        {error,_} -> {error, no_writer}
                    end.
        

count(RecordName) -> mnesia:table_info(RecordName, size).
all(R) -> lists:flatten(many(fun() -> L= mnesia:all_keys(R), [ mnesia:read({R, G}) || G <- L ] end)).

seq() -> erlang:integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_monotonic_time_source))))).
seq([],[]) -> seq(global_seq, 1);
seq(RecordName, Incr) -> integer_to_list(mnesia:dirty_update_counter({id_seq, RecordName}, Incr)).
many(Fun) -> case mnesia:activity(context(),Fun) of {atomic, [R]} -> R; {aborted, Error} -> {error, Error}; X -> X end.
void(Fun) -> case mnesia:activity(context(),Fun) of {atomic, ok} -> ok; {aborted, Error} -> {error, Error}; X -> X end.
info(T) -> try mnesia:table_info(T,all) catch _:_ -> [] end.
create_table(Name,Options) -> mnesia:create_table(Name, Options).
add_table_index(Record, Field) -> mnesia:add_table_index(Record, Field).
exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:activity(context(),F), Val.
just_one(Fun) ->
    case mnesia:activity(context(),Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        [] -> {error, not_found};
        [R] -> {ok,R};
        R when is_list(R) -> {ok,R};
        Error -> Error end.

%add(Record) -> mnesia:activity(context(),fun() -> kvs:append(Record,#kvs{mod=?MODULE}) end).
context() -> application:get_env(kvs,mnesia_context,async_dirty).

dump() -> dump([ N || #table{name=N} <- kvs:tables() ]), ok.
dump(short) ->
    Gen = fun(T) ->
        {S,M,C}=lists:unzip3([ dump_info(R) || R <- T ]),
        {lists:usort(S),lists:sum(M),lists:sum(C)}
    end,
    dump_format([ {T,Gen(T)} || T <- [ N || #table{name=N} <- kvs:tables() ] ]);
dump(Table) when is_atom(Table) -> dump(Table);
dump(Tables) ->
    dump_format([{T,dump_info(T)} || T <- lists:flatten(Tables) ]).
dump_info(T) ->
    {mnesia:table_info(T,storage_type),
    mnesia:table_info(T,memory) * erlang:system_info(wordsize) / 1024 / 1024,
    mnesia:table_info(T,size)}.
dump_format(List) ->
    io:format("~20s ~32s ~14s ~10s~n~n",["NAME","STORAGE TYPE","MEMORY (MB)","ELEMENTS"]),
    [ io:format("~20s ~32w ~14.2f ~10b~n",[T,S,M,C]) || {T,{S,M,C}} <- List ],
    io:format("~nSnapshot taken: ~p~n",[calendar:now_to_datetime(os:timestamp())]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% stream
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% next(#reader{cache={T,R}, pos=P}=C) -> kvs:n(kvs:get(T, R), C, P+1).
% prev(#reader{cache={T,R}, pos=P}=C) -> kvs:p(kvs:get(T, R), C, P-1).
  
% take(#reader{cache=[]}=C) -> C#reader{args=[]};
% take(#reader{dir=D, cache=_B, args=N, pos=P}=C)  -> take(kvs:acc(D), N, C, C, [], P).
% take(_, _, {error,_}, C2, R, P) -> C2#reader{args=lists:flatten(R),pos=P,cache={kvs:tab(hd(R)), kvs:en(hd(R))}};
% take(_, 0, _, C2, R, P)         -> C2#reader{args=lists:flatten(R),pos=P,cache={kvs:tab(hd(R)), kvs:en(hd(R))}};
% take(A, N, #reader{cache={T,I}, pos=P}=C, C2, R, _) -> take(A, N-1, ?MODULE:A(C), C2, [element(2, kvs:get(T, I))|R], P).

take(#reader{cache=[]}=R) -> R#reader{args=[]};
take(#reader{args=N}=R) -> take(R#reader{args=[]}, N).
take(R, 0) -> R;
take(#reader{dir=D, pos=P, cache={T, I}}=R, N) -> case kvs:get(T, I) of
                                                    {ok, Elem} -> R0 = R#reader{args=[Elem|R#reader.args]},
                                                                  take(R0, N-1, T, kvs:acc(Elem, D), P+D);
                                                    _ -> R
                                            end.
take(#reader{}=R, N, _T, I, _P) when N==0 orelse I==[] -> R;
take(#reader{cache=C}=R, _N, T, I, _P) when C == {T, I} -> R;
take(#reader{}=R, N, T, I, P) -> take(R#reader{pos=P, cache={T, I}}, N).


drop(#reader{dir=D,cache=B,args=N,pos=P}=C)  -> drop(kvs:acc(D), N, C, C, P, B).
drop(_, _, {error,_}, C2, P, B)     -> C2#reader{pos=P,cache=B};
drop(_, 0, _, C2, P, B)             -> C2#reader{pos=P,cache=B};
drop(A,N,#reader{cache=B,pos=P}=C,C2,_,_) -> drop(A, N-1, ?MODULE:A(C), C2, P, B).

put(Records, _Feed) when is_list(Records) -> put(Records);
put(Record, _Feed) -> put([Record]).
