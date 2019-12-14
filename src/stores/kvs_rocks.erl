-module(kvs_rocks).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include("cursors.hrl").
-include_lib("stdlib/include/qlc.hrl").
% -export(?BACKEND).
% -export([seq/0]).
% -export([ref/0,next/8,format/1,bt/1,bt/2]).
-compile(export_all).

bt(X)      -> bt(X,false).
bt(X, true)      -> binary_to_term(X,[safe]);
bt(X, false)      -> binary_to_term(X).
tb(X) -> term_to_binary(X).
lb(X) -> list_to_binary(X).

start()    -> ok.
stop()     -> ok.
destroy()  -> ok.
version()  -> {version,"KVS ROCKSDB"}.
dir()      -> [].
leave() -> case ref() of [] -> skip; X -> rocksdb:close(X) end.
join(_) -> application:start(rocksdb),
           leave(), {ok, Ref} = rocksdb:open(application:get_env(kvs,rocks_name,"rocksdb"), [{create_if_missing, true}]),
           initialize(),
           application:set_env(kvs,rocks_ref,Ref).
initialize() -> [ kvs:initialize(kvs_rocks,Module) || Module <- kvs:modules() ].
ref() -> application:get_env(kvs,rocks_ref,[]).
index(_,_,_) -> [].

get(Tab, Key) ->
    Address = get_address(Tab, Key),
    % io:format("KVS.GET.Address: ~s~n",[Address]),
    case rocksdb:get(ref(), Address, []) of
         not_found -> {error,not_found};
         {ok,Bin} -> {ok,bt(Bin)} end.

put(Records) when is_list(Records) -> lists:foreach(fun(Record) -> put(Record) end, Records);
put(Record) -> put(Record, Record).
feed_key(Record, Feed) -> get_address(Feed, kvs:id(Record)).
get_address(Tab, Id) -> <<(lb("/" ++ kvs_rocks:format(Tab) ++ "/"))/binary, (tb(Id))/binary>>.
put(Records, Feed) when is_list(Records) -> lists:foreach(fun(Record) -> put(Record, Feed) end, Records);
put(Record, Feed) -> 
    Address = feed_key(Record, Feed),
    % io:format("KVS.PUT.Address: ~s~n",[Address]),
    rocksdb:put(ref(), Address, tb(Record), [{sync,true}])
.


format(X) when is_tuple(X) andalso is_atom(element(1, X)) -> format(element(1, X));
format(X) when is_list(X) -> X;
format(X) when is_atom(X) -> atom_to_list(X);
format(X) -> io_lib:format("~p",[X]).

delete(Feed, Id) ->
    Key    = lb(lists:concat(["/",format(Feed),"/"])),
    A      = <<Key/binary,(tb(Id))/binary>>,
    rocksdb:delete(ref(), A, []).

count(_) -> 0.
all(R) -> {ok,I} = rocksdb:iterator(ref(), []),
           Key = lb(lists:concat(["/",format(R)])),
           First = rocksdb:iterator_move(I, {seek,Key}),
           lists:reverse(next(I,Key,size(Key),First,[],[],-1,0)).

next(_,_,_,_,_,T,N,C) when C == N -> T;
next(I,Key,S,{ok,A,X},_,T,N,C) -> next(I,Key,S,A,X,T,N,C);
next(_,___,_,{error,_},_,T,_,_) -> T;
next(I,Key,S,A,X,T,N,C) when size(A) > S ->
     case binary:part(A,0,S) of Key ->
          next(I,Key,S,rocksdb:iterator_move(I, next), [], [bt(X)|T],N,C+1);
                  _ -> T end;
next(_,_,_,_,_,T,_,_) -> T.

%seq(_,_) ->
%  case os:type() of
%       {win32,nt} -> {Mega,Sec,Micro} = erlang:now(), integer_to_list((Mega*1000000+Sec)*1000000+Micro);
%                _ -> erlang:integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_monotonic_time_source)))))
%  end.
seq() ->  erlang:integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_monotonic_time_source))))) .
seq([],[]) -> seq(global_seq, 1); 
seq(RecordName, Incr) -> Key = kvs_mnesia:seq(RecordName, Incr),
                        %  io:format("new key ~p = ~p~n", [RecordName, Key]),
                        %  integer_to_list(Key)
                        Key
                        .

create_table(_,_) -> [].
add_table_index(_, _) -> ok.
dump() -> ok.




% stream 


next (#reader{feed=Feed,cache=I}=C) when is_tuple(I) ->
    Key = feed_key(I,Feed),
    rocksdb:iterator_move(I, {seek,Key}),
    case rocksdb:iterator_move(I, next) of
        {ok,_,Bin} -> C#reader{cache=bt(Bin)};
            {error,Reason} -> {error,Reason} end.

prev (#reader{cache=I,id=Feed}=C) when is_tuple(I) ->
    Key = feed_key(I,Feed),
    rocksdb:iterator_move(I, {seek,Key}),
    case rocksdb:iterator_move(I, prev) of
        {ok,_,Bin} -> C#reader{cache=bt(Bin)};
            {error,Reason} -> {error,Reason} end.

% section: take, drop

drop(#reader{args=N,feed=Feed,cache=I}=C) when N == 0 ->
    Key = lb(lists:concat(["/",kvs_rocks:format(Feed)])),
    case rocksdb:iterator_move(I, {seek,Key}) of
        {ok,_,Bin} -> C#reader{cache=bt(Bin)};
                    _ -> C#reader{cache=[]} end;

drop(#reader{args=N,feed=Feed,cache=I}=C) when N > 0 ->
    Key   = lb(lists:concat(["/",kvs_rocks:format(Feed)])),
    First = rocksdb:iterator_move(I, {seek,Key}),
    Term  = lists:foldl(
    fun (_,{{ok,K,_},{_,X}}) when N > X -> {K,{<<131,106>>,N}};
        (_,{{ok,K,Bin},{A,X}}) when N =< X->
            case binary:part(K,0,size(Key)) of
                Key -> {rocksdb:iterator_move(I,next),{Bin,X+1}};
                    _ -> {{error,range},{A,X}} end;
        (_,{_,{_,_}}) -> {[],{<<131,106>>,N}}
        end,
            {First,{<<131,106>>,1}},
            lists:seq(0,N)),
    C#reader{cache=bt(element(1,element(2,Term)))}.

take(#reader{args=N,feed=Feed,cache={T,O}}=C) ->
    Key = lb(lists:concat(["/",kvs_rocks:format(Feed)])),
    {ok,I} = rocksdb:iterator(ref(), []),
    {ok,K,BERT} = rocksdb:iterator_move(I, {seek,feed_key({T,O},Feed)}),
    Res = kvs_rocks:next(I,Key,size(Key),K,BERT,[],case N of -1 -> -1; J -> J + 1 end,0),
    case {Res,length(Res) < N + 1 orelse N == -1} of
        {[],_}    -> C#reader{args=[],cache=[]};
        {[H|X],false} -> C#reader{args=X,cache={element(1, H), element(2, H)}};
        {[H|X],true} -> C#reader{args=Res,cache=[]} end.


remove(Rec,Feed) ->
    kvs:ensure(#writer{id=Feed}),
    W = #writer{count=C} = kvs:writer(Feed),
    {ok,I} = rocksdb:iterator(ref(), []),
    case kvs:delete(Feed, kvs:id(Rec)) of
        ok -> Count = C - 1,
                kvs:save(W#writer{count = Count, cache = I}),
                Count;
            _ -> C end.

set_iterator(Feed, _First) ->
        Key = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
        {ok,I} = rocksdb:iterator(ref(), []),
        {ok,K,BERT} = rocksdb:iterator_move(I, {seek,Key}),
        F = bt(BERT),
        F
.

       
prev(_,_,_,_,_,_,N,C) when C == N -> C;
prev(I,Key,S,{ok,A,X},_,T,N,C) -> prev(I,Key,S,A,X,T,N,C);
prev(_,___,_,{error,_},_,_,_,C) -> C;
prev(I,Key,S,A,_,_,N,C) when size(A) > S ->
        case binary:part(A,0,S) of Key ->
            rocksdb:delete(ref(), A, []),
            Next = rocksdb:iterator_move(I, prev),
            prev(I,Key, S, Next, [], A, N, C + 1);
                                    _ -> C end;
prev(_,_,_,_,_,_,_,C) -> C.
        
cut(Feed,Id) ->
    Key    = lb(lists:concat(["/",kvs_rocks:format(Feed),"/"])),
    A      = <<Key/binary,(tb(Id))/binary>>,
    {ok,I} = rocksdb:iterator(ref(), []),
    case rocksdb:iterator_move(I, {seek,A}) of
            {ok,A,X} -> {ok,prev(I,Key,size(Key),A,X,[],-1,0)};
                _ -> {error,not_found} end.
        
save_feed(Feed) -> ok.