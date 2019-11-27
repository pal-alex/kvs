-module(kvs).
-behaviour(application).
-behaviour(supervisor).
-description('KVS Abstract Chain Store').
-include_lib("stdlib/include/assert.hrl").
-include("api.hrl").
-include("metainfo.hrl").
-include("stream.hrl").
-include("cursors.hrl").
-include("kvs.hrl").
-include("backend.hrl").
-export(?API).
-export(?STREAM).

% -compile(export_all).
-export([init/1, start/2, stop/1]).

fields(Table)      -> T = table(Table),
                    case T of
                        false -> [];
                        _V -> T#table.fields
                    end
.
has_field(TableRecord, Field) -> FieldsList = fields(element(1, TableRecord)),
                                 lists:member(Field, FieldsList).

get_field(TableRecord, Field) -> FieldsList = fields(element(1, TableRecord)),
                                Index = string:str(FieldsList, [Field]) + 1,
                                element(Index, TableRecord).    


-record('$msg', {id,next,prev,user,msg}).

init([]) -> {ok, { {one_for_one, 5, 10}, []} }.
start(_,_) -> supervisor:start_link({local, kvs}, kvs, []).
stop(_) -> ok.
test_tabs() -> [ #table{name='$msg', fields=record_info(fields,'$msg')} ].

% kvs api

dba()              -> application:get_env(kvs,dba,kvs_mnesia).
% kvs_stream()       -> application:get_env(kvs,dba_st,kvs_stream).
all(Table)         -> all     (Table, #kvs{mod=dba()}).
delete(Table,Key)  -> delete  (Table, Key, #kvs{mod=dba()}).
get(Table,Key)     -> ?MODULE:get     (Table, Key, #kvs{mod=dba()}).
get_value(Table, Key) -> fetch(Table, Key, []).
get_value(Table, Key, Default) -> fetch(Table, Key, Default).
index(Table,K,V)   -> index   (Table, K,V, #kvs{mod=dba()}).
join()             -> join    ([],    #kvs{mod=dba()}).
dump()             -> dump    (#kvs{mod=dba()}).
join(Node)         -> join    (Node,  #kvs{mod=dba()}).
leave()            -> leave   (#kvs{mod=dba()}).
count(Table)       -> count   (Table, #kvs{mod=dba()}).
put(Record)        -> ?MODULE:put     (Record, #kvs{mod=dba()}).
fold(Fun,Acc,T,S,C,D) -> fold (Fun,Acc,T,S,C,D, #kvs{mod=dba()}).
stop()             -> stop_kvs(#kvs{mod=dba()}).
start()            -> start   (#kvs{mod=dba()}).
ver()              -> ver(#kvs{mod=dba()}).
dir()              -> dir     (#kvs{mod=dba()}).
seq()              -> seq(#kvs{mod=dba()}).
seq(Table,DX)      -> seq     (Table, DX, #kvs{mod=dba()}).


metainfo() ->  #schema { name = kvs, tables = core() ++ stream_tables() ++ test_tabs()}.
core()    -> [ #table { name = id_seq, fields = record_info(fields,id_seq), keys=[thing]} ].

initialize(Backend, Module) ->
    [ begin
        Backend:create_table(T#table.name, [{attributes,T#table.fields},
               {T#table.copy_type, [node()]},{type,T#table.type}]),
        [ Backend:add_table_index(T#table.name, Key) || Key <- T#table.keys ],
        T
    end || T <- (Module:metainfo())#schema.tables ].

all(Tab,#kvs{mod=DBA}) -> DBA:all(Tab).
start(#kvs{mod=DBA}) -> DBA:start().
stop_kvs(#kvs{mod=DBA}) -> DBA:stop().
join(Node,#kvs{mod=DBA}) -> DBA:join(Node).
leave(#kvs{mod=DBA}) -> DBA:leave().
ver(#kvs{mod=DBA}) -> DBA:version().
tables() -> lists:flatten([ (M:metainfo())#schema.tables || M <- modules() ]).
table(Name) when is_atom(Name) -> lists:keyfind(Name,#table.name,tables());
table(_) -> false.
dir(#kvs{mod=DBA}) -> DBA:dir().
modules() -> application:get_env(kvs, schema, []).
cursors() ->
    lists:flatten([ [ {T#table.name,T#table.fields}
        || #table{name=Name}=T <- (M:metainfo())#schema.tables, Name == reader orelse Name == writer  ]
    || M <- modules() ]).

fold(___,Acc,_,[],_,_,_) -> Acc;
fold(___,Acc,_,undefined,_,_,_) -> Acc;
fold(___,Acc,_,_,0,_,_) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Driver) ->
    try
    case kvs:get(Table, Start, Driver) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), Table, Prev, Count1, Direction, Driver);
          _Error -> Acc
    end catch _:_ -> Acc end.

seq_gen() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> {Key,kvs:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- cursors() ].

put(Records,#kvs{mod=Mod}) when is_list(Records) -> Mod:put(Records);
put(Record,#kvs{mod=Mod}) -> Mod:put(Record).
get(RecordName, Key, #kvs{mod=Mod}) -> Mod:get(RecordName, Key).
delete(Tab, Key, #kvs{mod=Mod}) -> Mod:delete(Tab, Key).
count(Tab,#kvs{mod=DBA}) -> DBA:count(Tab).
index(Tab, Key, Value,#kvs{mod=DBA}) -> DBA:index(Tab, Key, Value).
seq(#kvs{mod=DBA}) ->  DBA:seq().
seq(Tab, Incr,#kvs{mod=DBA}) -> DBA:seq(Tab, Incr).
dump(#kvs{mod=Mod}) -> Mod:dump().
% feed(Key, #kvs{mod=Mod}=KVS) -> (Mod:take((kvs:reader(Key))#reader{args=-1}))#reader.args.
% feed(Key, #kvs{mod=Mod}) -> Mod:feed(Key).
head(Key) -> case (kvs:take((kvs:reader(Key))#reader{args=1}))#reader.args of [X] -> X; [] -> [] end.
head(Key,Count) -> (kvs:take((kvs:reader(Key))#reader{args=Count,dir=1}))#reader.args.
fetch(Table, Key) -> fetch(Table, Key, []).
fetch(Table, Key, Default) -> case get(Table, Key) of
                                        {ok, Value} -> Value;
                                        _ -> Default
                                  end.

% stream api

top  (X) -> top (X, #kvs{mod=dba()}).
bot  (X) -> bot (X, #kvs{mod=dba()}).
next (X) -> next(X, #kvs{mod=dba()}).
prev (X) -> prev(X, #kvs{mod=dba()}).
drop (X) -> drop(X, #kvs{mod=dba()}).
add  (X) -> add (X, #kvs{mod=dba()}).
take (X) -> take(X, #kvs{mod=dba()}).
save (X) -> save(X, #kvs{mod=dba()}).

remove(X, Y) -> remove(X, Y, #kvs{mod=dba()}).
cut  (X, Y) -> cut (X, Y, #kvs{mod=dba()}).

append  (X, Y) -> append (X, Y, false, #kvs{mod=dba()}).
append  (X, Y, Z) -> append (X, Y, Z, #kvs{mod=dba()}).
load_reader (X) -> load_reader(X, #kvs{mod=dba()}).
writer      (X) -> writer(X, #kvs{mod=dba()}).
reader      (X) -> reader(X, #kvs{mod=dba()}).
ensure(#writer{id=Id}) -> ensure(#writer{id=Id}, #kvs{mod=dba()}).
feed(X) -> feed(X, #kvs{mod=dba()}).
      

% general stream
stream_tables() -> [ #table  { name = writer, fields = record_info(fields, writer) },
                     #table  { name = reader, fields = record_info(fields, reader) } ].

% section: kvs_stream prelude

se(X,Y,Z)  -> setelement(X,Y,Z).
e(X,Y)  -> element(X,Y).
c4(R,V) -> se(#reader.args,  R, V).
sn(M,T) -> se(#iter.next, M, T).
sp(M,T) -> se(#iter.prev, M, T).
si(M,T) -> se(#iter.id, M, T).
tab(T)  -> e(1, T).
id(T)   -> e(#iter.id, T).
en(T)   -> e(#iter.next, T).
ep(T)   -> e(#iter.prev, T).
acc(0)  -> next;
acc(1)  -> prev.


% section: top, bot, next, prev

top(#reader{feed=F}=C, KVS) -> w(kvs:writer(F, KVS), top, C).
bot(#reader{feed=F}=C, KVS) -> w(kvs:writer(F, KVS), bot, C).
next(#reader{cache=[]}, _KVS) -> {error,empty};
next(#reader{cache={T,R}, pos=P}=C, KVS) -> n(kvs:get(T, R, KVS), C, P+1, KVS).
prev(#reader{cache=[]}, _KVS) -> {error,empty};
prev(#reader{cache={T,R},pos=P}=C, KVS) -> p(kvs:get(T, R, KVS), C, P-1, KVS).

n({ok,R}, C, P, KVS)    -> r(kvs:get(tab(R), en(R), KVS), C, P);
n({error,X},_,_, _KVS) -> {error,X}.
p({ok,R}, C, P, KVS)    -> r(kvs:get(tab(R), ep(R), KVS), C, P);
p({error,X},_,_, _KVS) -> {error,X}.
r({ok,R}, C, P)    -> C#reader{cache={tab(R), id(R)}, pos=P};
r({error,X},_,_) -> {error,X}.
w({ok, #writer{first=[]}}, bot, C)           -> C#reader{cache=[], pos=1};
w({ok, #writer{first=B}}, bot, C)            -> C#reader{cache={tab(B), id(B)}, pos=1};
w({ok, #writer{cache=B, count=Size}}, top, C) -> C#reader{cache={tab(B), id(B)}, pos=Size};
w({error,X},_,_)                          -> {error,X}.

% section: take, drop

drop(#reader{cache=[]}=C, _KVS) -> C#reader{args=[]};
drop(#reader{dir=D,cache=B,args=N,pos=P}=C, _KVS)  -> drop(acc(D), N, C, C, P, B).
drop(_,_,{error,_},C2,P,B)     -> C2#reader{pos=P,cache=B};
drop(_,0,_,C2,P,B)             -> C2#reader{pos=P,cache=B};
drop(A,N,#reader{cache=B,pos=P}=C,C2,_,_) -> drop(A,N-1,?MODULE:A(C),C2,P,B).

take(#reader{cache=[]}=C, _KVS) -> C#reader{args=[]};
take(#reader{dir=D,cache=_B, args=N, pos=P}=C, KVS)  -> take(acc(D), N, C, C, [], P, KVS).
take(_, _, {error,_}, C2, R, P, _KVS) -> C2#reader{args=lists:flatten(R),pos=P,cache={tab(hd(R)),en(hd(R))}};
take(_, 0, _, C2, R, P, _KVS)         -> C2#reader{args=lists:flatten(R),pos=P,cache={tab(hd(R)),en(hd(R))}};
take(A, N, #reader{cache={T,I}, pos=P}=C, C2, R, _, KVS) -> take(A, N-1, ?MODULE:A(C), C2, [element(2,kvs:get(T, I, KVS))|R], P, KVS).


% reader, writer, save
load_reader (Id, KVS) -> case kvs:get(reader, Id, KVS) of {ok, C} -> C; _ -> #reader{id=[]} end.
reader (Id, KVS) -> case kvs:writer(writer, Id, KVS) of
                            {ok, #writer{first=[]}} -> #reader{id=kvs:seq(reader, 1), feed=Id, cache=[]};
                            {ok, #writer{first=F}}  -> #reader{id=kvs:seq(reader, 1), feed=Id, cache={tab(F), id(F)}}
                    end.

ensure(#writer{id=Id}, KVS) -> writer(Id, KVS).
writer(Id, KVS) -> case kvs:get(writer, Id, KVS) of
                     {ok, W} -> W;
                     {error,_} -> W0 = #writer{id = Id},
                                  kvs:save(W0),
                                  W0
                   end. 
feed(Key, KVS) -> R = kvs:reader(Key, KVS),
                   RA = R#reader{args=-1},
                   T = kvs:take(RA),
                   T#reader.args.

save (C, KVS) -> NC = c4(C,[]), kvs:put(NC, KVS), NC.

% add

add(#writer{args=M}=C, KVS) when element(2,M) == [] -> add(si(M, kvs:seq(tab(M), 1, KVS)), C);
add(#writer{args=M, cache=[]}=C, KVS) ->
    _Id=id(M), N=sp(sn(M,[]),[]), kvs:put(N, KVS),
    C#writer{cache=N,count=1,first=N};
add(#writer{args=M, cache=V1, count=S}=C, KVS) ->
    TabId = tab(V1),
    ValueId = id(V1),
    {V, VId} = case kvs:get(TabId, ValueId, KVS) of
            {ok, V0} -> {V0, id(V0)};
            {error,_} -> kvs:writer(TabId), 
                        {V1, ValueId} 
        end,
    N=sp(sn(M,[]), VId), 
    P=sn(V, id(M)), 
    kvs:put([N,P]),
    C#writer{cache=N,count=S+1}.

remove(Rec, Feed, KVS) ->
    % TODO: определить корректно cache (KVS:cache?)
    W = #writer{count=C, cache = I} = kvs:writer(Feed),
    case kvs:delete(Feed, id(Rec), KVS) of
        ok -> Count = C - 1,
              kvs:save(W#writer{count = Count, cache = I}),
              Count;
         _ -> C 
    end.

% remove(Rec,Feed) ->
%    kvs:ensure(#writer{id=Feed}),
%    W = #writer{count=C} = kvs:writer(Feed),
%    {ok,I} = rocksdb:iterator(ref(), []),
%    case kvs:delete(Feed,id(Rec)) of
%         ok -> Count = C - 1,
%               kvs:save(W#writer{count = Count, cache = I}),
%               Count;
%          _ -> C end.

append(Rec, Feed, Modify, KVS) -> 
     Name = element(1,Rec),
     Id = element(2,Rec),
     
     case kvs:get(Name, Id) of
          {ok, _}    -> case Modify of
                              true -> kvs:put(Rec); %raw_append(Rec,Feed), kvs:save(W#writer{cache=Rec,count=W#writer.count + 1});
                              false -> skip
                         end;
          {error,_} ->  W = kvs:writer(Feed, KVS),
                        kvs:save(kvs:add(W#writer{args=Rec,cache=Rec}))
     end,
     Id.   
cut(Feed, Id, #kvs{mod=DBA}) -> DBA:cut(Feed, Id).

% append(Rec, Feed, Modify) -> 
%     Name = element(1,Rec),
%     Id = element(2,Rec),
%     case kvs:get(Name, Id) of
%             {ok, _}    -> case Modify of
%                             true -> kvs:put(Rec);
%                             false -> skip
%                         end;
%             {error,_} ->  W = kvs:writer(Feed), 
%                         kvs:save(kvs:add(W#writer{args=Rec}))
%     end,
%     Id.

%append(Rec,Feed) ->
%   kvs:ensure(#writer{id=Feed}),
%   Name = element(1,Rec),
%   Id = element(2,Rec),
%   case kvs:get(Name,Id) of
%        {ok,_}    -> Id;
%        {error,_} -> kvs:save(kvs:add((kvs:writer(Feed))#writer{args=Rec})), Id end.
%




% sugar
bt(X)      -> bt(X,false).
bt(X, true)      -> binary_to_term(X,[safe]);
bt(X, false)      -> binary_to_term(X).


