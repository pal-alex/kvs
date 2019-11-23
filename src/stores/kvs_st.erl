-module(kvs_st).
-description('KVS STREAM NATIVE ROCKS').
-include("kvs.hrl").
-include("stream.hrl").
-include("metainfo.hrl").
-compile(export_all).
% -export(?STREAM).
% -export([prev/8, append/3]).



ref() -> kvs_rocks:ref().

% section: kvs_stream prelude

se(X,Y,Z)  -> setelement(X,Y,Z).
e(X,Y)  -> element(X,Y).
c3(R,V) -> se(#reader.cache, R, V).
c4(R,V) -> se(#reader.args,  R, V).
si(M,T) -> se(#it.id, M, T).
id(T)   -> e(#it.id, T).

% section: next, prev

top  (#reader{}=C) -> C.
bot  (#reader{}=C) -> C.

next (#reader{cache=[]}) -> {error,empty};
next (#reader{cache=I}=C) ->
   case rocksdb:iterator_move(I, next) of
        {ok,_,Bin} -> C#reader{cache=binary_to_term(Bin,[safe])};
            {error,Reason} -> {error,Reason} end.

prev (#reader{cache=[]}) -> {error,empty};
prev (#reader{cache=I}=C) ->
   case rocksdb:iterator_move(I, prev) of
        {ok,_,Bin} -> C#reader{cache=binary_to_term(Bin,[safe])};
            {error,Reason} -> {error,Reason} end.

% section: take, drop

drop(#reader{args=N}) when N < 0 -> #reader{};

drop(#reader{args=N,feed=Feed,cache=I}=C) when N == 0 ->
   Key = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
   case rocksdb:iterator_move(I, {seek,Key}) of
        {ok,_,Bin} -> C#reader{cache=binary_to_term(Bin,[safe])};
                 _ -> C#reader{cache=[]} end;

drop(#reader{args=N,feed=Feed,cache=I}=C) when N > 0 ->
   Key   = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
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
   C#reader{cache=binary_to_term(element(1,element(2,Term)))}.

take(#reader{args=N,feed=Feed,cache=I}=C) ->
   Key = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
   Fir = rocksdb:iterator_move(I, {seek,Key}),
   Res = kvs_rocks:next(I,Key,size(Key),Fir,[],[],N,0),
   C#reader{args=Res}.

% new, save, load, up, down, top, bot

load_reader (Id) ->
    case kvs:get(reader,Id) of
         {ok,#reader{}=C} -> C#reader{cache=element(2,rocksdb:iterator(ref(),[]))};
              _ -> #reader{id=[]} end.

writer (Id) -> case kvs:get(writer,Id) of {ok,W} -> W; {error,_} -> #writer{id=Id} end.
get_writer(Id) -> case kvs:get(writer, Id) of
                     {ok, W} -> W;
                     {error,_} -> W0 = #writer{id = Id},
                                  kvs:save(W0),
                                  W0
                 end. 

reader (Id) ->
    case kvs:get(writer,Id) of
         {ok,#writer{}} ->
             {ok,I} = rocksdb:iterator(ref(), []),
             #reader{id=kvs:seq(),feed=Id,cache=I};
         {error,_} -> #reader{} end.
save (C) -> NC = c4(C,[]), N2 = c3(NC,[]), kvs:put(N2), N2.

feed(Key) -> kvs:all(Key).

% add

add(#writer{args=M}=C) when element(2,M) == [] -> add(si(M,kvs:seq([],[])),C);
add(#writer{args=M}=C) -> add(M,C).

add(M,#writer{id=Feed,count=S}=C) -> NS=S+1,
    rocksdb:put(ref(),
       <<(list_to_binary(lists:concat(["/",kvs_rocks:format(Feed),"/"])))/binary,
         (term_to_binary(id(M)))/binary>>, term_to_binary(M), [{sync,true}]),
    C#writer{cache=M,count=NS}.

append(Rec,Feed) -> append(Rec, Feed, false).

append(Rec, Feed, Modify) -> 
     Name = element(1,Rec),
     Id = element(2,Rec),
     case kvs:get(Name, Id) of
          {ok, _}    -> case Modify of
                              true -> kvs:put(Rec);
                              false -> skip
                         end;
          {error,_} ->  W = kvs:get_writer(Feed), 
                         kvs:save(kvs:add(W#writer{args=Rec}))
     end,
     Id.

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
    Key    = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed),"/"])),
    A      = <<Key/binary,(term_to_binary(Id))/binary>>,
    {ok,I} = rocksdb:iterator(ref(), []),
    case rocksdb:iterator_move(I, {seek,A}) of
         {ok,A,X} -> {ok,prev(I,Key,size(Key),A,X,[],-1,0)};
                _ -> {error,not_found} end.
