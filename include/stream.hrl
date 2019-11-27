-ifndef(STREAM_HRL).
-define(STREAM_HRL, true).
-include("kvs.hrl").
-include("cursors.hrl").
-define(STREAM, [top/1, bot/1, next/1, prev/1, drop/1, take/1, append/2, append/3, cut/2,
                 load_reader/1, writer/1, ensure/1, reader/1, save/1, add/1, feed/1, remove/2]).
-spec top(#reader{}) -> #reader{}.
-spec bot(#reader{}) -> #reader{}.
-spec next(#reader{}) -> #reader{} | {error,not_found | empty}.
-spec prev(#reader{}) -> #reader{} | {error,not_found | empty}.
-spec drop(#reader{}) -> #reader{}.
-spec take(#reader{}) -> #reader{}.
-spec load_reader (term()) -> #reader{}.
-spec writer (term()) -> #writer{}.
-spec reader (term()) -> #reader{}.
-spec feed(term()) -> list().
-spec save (#reader{} | #writer{}) -> #reader{} | #writer{}.
-spec add(#writer{}) -> #writer{}.
-spec append(tuple(),term()) -> any().
-spec remove(tuple(),term()) -> integer().
-spec cut(term(),term()) -> {ok,non_neg_integer()} | {error, not_found}.
-endif.
