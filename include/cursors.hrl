-ifndef(CURSORS_HRL).
-define(CURSORS_HRL, true).

-record(writer, { id    = [] :: term(),
                  count =  0 :: integer(),
                  cache = [] :: [] | integer() | {term(),term()},
                  args  = [] :: term(),
                  first = [] :: [] | tuple() } ).
-record(reader, { id    = [] :: [] | integer(),
                  pos   =  0 :: integer(),
                  cache = [] :: [] | integer() | {term(),term()},
                  args  = -1 :: term(),
                  feed  = [] :: term(),
                  dir   =  1 :: -1 | 1 } ).
-endif.
