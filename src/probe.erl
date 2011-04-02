-module(probe).
-export([probe/2, probe/3]).

probe(_Node, _Sig) ->
    true.

probe(_Node, _Event, _Args) ->
    true.
