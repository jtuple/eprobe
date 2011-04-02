-module(probing).
-export([start_probing/2, stop_probing/0]).

start_probing(Mod, Fun) ->
    install_probe(Mod, Fun).

stop_probing() ->
    code:purge(probe),
    code:delete(probe),
    code:purge(probe).

install_probe(Mod, Fun) ->
    %% Forms correspond to the following code:
    %%
    %% probe(Node, Sig) ->
    %%     Mod:Fun(Node, Sig).
    %%
    %% probe(Node, Event, Args) ->
    %%     Mod:Fun(Node, Event, Args).

    Code = [{attribute,1,file,{"probe.erl",1}},
            {attribute,1,module,probe},
            {attribute,2,export,[{probe,2},{probe,3}]},
            {function,4,probe,2,
             [{clause,4,
               [{var,4,'Node'},{var,4,'Sig'}],
               [],
               [{call,6,
                 {remote,6,{atom,6,Mod},{atom,6,Fun}},
                 [{var,6,'Node'},{var,6,'Sig'}]}]}]},
            {function,14,probe,3,
             [{clause,14,
               [{var,14,'Node'},{var,14,'Event'},{var,14,'Args'}],
               [],
               [{call,15,
                 {remote,15,{atom,15,Mod},{atom,15,Fun}},
                 [{var,15,'Node'},
                  {var,15,'Event'},
                  {var,15,'Args'}]}]}]},
            {eof,16}],
    code:purge(probe),
    Opts = [verbose, report_errors],
    case compile:forms(Code, Opts) of
        {ok, ModName, Binary} ->
            code:load_binary(ModName, "probe.erl", Binary);
        {ok, ModName, Binary, _Warnings} ->
            code:load_binary(ModName, "probe.erl", Binary)
    end,
    ok.

