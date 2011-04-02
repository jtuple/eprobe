-module(ep).
-include_lib("eunit/include/eunit.hrl").
-behaviour(gen_server).
-export([new/0, new/1, start/0, stop/0, stop/1, once/3, once/4, history/0, validate/0,
         clear/0, clear/1, wait/0, release/1, check/0, load_expectations/4, check_expectations/1,
         check_expectations/2, put/2, get/2, probe/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).
-export([rpc_test_expect/0]).

-record(expectation, {sig :: [term()],
                      type :: term(),
                      vertex :: term(),
                      func :: term()}).

-record(state, {node_expect :: dict(),
                graph :: digraph(),
                valid :: boolean(),
                history :: [term()],
                test_state :: term(),
                waiting :: [pid()],
                meta :: dict()}).

new() ->
    new(no_state).

new(TState) ->
    SL = gen_server:start_link({local, ?MODULE}, ?MODULE, [TState], []),
    case SL of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid2}} ->
            gen_server:call(Pid2, release),
            gen_server:call(Pid2, {clear, TState}),
            Pid2
    end.

start() ->
    gen_server:call(?MODULE, start).

stop() ->
    gen_server:call(?MODULE, stop).

stop(Nodes) ->
    [gen_server:call({?MODULE, N}, stop) || N <- Nodes].

clear() ->
    gen_server:call(?MODULE, clear).

clear(Nodes) ->
    [gen_server:call({?MODULE, N}, clear) || N <- Nodes].

wait() ->
    fun() -> gen_server:call(?MODULE, wait, infinity) end.

release(Nodes) ->
    {_, []} = gen_server:multi_call(Nodes, ?MODULE, release, 1000),
    ok.

put(Key, Val) ->    
    gen_server:call(?MODULE, {put, Key, Val}).

get(Node, Key) ->
    gen_server:call({?MODULE, Node}, {get, Key}).

init([TState]) ->
    State = clear_state(TState, #state{waiting=[], meta=dict:new()}),
    {ok, State}.

history() ->
    lists:reverse(gen_server:call(?MODULE, history)).

validate() ->
    gen_server:call(?MODULE, validate).

check() ->
    gen_server:call(?MODULE, check).

once(Pred, Node, Sig) ->
    once(Pred, Node, Sig, none).

once(Pred, Node, Sig, Fun) ->
    gen_server:call(?MODULE, {once, Pred, Node, Sig, Fun}).

load_code(Module, Nodes) ->
    {Module, Bin, File} = code:get_object_code(Module),
    {_, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]).
    
load_expectations(Module, ExpFn, Args, Nodes) ->
    [pong = net_adm:ping(N) || N <- Nodes],

    load_code(Module, Nodes),
    load_code(ep, Nodes),
    load_code(meck, Nodes),
    load_code(em, Nodes),

    %%{ResL, []} = rpc:multicall(Nodes, Module, ExpFn, Args, 500),
    Result = rpc:multicall(Nodes, Module, ExpFn, Args, 500),
    ?debugFmt("rpc_result: ~p~n", [Result]),
    {ResL, []} = Result,
    [ok = R || R <- ResL].

%% Load expectations onto local node
%% load_expectations(Module, ExpFn, Args) ->
%%     apply(Module, ExpFn, Args).

check_expectations(Nodes) ->
    {Valid, _Hist} = check_expectations(Nodes, history),
    Valid.

check_expectations(Nodes, history) ->
    [pong = net_adm:ping(N) || N <- Nodes],
    %%{ResL, []} = rpc:multicall(Nodes, ?MODULE, check, [], 500),
    Result = rpc:multicall(Nodes, ?MODULE, check, [], 500),
    ?debugFmt("rpc_result: ~p~n", [Result]),
    {ResL, []} = Result,
    {ValidL, HistL} = lists:unzip(ResL),
    Valid = lists:all(fun(V) -> V end, ValidL),
    ?debugFmt("ValidL: ~p~n", [ValidL]),
    ?debugFmt("HistL:  ~p~n", [HistL]),
    CombinedHist = lists:keysort(1, lists:flatten(HistL)),
    Hist = untagged_history(CombinedHist),
    ?debugFmt("Hist: ~p~n", [Hist]),
    {CValid, CHist} = gen_server:call({?MODULE, hd(Nodes)}, {check_events, CombinedHist}),
    ?debugFmt("Check: ~p~n", [{CValid, CHist}]),
    case Valid and CValid of
        true ->
            {true, CHist};
        false ->
            io:format("~n~p~n", [CHist]),
            {false, CHist}
    end.

%% Local check_expectations
%% check_expectations() ->
%%     Check = check(),
%%     {Valid, HistL} = check(),
%%     CombinedHist = lists:keysort(1, HistL),
%%     Hist = untagged_history(CombinedHist),
%%     ?debugFmt("Hist: ~p~n", [Hist]),
%%     {CValid, CHist} = gen_server:call(?MODULE, {check_events, CombinedHist}),
%%     ?debugFmt("Check: ~p~n", [{CValid, CHist}]),
%%     case Valid and CValid of
%%         true ->
%%             {true, CHist};
%%         false ->
%%             io:format("~n~p~n", [CHist]),
%%             {false, CHist}
%%     end.

check_events(G, Valid, [], Hist2) ->
    Hist3 = lists:reverse(untagged_history(Hist2)),
    %%Hist3 = untagged_history(Hist2),
    case {Valid, digraph:no_vertices(G)} of
        {_, 0} ->
            {Valid, Hist3};
        {false, _} ->
            {Valid, Hist3};
        {true, _} ->
            Missing = [digraph:vertex(G,V) || V <- lists:sort(digraph:vertices(G))],
            {_, MExpect} = lists:unzip(Missing),
            MissHist = [{missing, Node, Sig, failure} || {Node, Sig} <- MExpect],
            {false, Hist3 ++ MissHist}
    end;
check_events(G, Valid, [H|Hs], Hist2) ->
    {Time, Vertex, Event} = H,
    {probe, Node, Sig, _Msg} = Event,
    InDeg = digraph:in_degree(G, Vertex),

    case {Vertex, InDeg} of
        {error, _} ->
            check_events(G, false, Hs, [H | Hist2]);
        {_, 0} ->
            digraph:del_vertex(G, Vertex),
            check_events(G, Valid, Hs, [H | Hist2]);
        {_, _} ->
            digraph:del_vertex(G, Vertex),
            Event2 = {probe, Node, Sig, {failure, {out_of_order}}},
            H2 = {Time, Vertex, Event2},
            check_events(G, false, Hs, [H2 | Hist2])
    end.

clear_state(TState, State=#state{graph=G}) ->
    probing:stop_probing(),
    catch digraph:delete(G),
    State#state{node_expect=dict:new(),
                graph=digraph:new(),
                test_state=TState,
                valid=true,
                history=[]}.

handle_call({get, Key}, _From, State=#state{meta=Meta}) ->
    Val = dict:find(Key, Meta),
    {reply, Val, State};
handle_call({put, Key, Val}, _From, State=#state{meta=Meta}) ->
    Meta2 = dict:store(Key, Val, Meta),
    {reply, ok, State#state{meta=Meta2}};
handle_call(wait, From, State=#state{waiting=W}) ->
    {noreply, State#state{waiting=[From|W]}};
handle_call(release, _From, State=#state{waiting=W}) ->
    [gen_server:reply(N, ok) || N <- W],
    {reply, ok, State#state{waiting=[]}};
handle_call({check_events, Hist}, _From, State=#state{graph=G}) ->
    {Valid, Hist2} = check_events(G, true, Hist, []),
    {reply, {Valid, Hist2}, State};
handle_call(start, _From, State) ->
    probing:start_probing(ep, probe),
    {reply, ok, State};
    %% Pid = self(),
    %% EM = em:new(),
    %% em:stub(EM, probe, probe, [em:any(), em:any()],
    %%         {function,
    %%          fun([N, Sig]) ->
    %%                  case gen_server:call(Pid, {invoke, N, Sig}) of
    %%                      ok ->
    %%                          ok;
    %%                      {ok, none} ->
    %%                          ok;
    %%                      {ok, F} ->
    %%                          F(),
    %%                          ok
    %%                  end
    %%          end}),
    %% em:replay(EM),

    %% meck:new(probe),
    %% meck:expect(probe, probe,
    %%             fun(N, Sig) -> gen_server:call(Pid, {invoke, N, Sig}) end),
    %% {reply, ok, State#state{em=EM}};

handle_call(stop, _From, State=#state{test_state=TState, waiting=W}) ->
    %%meck:unload(probe),
    %% case is_pid(EM) and is_process_alive(EM) of
    %%     true ->
    %%         em:verify(State#state.em);
    %%     false ->
    %%         ok
    %% end,
    [gen_server:reply(N, ok) || N <- W],
    State2 = clear_state(TState, State),
    {stop, normal, ok, State2};

handle_call(clear, _From, State) ->
    {reply, ok, clear_state(State#state.test_state, State)};
handle_call({clear, TState}, _From, State) ->
    %% {reply, ok, #state{node_expect=dict:new(),
    %%                    graph=digraph:new(),
    %%                    valid=true,
    %%                    history=[]}};
    {reply, ok, clear_state(TState, State)};
handle_call({once, Pred, Node, Sig, Fun}, _From,
            State=#state{node_expect=NodeExp, graph=G}) ->
    V = digraph:add_vertex(G),
    digraph:add_vertex(G, V, {Node, Sig}),
    [digraph:add_edge(G, P, V) || P <- Pred],
    Expect = #expectation{sig=Sig, type=once, vertex=V, func=Fun},
    NodeExp2 = dict:append(Node, Expect, NodeExp),
    {reply, V, State#state{node_expect=NodeExp2}};

handle_call({invoke, Node, Sig}, _From,
            State=#state{node_expect=NodeExp, valid=V}) ->
    case V of
        false ->
            State2 = add_history(Node, Sig, error, test_already_failed, State),
            {reply, ok, State2};
        true ->
            case dict:find(Node, NodeExp) of
                error ->
                    State2 = add_history(Node, Sig, error, unexpected_node, State),
                    State3 = invalidate(State2),
                    {reply, ok, State3};
                {ok, Expect} ->
                    check_expect(Node, Sig, Expect, State)
            end
    end;

handle_call(history, _From, State) ->
    Hist = untagged_history(State#state.history),
    {reply, Hist, State};

handle_call(validate, _From, State) ->
    {reply, State#state.valid, State};

handle_call(check, _From, State=#state{valid=V, history=H}) ->
    {reply, {V, H}, State}.

handle_cast(_Msg, S)  -> {noreply, S}.
handle_info(_Info, S) -> {noreply, S}.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

terminate(_Reason, _S) -> ok.

check_expect(Node, Sig, [], State) ->
    fail(Node, Sig, unexpected_event, State);
check_expect(Node, Sig, Exp=[#expectation{sig=ESig}|_], State)
  when is_function(ESig) and is_function(ESig,1) ->
    Result = (catch ESig(Sig)),
    case Result of
        ok ->
            pass(Node, Sig, Exp, State);
        Reason ->
            fail(Node, Sig, Reason, State)
    end;
check_expect(Node, Sig, Exp=[#expectation{sig=ESig}|_], State)
  when is_function(ESig) and is_function(ESig,2) ->
    TState0 = State#state.test_state,
    Result = (catch ESig(Sig, TState0)),
    case Result of
        {ok, TStateN} ->
            State2 = State#state{test_state=TStateN},
            pass(Node, Sig, Exp, State2);
        Reason ->
            fail(Node, Sig, Reason, State)
    end;
check_expect(Node, Sig, Exp=[#expectation{sig=ESig}|_], State) ->
    case Sig of
        ESig ->
            pass(Node, Sig, Exp, State);
        _ ->
            Reason = {failure, {expected, ESig}},
            fail(Node, Sig, Reason, State)
    end.

pass(Node, Sig, [#expectation{vertex=Vtx, func=Fun} | Rest ], State) ->
    State2 = add_history(Node, Sig, Vtx, ok, State),
    NodeExp = dict:store(Node, Rest, State#state.node_expect),
    {reply, {ok, Fun}, State2#state{node_expect=NodeExp}}.

fail(Node, Sig, Reason, State) ->
    State2 = add_history(Node, Sig, error, Reason, State),
    State3 = invalidate(State2),
    {reply, ok, State3}.

invalidate(State) ->
    State#state{valid=false}.

untagged_history(Hist) ->
    [Msg || {_Time, _Vertex, Msg} <- Hist].

add_history(Node, Sig, Vertex, Msg, State=#state{history=Hist}) ->
    Hist2 = [{erlang:now(), Vertex, {probe, Node, Sig, Msg}} | Hist],
    State#state{history=Hist2}.
    
valid_test() ->
    ep:new(),
    ep:once([], n1, initialize),
    ep:once([], n1, start_election),
    ep:once([], n1, {elected, n3}),
    ep:start(),

    probe:probe(n1, initialize),
    probe:probe(n1, start_election),
    probe:probe(n1, {elected, n3}),

    ?debugFmt("validate: ~p~n", [ep:validate()]),
    ?debugFmt("history: ~p~n", [ep:history()]),

    V = ep:validate(),
    ep:stop(),

    ?assert(V).

invalid_test() ->
    ep:new(),
    ep:once([], n1, initialize),
    ep:once([], n1, start_election),
    ep:once([], n1, {elected, n3}),
    ep:start(),

    probe:probe(n1, initialize),
    probe:probe(n1, start_election),

    %% Elect an unexpected node
    probe:probe(n1, {elected, n2}),

    ?debugFmt("validate: ~p~n", [ep:validate()]),
    ?debugFmt("history: ~p~n", [ep:history()]),

    H = ep:history(),
    V = ep:validate(),
    ep:stop(),

    ?assert(V == false),
    
    LH = hd(lists:reverse(H)),
    LH = {probe,n1,{elected,n2},{failure,{expected,{elected,n3}}}}.

missing_test() ->
    ep:new(),
    ep:once([], n1, initialize),
    ep:once([], n1, start_election),
    ep:once([], n1, {elected, n3}),
    ep:start(),

    probe:probe(n1, initialize),
    probe:probe(n1, start_election),

    ?debugFmt("validate: ~p~n", [ep:validate()]),
    ?debugFmt("history: ~p~n", [ep:history()]),

    Check = ep:check_expectations([node()]),
    ep:stop(),
    ?assert(Check == false),
    
    %%LH = hd(lists:reverse(H)),
    %%LH = {probe,n1,{elected,n2},{failure,{expected,{elected,n3}}}}.
    ok.
    
unexpected_node_test() ->
    ep:new(),
    ep:once([], n1, initialize),
    ep:start(),

    %% Probe call from unexpected node
    probe:probe(n2, initialize),

    ?debugFmt("validate: ~p~n", [ep:validate()]),
    ?debugFmt("history: ~p~n", [ep:history()]),

    H = ep:history(),
    V = ep:validate(),
    ep:stop(),

    ?assert(V == false),
    
    LH = hd(lists:reverse(H)),
    LH = {probe,n2,initialize,unexpected_node}.

unexpected_event_test() ->
    ep:new(),
    ep:once([], n1, initialize),
    ep:start(),

    %% Send probe event twice that is expected once
    probe:probe(n1, initialize),
    probe:probe(n1, initialize),

    ?debugFmt("validate: ~p~n", [ep:validate()]),
    ?debugFmt("history: ~p~n", [ep:history()]),

    H = ep:history(),
    V = ep:validate(),
    ep:stop(),

    ?assert(V == false),
    
    LH = hd(lists:reverse(H)),
    LH = {probe,n1,initialize,unexpected_event}.

already_failed_test() ->
    ep:new(),
    ep:once([], n1, initialize),
    ep:start(),

    %% Send probe event three times that is expected once
    probe:probe(n1, initialize),
    probe:probe(n1, initialize),
    probe:probe(n1, initialize),

    ?debugFmt("validate: ~p~n", [ep:validate()]),
    ?debugFmt("history: ~p~n", [ep:history()]),

    H = ep:history(),
    V = ep:validate(),
    ep:stop(),

    ?assert(V == false),
    
    LH = hd(lists:reverse(H)),
    LH = {probe,n1,initialize,test_already_failed}.

out_of_order_test() ->
    ep:new(),

    E1 = ep:once([], n1, start),
    E2 = ep:once([], n2, start),

    ep:once([E1,E2], n1, send),
    ep:start(),

    probe:probe(n1, start),
    probe:probe(n1, send),
    probe:probe(n2, start),

    V = ep:check_expectations([node()]),
    ep:stop(),
    ?assert(V == false).

state_test() ->    
    ep:new(0),
    
    CountCheck = fun([count, I], Prev) ->
                         case I == (Prev + 1) of
                             true ->
                                 {ok, I};
                             false ->
                                 {expected, Prev+1}
                         end
                 end,

    ep:once([], n1, CountCheck),
    ep:once([], n1, CountCheck),
    ep:once([], n1, CountCheck),
    ep:start(),
    
    probe:probe(n1, [count, 1]),
    probe:probe(n1, [count, 2]),
    probe:probe(n1, [count, 0]),

    {V,H} = ep:check_expectations([node()], history),
    ep:stop(),

    ExHist = [{probe,n1,[count,1],ok},
              {probe,n1,[count,2],ok},
              {probe,n1,[count,0],{expected,3}}],

    ?assertEqual(V, false),
    ?assertEqual(H, ExHist).

wait_test() ->
    ep:new(),
    ep:once([], n1, ping1),
    ep:once([], n1, ping2, ep:wait()),
    ep:once([], n1, ping3),
    ep:start(),
    
    spawn(fun() ->
                  probe:probe(n1, ping1),
                  probe:probe(n1, ping2),
                  probe:probe(n1, ping3)
          end),

    timer:sleep(500),
    {V,H} = ep:check_expectations([node()], history),
    ?debugFmt("WaitH: ~p~n", [H]),

    ExHist = [{probe,n1,ping1,ok},
              {probe,n1,ping2,ok},
              {missing,n1,ping3,failure}],

    ?assertEqual(V, false),
    ?assertEqual(H, ExHist),

    ep:release([node()]),

    timer:sleep(500),
    {V2,H2} = ep:check_expectations([node()], history),
    ?debugFmt("WaitH: ~p~n", [H2]),

    ExHist2 = [{probe,n1,ping1,ok},
               {probe,n1,ping2,ok},
               {probe,n1,ping3,ok}],

    ?assertEqual(V2, true),
    ?assertEqual(H2, ExHist2),
    ep:stop().

meta_test() ->    
    ep:new(),

    ep:once([], n1, test,
            fun() -> ep:put(n1, self()) end),

    ep:once([], n2, test,
            fun() -> ep:put(n2, self()) end),

    ep:start(),
    
    Pid1 = spawn(fun() -> probe:probe(n1, test) end),
    Pid2 = spawn(fun() -> probe:probe(n2, test) end),

    timer:sleep(500),
    ?assertMatch({ok, Pid1}, ep:get(node(), n1)),
    ?assertMatch({ok, Pid2}, ep:get(node(), n2)),
    ?assert(ep:check_expectations([node()])),
    ep:stop().

rpc_test_expect() ->
    ep:new(),
    ep:once([], 'dev1@127.0.0.1', initialize),
    ep:once([], 'dev2@127.0.0.1', initialize),
    ep:once([], 'dev3@127.0.0.1', initialize),
    ep:start(),
    ok.
    
rpc_test() ->
    net_kernel:start(['test@127.0.0.1']),
    erlang:set_cookie(node(), riak_zab),
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    ep:load_expectations(?MODULE, rpc_test_expect, [], Nodes),
    ?debugFmt("Done w/ load~n", []),
    ok = rpc:call('dev1@127.0.0.1', probe, probe, ['dev1@127.0.0.1', initialize], 500),
    ok = rpc:call('dev3@127.0.0.1', probe, probe, ['dev3@127.0.0.1', initialize], 500),
    ok = rpc:call('dev2@127.0.0.1', probe, probe, ['dev2@127.0.0.1', initialize], 500),
    ?assert(ep:check_expectations(Nodes)),
    ep:stop(Nodes).

probe(Node, Sig) ->
    case gen_server:call(?MODULE, {invoke, Node, Sig}) of
        ok ->
            ok;
        {ok, none} ->
            ok;
        {ok, Fun} ->
            Fun(),
            ok
    end.
