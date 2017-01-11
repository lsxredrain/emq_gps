%%--------------------------------------------------------------------
%% Copyright (c) 2015-2016 Feng Lee <feng@emqtt.io>.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Gps Client Connection
-module(emq_gps_client).

-behaviour(gen_server).

-include("emq_gps.hrl").

-include_lib("emqttd/include/emqttd_internal.hrl").

%% API Function Exports
-export([start_link/2, info/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(gps_client, {connection, connname, peername, peerhost, peerport,
                       await_recv, conn_state, rate_limit, parser_fun,
                       proto_state, proto_env, heartbeat}).

-define(INFO_KEYS, [peername, peerhost, peerport, await_recv, conn_state]).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).

-define(LOG(Level, Format, Args, State),
            lager:Level("Gps(~s): " ++ Format, [State#gps_client.connname | Args])).

start_link(Connection, ProtoEnv) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Connection, ProtoEnv]])}.

info(CPid) ->
    gen_server:call(CPid, info, infinity).

init([Connection0, ProtoEnv]) ->
    process_flag(trap_exit, true),
    {ok, Connection} = Connection0:wait(),
    {PeerHost, PeerPort, PeerName} =
    case Connection:peername() of
        {ok, Peer = {Host, Port}} ->
            {Host, Port, Peer};
        {error, enotconn} ->
            Connection:fast_close(),
            exit(normal);
        {error, Reason} ->
            Connection:fast_close(),
            exit({shutdown, Reason})
    end,
    ConnName = esockd_net:format(PeerName),
    Self = self(),
    SendFun = fun(Data) ->
        try Connection:async_send(Data) of
            true -> ok
        catch
            error:Error -> Self ! {shutdown, Error}
        end
    end,
    ParserFun = emq_gps_frame:parser(ProtoEnv),
    ProtoState = emq_gps_proto:init(PeerName, SendFun, ProtoEnv),
    RateLimit = proplists:get_value(rate_limit, Connection:opts()),
    State = run_socket(#gps_client{connection   = Connection,
                                     connname     = ConnName,
                                     peername     = PeerName,
                                     peerhost     = PeerHost,
                                     peerport     = PeerPort,
                                     await_recv   = false,
                                     conn_state   = running,
                                     rate_limit   = RateLimit,
                                     parser_fun   = ParserFun,
                                     proto_env    = ProtoEnv,
                                     proto_state  = ProtoState}),
    gen_server:enter_loop(?MODULE, [], State, 10000).

handle_call(info, _From, State = #gps_client{connection  = Connection,
                                               proto_state = ProtoState}) ->

    ClientInfo = ?record_to_proplist(gps_client, State, ?INFO_KEYS),
    ProtoInfo  = emq_gps_proto:info(ProtoState),
    {ok, SockStats} = Connection:getstat(?SOCK_STATS),
    {noreply, lists:append([ClientInfo, [{proto_info, ProtoInfo},
                                         {sock_stats, SockStats}]]), State};

handle_call(Req, _From, State) ->
    ?LOG(critical, "Unexpected request: ~p", [Req], State),
    
    {reply, {error, unsupported_request}, State}.

handle_cast(Msg, State) ->
    ?LOG(critical, "Unexpected msg: ~p", [Msg], State),
    noreply(State).

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, Error}, State) ->
    shutdown(Error, State);

handle_info({transaction, {timeout, Id}}, State) ->
    emq_gps_transaction:timeout(Id),
    noreply(State);

handle_info({heartbeat, start, {Cx, Cy}}, State = #gps_client{connection = Connection}) ->
    Self = self(),
    Incomming = {Cx, statfun(recv_oct, State), fun() -> Self ! {heartbeat, timeout} end},
    Outgoing  = {Cy, statfun(send_oct, State), fun() -> Connection:send(<<$\n>>) end},
    {ok, HbProc} = emq_gps_heartbeat:start_link(Incomming, Outgoing),
    noreply(State#gps_client{heartbeat = HbProc});

handle_info({heartbeat, timeout}, State) ->
    stop({shutdown, heartbeat_timeout}, State);

handle_info({'EXIT', HbProc, Error}, State = #gps_client{heartbeat = HbProc}) ->
    stop(Error, State);

handle_info(activate_sock, State) ->
    noreply(run_socket(State#gps_client{conn_state = running}));

handle_info({inet_async, _Sock, _Ref, {ok, Bytes}}, State) ->
    ?LOG(debug, "RECV ~p", [Bytes], State),
    received(Bytes, rate_limit(size(Bytes), State#gps_client{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    noreply(State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({dispatch, _Topic, Msg}, State = #gps_client{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emq_gps_proto:send(Msg, ProtoState),
    noreply(State#gps_client{proto_state = ProtoState1});

handle_info(Info, State) ->
    ?LOG(critical, "Unexpected info: ~p", [Info], State),
    noreply(State).

terminate(Reason, State = #gps_client{connection  = Connection,
                                        proto_state = ProtoState}) ->
    ?LOG(info, "terminated for ~p", [Reason], State),
    io:format("terminated for ~p", [Reason]),
    Connection:fast_close(),
    case {ProtoState, Reason} of
        {undefined, _} ->
            ok;
        {_, {shutdown, Error}} -> 
            emq_gps_proto:shutdown(Error, ProtoState);
        {_,  Reason} ->
            emq_gps_proto:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% receive and parse tcp data 
%%------------------------------------------------------------------------------

received(<<>>, State) ->
    noreply(State);

received(Bytes, State = #gps_client{parser_fun  = ParserFun,
                                      proto_state = ProtoState}) ->
    case catch ParserFun(Bytes) of
        {more, NewParser} ->
            noreply(run_socket(State#gps_client{parser_fun = NewParser}));
        {ok, Frame, Rest} ->
            ?LOG(info, "RECV Frame: ~s", [emq_gps_frame:format(Frame)], State),
            case emq_gps_proto:received(Frame, ProtoState) of
                {ok, ProtoState1}           ->
                    received(Rest, reset_parser(State#gps_client{proto_state = ProtoState1}));
                {error, Error, ProtoState1} ->
                    shutdown(Error, State#gps_client{proto_state = ProtoState1});
                {stop, Reason, ProtoState1} ->
                    stop(Reason, State#gps_client{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?LOG(error, "Framing error - ~s", [Error], State),
            ?LOG(error, "Bytes: ~p", [Bytes], State),
            shutdown(frame_error, State);
        {'EXIT', Reason} ->
            ?LOG(error, "Parser failed for ~p", [Reason], State),
            ?LOG(error, "Error data: ~p", [Bytes], State),
            shutdown(parser_error, State)
    end.

reset_parser(State = #gps_client{proto_env = ProtoEnv}) ->
    State#gps_client{parser_fun = emq_gps_frame:parser(ProtoEnv)}.

rate_limit(_Size, State = #gps_client{rate_limit = undefined}) ->
    run_socket(State);
rate_limit(Size, State = #gps_client{rate_limit = Rl}) ->
    case Rl:check(Size) of
        {0, Rl1} ->
            run_socket(State#gps_client{conn_state = running,
                                          rate_limit = Rl1});
        {Pause, Rl1} ->
            ?LOG(error, "Rate limiter pause for ~p", [Pause], State),
            erlang:send_after(Pause, self(), activate_sock),
            State#gps_client{conn_state = blocked, rate_limit = Rl1}    
    end.

run_socket(State = #gps_client{conn_state = blocked}) ->
    State;
run_socket(State = #gps_client{await_recv = true}) ->
    State;
run_socket(State = #gps_client{connection = Connection}) ->
    Connection:async_recv(0, infinity),
    State#gps_client{await_recv = true}.

statfun(Stat, #gps_client{connection = Connection}) ->
    fun() ->
        case Connection:getstat([Stat]) of
            {ok, [{Stat, Val}]} -> {ok, Val};
            {error, Error}      -> {error, Error}
        end
    end.

noreply(State) ->
    {noreply, State, hibernate}.

stop(Reason, State) ->
    {stop, Reason, State}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

