%%--------------------------------------------------------------------
%% Copyright (c) 2018 Wen Jing<wenjing2016@gmail.com>, All Rights Reserved.
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

-module(emq_kafka).

-include("emq_kafka.hrl").

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("ekaf/include/ekaf_definitions.hrl").

-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/4]).

-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

-export([emq_kafka_callback/5]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
    emqttd:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqttd:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqttd:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

ekaf_init(_Env) ->
    % broker 代理服务器的地址
    {ok, BootstrapBroker} = get_bootstrap_broker(),
    % data points 数据流主题及策略
    {ok, DpTopic, DpPartitionStrategy, DpPartitionWorkers} = get_points_topic(),
    % device status 设备状态流主题及策略
    {ok, DsTopic, DsPartitionStrategy, DsPartitionWorkers} = get_status_topic(),

    % Set broker url and port. 设置Kafka代理地址
    %
    % Ideally should be the IP of a load balancer so that any broker can be contacted
    % 要找到一个 load balancer 的 IP 作为地址传入，可以使用apache作为balancer吗？
    %
    % ekaf_bootstrap_broker 只有一个使用场景，即查询metadata，获得kafka partitions集群的的信息。
    % 之后 ekaf_bootstrap_broker 就不会再用到了，之后都是直接连接具体brokers地址。
    % application:set_env(ekaf, ekaf_bootstrap_broker, {"127.0.0.1", 9092}),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),

    % Set topic. 设置主题地址
    % 注释掉，以实现主题 workers 的懒加载。
    % application:set_env(ekaf, ekaf_bootstrap_topics, DpTopic),

    % Set partition strategy. 设置分区策略，默认是random.
    % eg. application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_partition_strategy,
                        [
                         {DpTopic, DpPartitionStrategy},
                         {DsTopic, DsPartitionStrategy},
                         {ekaf_partition_strategy, strict_round_robin}
                         ]),

    % 可以通过此方式改写分区选择
    % application:set_env(ekaf, ekaf_callback_custom_partition_picker, {ekaf_callbacks, default_custom_partition_picker}),

    % batch setting 批量消息设置
    % reach 20, then send batch. 达到20条消息则推送
    % 1 second(s) of inactivity, then send batch. 1秒后批量推送
    application:set_env(ekaf, ekaf_max_buffer_size, 20), % 默认值(default) 100
    application:set_env(ekaf, ekaf_buffer_ttl, 1000), % 默认值(default) 5000

    % the count of partition workers. 分区的worker个数
    % application:set_env(ekaf, ekaf_per_partition_workers, 5), % 默认值(default) 100
    % application:set_env(ekaf, ekaf_per_partition_workers_max, 10), % 默认值(default) 100
    application:set_env(ekaf, ekaf_per_partition_workers,
                         [
                           {DpTopic, DpPartitionWorkers},
                           {DsTopic, DsPartitionWorkers},
                           {ekaf_per_partition_workers, 2}    % for remaining topics
                        ]),
    application:set_env(ekaf, ekaf_per_partition_workers_max,
                         [
                           {DpTopic, DpPartitionWorkers},
                           {DsTopic, DsPartitionWorkers},
                           {ekaf_per_partition_workers_max, 2}    % for remaining topics
                        ]),

    % downtime buffer size. 
    % 当kafka代理不可用时，缓存待发送消息的buffer的大小。默认不会设置，即0
    application:set_env(ekaf, ekaf_max_downtime_buffer_size, 100),

    % intrument settings

    % enable each worker reference to a UDP socket to statsd.
    % or statsd would be a bottleneck when there are too many workers.
    % 使每一个 worker 都关联一个到 statsd 服务的 udp socket，否则，当worker数量多时，statsd会有瓶颈。
    % application:set_env(ekaf, ?EKAF_PUSH_TO_STATSD_ENABLED,  true),
    % set intrument callbacks. 监听各类状态
    application:set_env(ekaf, ?EKAF_CALLBACK_FLUSH_ATOM,  {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_FLUSHED_REPLIED_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_WORKER_DOWN_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_WORKER_UP_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_DOWNTIME_SAVED_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_DOWNTIME_REPLAYED_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_TIME_TO_CONNECT_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_TIME_DOWN_ATOM, {?APP, emq_kafka_callback}),
    application:set_env(ekaf, ?EKAF_CALLBACK_MAX_DOWNTIME_BUFFER_REACHED_ATOM, {?APP, emq_kafka_callback}),

    %% 启动各个子模块，其中kafkamocker会模拟一个kafka代理，
    %% ranch是kafkamocker所有依赖的，提供tcp支持的模块
    %% {ok, _} = application:ensure_all_started(kafkamocker),
    {ok, _} = application:ensure_all_started(gproc),
    %% {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(ekaf),

    ekaf:metadata(DpTopic),
    ekaf:metadata(DsTopic),

    io:format("~p: Init ekaf with ~p~n", [?MODULE, BootstrapBroker]).

on_client_connected(_ConnAck, Client = #mqtt_client{
                        client_id    = ClientId,
                        username     = Username,
                        connected_at = ConnectedAt}, _Env) ->
    io:format("~n~p: client ~s connected~n", [?MODULE, ClientId]),
    Json = mochijson2:encode([
        {type, <<"connected">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, node()},
        {ts, emqttd_time:now_ms(ConnectedAt)}
    ]),
    ok = produce_status(ClientId, Json),
    {ok, Client}.

on_client_disconnected(Reason, _Client = #mqtt_client{
                        client_id    = ClientId,
                        username     = Username,
                        connected_at = ConnectedAt}, _Env) ->
    io:format("~n~p: client ~s disconnected, reason: ~w~n", [?MODULE, ClientId, Reason]),
    Json = mochijson2:encode([
        {type, <<"disconnected">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, node()},
        {reason, Reason},
        {ts, emqttd_time:now_ms(ConnectedAt)}
    ]),
    ok = produce_status(ClientId, Json),
    ok.

on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("~p: client(~s/~s) will subscribe: ~p~n", [?MODULE, Username, ClientId, TopicTable]),
    {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("~p: client(~s/~s) unsubscribe ~p~n", [?MODULE, ClientId, Username, TopicTable]),
    {ok, TopicTable}.

on_session_created(ClientId, Username, _Env) ->
    io:format("~p: session(~s/~s) created.~n", [?MODULE, ClientId, Username]).

on_session_subscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    io:format("~p: session(~s/~s) subscribed: ~p~n", [?MODULE, Username, ClientId, {Topic, Opts}]),
    {ok, {Topic, Opts}}.

on_session_unsubscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    io:format("~p: session(~s/~s) unsubscribed: ~p~n", [?MODULE, Username, ClientId, {Topic, Opts}]),
    ok.

on_session_terminated(ClientId, Username, Reason, _Env) ->
    io:format("~p: session(~s/~s) terminated: ~p.~n", [?MODULE, ClientId, Username, Reason]).

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{
                        from      = {ClientId, Username},
                        pktid     = _PkgId,
                        qos       = QoS,
                        retain    = Retain,
                        dup       = Dup,
                        topic     = Topic,
                        payload   = Payload,
                        timestamp = Timestamp}, _Env) ->
    io:format("~p: publish ~s~n", [?MODULE, emqttd_message:format(Message)]),
    Json = mochijson2:encode([
        {type, <<"published">>},
        {client_id, ClientId},
        {username, Username},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {dup, Dup},
        {retain, Retain},
        {cluster_node, node()},
        {ts, emqttd_time:now_ms(Timestamp)}
    ]),
    ok = produce_points(ClientId, Json),
    {ok, Message}.

on_message_delivered(ClientId, Username, Message, _Env) ->
    io:format("~p: delivered to client(~s/~s): ~s~n", [?MODULE, Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.

on_message_acked(ClientId, Username, Message, _Env) ->
    io:format("~p: client(~s/~s) acked: ~s~n", [?MODULE, Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.

produce_points(ClientId, Json) ->
    Topic = get_points_topic(),
    produce(Topic, ClientId, Json),
    ok.

produce_status(ClientId, Json) ->
    Topic = get_status_topic(),
    produce(Topic, ClientId, Json),
    ok.

produce(TopicInfo, ClientId, Json) ->
    case TopicInfo of
        {ok, Topic, custom, _}->
            ekaf:produce_async_batched(Topic, {ClientId, list_to_binary(Json)}),
            ok;
        {ok, Topic, _, _} ->
            ekaf:produce_async_batched(Topic, list_to_binary(Json)),
            ok
    end.

%% 从配置中获取当前Kafka的初始broker配置
get_bootstrap_broker() ->
    {ok, Values} = application:get_env(?APP, bootstrap_broker),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    {ok, BootstrapBroker}.

get_config_prop_list() ->
    application:get_env(?APP, config).

get_instrument_config() ->
    {ok, Values} = get_config_prop_list(),
    Instrument = proplists:get_value(instrument, Values),
    {ok, Instrument}.

%% 从配置中获取设备数据流主题Points的配置
get_points_topic() ->
    {ok, Values} = application:get_env(?APP, points),
    get_topic(Values).

%% 从配置中获取设备状态流主题Status的配置
get_status_topic() ->
    {ok, Values} = application:get_env(?APP, status),
    get_topic(Values).

get_topic(Values) ->
    Topic = proplists:get_value(topic, Values),
    PartitionStrategy = proplists:get_value(partition_strategy, Values),
    PartitionWorkers = proplists:get_value(partition_workers, Values),
    {ok, Topic, PartitionStrategy, PartitionWorkers}.

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqttd:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/4).

emq_kafka_callback(Event, From, StateName, State, Extra) ->
    case get_instrument_config() of
        {ok, true} -> emq_kafka_callback_real(Event, From, StateName, State, Extra);
        _ -> ok
    end.

emq_kafka_callback_real(Event, _From, _StateName, #ekaf_fsm {
                          topic = Topic,
                          broker = _Broker,
                          partition = PartitionId,
                          last_known_size = BufferLength,
                          cor_id = CorId,
                          leader = Leader } = _State, Extra)->
    Stat = <<Topic/binary,".",  Event/binary, ".broker", (ekaf_utils:itob(Leader))/binary, ".", (ekaf_utils:itob(PartitionId))/binary>>,
    case Event of
        ?EKAF_CALLBACK_FLUSH ->
            % 消息已发送
            io:format("~n ~s ~w",[Stat, BufferLength]),
            %NOTE, if application:set_env(ekaf, ?EKAF_PUSH_TO_STATSD_ENABLED, true),
            % then you can call ekaf_stats:udp_gauge(_State#ekaf_fsm.statsd_socket, Stat)
            % eg: ekaf.events.broker1.0 => 100

            %io:format("~n ~p flush broker~w#~p when size was ~p corid ~p via:~p",[Topic, Leader, PartitionId, BufferLength, CorId, _From]);
            ok;
        ?EKAF_CALLBACK_FLUSHED_REPLIED ->
            % 收到消息的回复
            case Extra of
                {ok, {{replied, _, _}, #produce_response{ cor_id = ReplyCorId }} }->
                    Diff = case (CorId - ReplyCorId  ) of Neg when Neg < 0 -> 0; SomeDiff -> SomeDiff end,
                    FinalStat = <<Stat/binary,".diff">>,
                    io:format("~n~s ~w",[FinalStat, Diff]);
                _ ->
                    ?INFO_MSG("ekaf_fsm callback got ~p some:~p ~nextra:~p",[Event, Extra])
            end;
        ?EKAF_CALLBACK_WORKER_UP ->
            io:format("~n ~s 1",[Stat]),
            %NOTE, if application:set_env(ekaf, ?EKAF_PUSH_TO_STATSD_ENABLED, true),
            % then you can call ekaf_stats:udp_incr(_State#ekaf_fsm.statsd_socket, Stat)
            % eg: ekaf.events.worker_up
            ok;
        ?EKAF_CALLBACK_WORKER_DOWN ->
            io:format("~n ~s 1",[Stat]),
            ok;
        ?EKAF_CALLBACK_TIME_TO_CONNECT ->
            % 每个worker花了多长时间连接服务器，如果与8个分区，每个分区有100个worker，则会在初次连接时有800这样的回调
            case Extra of
                {ok, Micros}->
                    io:format("~n ~s => ~p",[Stat, ekaf_utils:ceiling(Micros/1000)]);
                _ ->
                    ok
            end;
        _ ->
            ?INFO_MSG("ekaf_fsm callback got ~p ~p",[Event, Extra])
    end;

emq_kafka_callback_real(Event, _From, StateName,
                #ekaf_server{ topic = Topic },
                Extra)->
    Stat = <<Topic/binary,".",  Event/binary>>,
    case Event of
        ?EKAF_CALLBACK_DOWNTIME_SAVED ->
            io:format("~n ~s => 1",[Stat]),
            ok;
        ?EKAF_CALLBACK_DOWNTIME_REPLAYED ->
            io:format("~n ~s => 1 during ~p",[Stat, StateName]),
            ok;
        ?EKAF_CALLBACK_TIME_DOWN ->
            case Extra of
                {ok, Micros}->
                    io:format("~n ~s => ~p",[Stat, ekaf_utils:ceiling(Micros/1000)]);
                _ ->
                    ok
            end;
        ?EKAF_CALLBACK_WORKER_DOWN ->
            FinalStat = <<Topic/binary,".mainbroker_unreachable">>,
            io:format("~n ~s => 1",[FinalStat]),
            ok;
        _ ->
            ?INFO_MSG("ekaf_server callback got ~p ~p",[Event, Extra])
    end.
