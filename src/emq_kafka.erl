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

-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/4]).

-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

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
    %% Get parameters 从配置中读取参数配置
    {ok, Values} = application:get_env(?APP, values),
    %% broker 代理服务器的地址
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    %% data points 数据流主题及策略
    {ok, DpTopic, DpPartitionStrategy} = get_data_points_topic(Values),
    %% device status 设备状态流主题及策略
    {ok, DsTopic, DsPartitionStrategy} = get_device_status_topic(Values),

    %% Set broker url and port. 设置Kafka代理地址
    %% application:set_env(ekaf, ekaf_bootstrap_broker, {"127.0.0.1", 9092}),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),

    %% Set topic. 设置主题地址
    %% application:set_env(ekaf, ekaf_bootstrap_topics, DpTopic),

    %% Set partition strategy. 设置分区策略
    %% eg. application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_partition_strategy,
                        [
                         {DpTopic, DpPartitionStrategy},
                         {DsTopic, DsPartitionStrategy},
                         {ekaf_partition_strategy, strict_round_robin}
                         ]),

    %% 启动各个子模块，其中kafkamocker会模拟一个kafka代理，
    %% ranch是kafkamocker所有依赖的，提供tcp支持的模块
    {ok, _} = application:ensure_all_started(kafkamocker),
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Init ekaf with ~p~n", [BootstrapBroker]).

%% 从配置中获取当前Kafka的设备数据流主题
get_data_points_topic() ->
    {ok, Values} = application:get_env(?APP, values),
    get_data_points_topic(Values).

%% 获取设备数据流主题
get_data_points_topic(Values) ->
    Topic= proplists:get_value(data_points_topic, Values),
    PartitionStrategy= proplists:get_value(data_points_partition_strategy, Values),
    {ok, Topic, PartitionStrategy}.

%% 从配置中获取当前Kafka的设备状态流主题
get_device_status_topic() ->
    {ok, Values} = application:get_env(?APP, values),
    get_device_status_topic(Values).

%% 获取设备状态流主题
get_device_status_topic(Values) ->
    Topic= proplists:get_value(device_status_topic, Values),
    PartitionStrategy= proplists:get_value(device_status_partition_strategy, Values),
    {ok, Topic, PartitionStrategy}.

on_client_connected(_ConnAck, Client = #mqtt_client{
                        client_id    = ClientId,
                        username     = Username,
                        connected_at = ConnectedAt}, _Env) ->
    Json = mochijson2:encode([
        {type, <<"connected">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs(ConnectedAt)}
    ]),
    {ok, ProduceTopic, _} = get_device_status_topic(),
    ekaf:produce_async(ProduceTopic, {ClientId, list_to_binary(Json)}),
    {ok, Client}.

on_client_disconnected(Reason, _Client = #mqtt_client{
                        client_id    = ClientId,
                        username     = Username,
                        connected_at = ConnectedAt}, _Env) ->
    io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),
        Json = mochijson2:encode([
        {type, <<"disconnected">>},
        {client_id, ClientId},
        {username, Username},
        {cluster_node, node()},
        {reason, Reason},
        {ts, emqttd_time:now_to_secs(ConnectedAt)}
    ]),
    {ok, ProduceTopic, _} = get_device_status_topic(),
    ekaf:produce_async(ProduceTopic, {ClientId, list_to_binary(Json)}),
    ok.

on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("client(~s/~s) will subscribe: ~p~n", [Username, ClientId, TopicTable]),
    {ok, TopicTable}.

on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("client(~s/~s) unsubscribe ~p~n", [ClientId, Username, TopicTable]),
    {ok, TopicTable}.

on_session_created(ClientId, Username, _Env) ->
    io:format("session(~s/~s) created.", [ClientId, Username]).

on_session_subscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    io:format("session(~s/~s) subscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
    {ok, {Topic, Opts}}.

on_session_unsubscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    io:format("session(~s/~s) unsubscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
    ok.

on_session_terminated(ClientId, Username, Reason, _Env) ->
    io:format("session(~s/~s) terminated: ~p.", [ClientId, Username, Reason]).

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{
                        from      = From,
                        pktid     = _PkgId,
                        qos       = QoS,
                        retain    = _Retain,
                        dup       = _Dup,
                        topic     = Topic,
                        payload   = Payload,
                        timestamp = Timestamp}, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    Json = mochijson2:encode([
        {type, <<"published">>},
        {client_id, From},
        {topic, Topic},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()},
        {ts, emqttd_time:now_to_secs(Timestamp)}
    ]),
    {ok, ProduceTopic, _} = get_data_points_topic(),
    ekaf:produce_async(ProduceTopic, {From, list_to_binary(Json)}),
    {ok, Message}.

on_message_delivered(ClientId, Username, Message, _Env) ->
    io:format("delivered to client(~s/~s): ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.

on_message_acked(ClientId, Username, Message, _Env) ->
    io:format("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.

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
