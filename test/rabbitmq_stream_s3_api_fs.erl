%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_fs).
-moduledoc """
A file system based implementation of the S3 API for testing purposes.
""".

-export([
         init/0,
         open/0,
         close/1,
         get/3,
         get_range/4,
         put/4,
         delete/3
        ]).

-behaviour(rabbitmq_stream_s3_api).

-type connection() :: rabbitmq_stream_s3_api:connection().
-type key() :: rabbitmq_stream_s3_api:key().

-spec init() -> ok.
init() ->
    ok.

-doc """
""".
-spec open() -> {ok, connection()} | {error, any()}.
open() ->
    DetsName = string:join([atom_to_list(?MODULE), pid_to_list(self())], "_"),
    dets:open_file(DetsName).

-spec close(connection()) -> ok.
close(Connection) ->
    dets:close(Connection).

-spec get(connection(), key(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, binary()} | {error, any()}.
get(Connection, Key, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Self = self(),
    with_timeout(Timeout, fun() ->
        % timer:sleep(<configurable_timeout>),
        case dets:lookup(Connection, Key) of
            [{Key, Value}] ->
                Self ! {self(), {ok, Value}};
            [] ->
                Self ! {self(), {error, not_found}}
        end
    end).

-spec get_range(connection(), key(), rabbitmq_stream_s3_api:range_spec(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Connection, Key, RangeSpec, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Self = self(),
    with_timeout(Timeout, fun() ->
        % timer:sleep(<configurable_timeout>),
        case dets:lookup(Connection, Key) of
            [{Key, Value}] ->
                try
                    Result = case RangeSpec of
                        {Start, End} when End =/= undefined ->
                            <<_:Start/binary, Slice:(End - Start + 1)/binary, _/binary>> = Value,
                            Slice;
                        {Start, undefined} ->
                            <<_:Start/binary, Slice/binary>> = Value,
                            Slice;
                        Suffix when Suffix < 0 ->
                            <<_:(byte_size(Value) + Suffix)/binary, Slice/binary>> = Value,
                            Slice;
                        Suffix when Suffix >= 0 ->
                            <<Slice:(Suffix)/binary, _/binary>> = Value,
                            Slice
                    end,
                    Self ! {self(), {ok, Result}}
                catch
                    error:badmatch ->
                        Self ! {self(), {error, invalid_range}}
                end;
            [] ->
                Self ! {self(), {error, not_found}}
        end
    end).

-spec put(connection(), key(), iodata(), rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
put(Connection, Key, Data, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Self = self(),
    with_timeout(Timeout, fun() ->
        % timer:sleep(<configurable_timeout>),
        Result = dets:insert(Connection, {Key, iolist_to_binary(Data)}),
        Self ! {self(), Result}
    end).

-spec delete(connection(), key() | [key()], rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
delete(Connection, Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    delete(Connection, [Key], Opts);
delete(Connection, Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Self = self(),
    with_timeout(Timeout, fun() ->
        % timer:sleep(<configurable_timeout>),
        Result = lists:filtermap(fun (K) ->
                   case dets:delete(Connection, K) of
                       ok -> false;
                       Error -> {true, {K, Error}}
                   end
                          end,
                 Keys),
        case Result of
            [] -> Self ! {self(), ok};
            _ -> Self ! {self(), {error, Result}}
        end
    end).

with_timeout(Fun, Timeout) ->
    Pid = spawn(Fun),
    receive
        {Pid, Result} -> Result
    after
        Timeout ->
            exit(Pid, kill),
            {error, timout}
    end.
