%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_fs).
-moduledoc """
A file system based implementation of the S3 API for testing purposes.

Each connection has an associated folder and each key for the connection has an
associated file in that folder.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/file.hrl").

-export([
         init/0,
         open/0,
         close/1,
         get/3,
         get_range/4,
         put/4,
         delete/3
        ]).

% Auxiliary function for thesting
-export([
    get_stream_data/1,
    clear/0,
    set_data_dir/1
]).


-behaviour(rabbitmq_stream_s3_api).

-type connection() :: rabbitmq_stream_s3_api:connection().
-type key() :: rabbitmq_stream_s3_api:key().

-spec init() -> ok.
init() ->
    ?LOG_INFO(?MODULE_STRING ": initializing"),
    case ets:whereis(?MODULE) of
        undefined ->
            ets:new(?MODULE, [named_table, public, set]),
            ok;
        _ ->
            ok
    end.

-doc """
""".
-spec open() -> {ok, connection()} | {error, any()}.
open() ->
    ?LOG_INFO(?MODULE_STRING ": opening connection"),
    {ok, ok}.

-spec close(connection()) -> ok.
close(_Connection) ->
    ?LOG_INFO(?MODULE_STRING ": closing connection"),
    ok.

-spec get(connection(), key(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, binary()} | {error, any()}.
get(_Connection, Key, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        ?LOG_INFO("Trying to find file ~p in : ~p", [Key, data_dir()]),
        case key_to_path(Key) of
            {error, path_not_set} = E ->
                E;
            FilePath ->
                case filelib:wildcard(binary_to_list(FilePath)) of
                    [Filename] ->
                        file:read_file(Filename);
                    [] ->
                        {error, not_found}
                end
        end
    end).

-spec get_range(connection(), key(), rabbitmq_stream_s3_api:range_spec(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(_Connection, Key, RangeSpec, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        case key_to_path(Key) of
            {error, path_not_set} = E ->
                E;
            FilePath ->
                case filelib:wildcard(binary_to_list(FilePath)) of
                    [Filename] ->
                        {ok, #file_info{size=FileSize}} = file:read_file_info(Filename),
                        {ok, Fd} = file:open(Filename, [read, binary]),
                        {Location, Number} = range_spec_to_location_number(FileSize, RangeSpec),
                        Result = case file:pread(Fd, Location, Number) of
                            {ok, Data} ->
                                {ok, Data};
                            eof ->
                                {ok, <<>>}
                        end,
                        file:close(Fd),
                        Result;
                    [] ->
                        {error, not_found}
                end
        end
    end).

-spec put(connection(), key(), iodata(), rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
put(_Connection, Key, Data, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        ?LOG_INFO("Writing file ~p in : ~p", [Key, data_dir()]),
        case key_to_path(Key) of
            {error, path_not_set} = E ->
                E;
            FilePath ->
                filelib:ensure_path(filename:dirname(FilePath)),
                Result = file:write_file(FilePath, Data),
                ?LOG_INFO("Write result: ~p", [Result]),
                Result
        end
    end).

-spec delete(connection(), key() | [key()], rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
delete(Connection, Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    delete(Connection, [Key], Opts);
delete(_Connection, Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        Result = lists:filtermap(
                    fun (K) ->
                        case key_to_path(K) of
                            {error, path_not_set} = E ->
                                {true, {K, E}};
                            FilePath ->
                                case file:delete(FilePath) of
                                    ok -> false;
                                    Error -> {true, {K, Error}}
                                end
                        end
                    end,
                    Keys),
        case Result of
            [] -> ok;
            _ -> {error, Result}
        end
    end).

-spec get_stream_data(StreamName) -> {ok, [FragmentFile]} | {error, not_found} when
      StreamName :: binary(),
      FragmentFile :: binary().
get_stream_data(StreamName0) ->
    StreamNameWildcard = binary_to_list(<<"*", StreamName0/binary, "*">>),
    case filelib:wildcard(string:join([data_dir(), "**", StreamNameWildcard], "/")) of
        [] -> {error, not_found};
        [StreamDir] ->
            Manifest = case filelib:wildcard(string:join([StreamDir, "**", "*manifest"], "/")) of
                [] -> undefined;
                [ManifestFile] -> ManifestFile
            end,
            Fragments = filelib:wildcard(string:join([data_dir(),
                                                      "**",
                                                      StreamNameWildcard,
                                                      "**",
                                                      "*.fragment"],
                                                     "/")),
            {ok, Manifest, Fragments}
    end.

-spec clear() -> ok | {error, any()}.
clear() ->
    file:del_dir_r(data_dir()).

-spec set_data_dir(string()) -> ok.
set_data_dir(DataDir) ->
    application:set_env(rabbitmq_stream_s3, api_fs_data_dir, DataDir).

-spec data_dir() -> binary().
data_dir() ->
    application:get_env(rabbitmq_stream_s3, api_fs_data_dir, undefined).

-spec key_to_path(rabbitmq_stream_s3_api:key()) -> {ok, binary()} | {error, path_not_set}.
key_to_path(Key) ->
    case data_dir() of
        undefined -> {error, path_not_set};
        DataDir ->
            filename:join(DataDir, Key)
    end.

with_timeout(Timeout, Fun) ->
    Self = self(),
    Pid = spawn(fun() -> Self ! {self(), Fun()} end),
    receive
        {Pid, Result} -> Result
    after
        Timeout ->
            ?LOG_INFO(?MODULE_STRING ": operation timeouted"),
            exit(Pid, kill),
            {error, timeout}
    end.

range_spec_to_location_number(FileSize, SuffixRange) when is_integer(SuffixRange), SuffixRange < 0 ->
    Location = FileSize - SuffixRange,
    {Location, -SuffixRange};
range_spec_to_location_number(FileSize, SuffixRange) when is_integer(SuffixRange) ->
    Location = 0,
    Number = min(SuffixRange, FileSize),
    {Location, Number};
range_spec_to_location_number(_FileSize, {StartByte, EndByte}) ->
    Number = case EndByte of
                 undefined -> infinity;
                 _ -> EndByte - StartByte + 1
             end,
    {StartByte, Number}.
