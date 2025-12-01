%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api).
-export([
    put_object/4, put_object/5,
    get_object/3, get_object/4,
    get_object_with_range/4, get_object_with_range/5,
    get_object_size/3, get_object_size/4,
    get_object_attributes/3,
    get_object_attributes/4,
    get_object_attributes/5
]).

-export([object_path/2]).

-type range_spec() ::
    {StartByte :: non_neg_integer(), EndByte :: non_neg_integer() | undefined}
    | SuffixRange :: integer().

-export_type([range_spec/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-spec put_object(rabbitmq_aws:connection_handle(), binary(), binary(), iodata()) ->
    ok | {error, term()}.
put_object(Handle, Bucket, Key, Object) ->
    put_object(Handle, Bucket, Key, Object, []).

-spec put_object(rabbitmq_aws:connection_handle(), binary(), binary(), iodata(), list()) ->
    ok | {error, term()}.
put_object(Handle, Bucket, Key, Object, Opts) when is_list(Opts) ->
    Path = object_path(Bucket, Key),
    {Headers, Opts1} =
        case proplists:get_value(crc32, Opts, []) of
            [] ->
                {[], Opts};
            Checksum when is_integer(Checksum) ->
                C = base64:encode_to_string(<<Checksum:32/unsigned>>),
                O = proplists:delete(crc32, Opts),
                {[{"x-amz-checksum-crc32", C}], O}
        end,
    case rabbitmq_aws:put(Handle, Path, Object, Headers, Opts1) of
        {ok, {_Headers, <<>>}} ->
            ok;
        Error ->
            {error, Error}
    end.

-spec get_object(rabbitmq_aws:connection_handle(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get_object(Handle, Bucket, Key) ->
    get_object(Handle, Bucket, Key, []).

-spec get_object(rabbitmq_aws:connection_handle(), binary(), binary(), list()) ->
    {ok, binary()} | {error, term()}.
get_object(Handle, Bucket, Key, Opts) ->
    Path = object_path(Bucket, Key),
    case rabbitmq_aws:get(Handle, Path, [], Opts) of
        {ok, {_Headers, Body}} ->
            {ok, Body};
        {error, "Not Found", _} ->
            {error, not_found};
        Error ->
            {error, Error}
    end.

-spec get_object_attributes(rabbitmq_aws:connection_handle(), binary(), binary()) ->
    {ok, [{binary(), binary()}]} | {error, term()}.
get_object_attributes(Handle, Bucket, Key) ->
    get_object_attributes(Handle, Bucket, Key, []).
get_object_attributes(Handle, Bucket, Key, Opts) ->
    get_object_attributes(Handle, Bucket, Key, [], Opts).

-spec get_object_attributes(
    rabbitmq_aws:connection_handle(),
    binary(),
    binary(),
    [binary()],
    [term()]
) ->
    {ok, [{binary(), binary()}]} | {error, term()}.
get_object_attributes(Handle, Bucket, Key, Attributes, Opts) ->
    Path = object_path(Bucket, Key),
    case rabbitmq_aws:request(Handle, head, Path, <<"">>, [], Opts) of
        {ok, {Headers, _Body}} ->
            {ok, filter_attributes(Headers, Attributes)};
        {error, "Not Found", _} ->
            {error, not_found};
        Error ->
            {error, Error}
    end.

-spec get_object_with_range(rabbitmq_aws:connection_handle(), binary(), binary(), range_spec()) ->
    {ok, binary()} | {error, term()}.
get_object_with_range(Handle, Bucket, Key, RangeSpec) ->
    get_object_with_range(Handle, Bucket, Key, RangeSpec, []).

get_object_with_range(Handle, Bucket, Key, RangeSpec0, Opts) ->
    Path = object_path(Bucket, Key),
    RangeValue = range_specifier(RangeSpec0),
    Headers = [{"Range", lists:flatten(["bytes=" | RangeValue])}],
    case rabbitmq_aws:get(Handle, Path, Headers, Opts) of
        {ok, {_Headers, Body}} ->
            {ok, Body};
        {error, "Not Found", _} ->
            {error, not_found};
        Error ->
            {error, Error}
    end.

%% https://www.rfc-editor.org/rfc/rfc9110.html#rule.ranges-specifier
range_specifier({StartByte, undefined}) ->
    io_lib:format("~b-", [StartByte]);
range_specifier({StartByte, EndByte}) ->
    io_lib:format("~b-~b", [StartByte, EndByte]);
range_specifier(SuffixLen) when is_integer(SuffixLen) andalso SuffixLen < 0 ->
    %% ~b will format the '-' for us.
    io_lib:format("~b", [SuffixLen]).

-spec get_object_size(rabbitmq_aws:connection_handle(), binary(), binary()) ->
    {ok, integer()} | {error, term()}.
get_object_size(Handle, Bucket, Key) ->
    get_object_size(Handle, Bucket, Key, []).

-spec get_object_size(rabbitmq_aws:connection_handle(), binary(), binary(), [term()]) ->
    {ok, integer()} | {error, term()}.
get_object_size(Handle, Bucket, Key, Opts) ->
    case get_object_attributes(Handle, Bucket, Key, [<<"content-length">>], Opts) of
        {ok, Attributes} ->
            [{<<"content-length">>, Size}] = Attributes,
            {ok, binary_to_integer(Size)};
        Error ->
            Error
    end.

object_path(Bucket, Key) ->
    BucketStr = ensure_string(Bucket),
    KeyStr = ensure_string(Key),
    {BucketStr, KeyStr}.

ensure_string(Binary) when is_binary(Binary) ->
    binary_to_list(Binary);
ensure_string(List) when is_list(List) ->
    List.

-spec filter_attributes([{binary(), binary()}], [binary()]) ->
    [{binary(), binary()}].
filter_attributes(Headers, []) ->
    Headers;
filter_attributes(Headers, Attributes) ->
    lists:filter(
        fun({Key, _Value}) ->
            lists:member(Key, Attributes)
        end,
        Headers
    ).
