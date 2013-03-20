-module(werken_utils).
-export([size_or_length/1, generate_job_id/0, generate_worker_id/0, generate_client_id/0, args_to_list/1, list_to_null_list/1]).

size_or_length(Term) when is_binary(Term) ->
  size(Term);

size_or_length(Term) when is_list(Term) ->
  length(Term).

generate_worker_id() ->
  "W:" ++ integer_to_list(random_int()).

generate_client_id() ->
  "C:" ++ integer_to_list(random_int()).

generate_job_id() ->
  "J:" ++ hmac:hexlify(erlsha2:sha224(integer_to_list(random_int()))).

args_to_list(Args) ->
  Parts = binary:split(Args, [<<0>>], [global]),
  Result = lists:map(fun(X) -> binary_to_list(X) end, Parts),
  case Result of 
    [[]] ->
      [];
    Other ->
      Other
  end.

list_to_null_list(List) ->
  L = lists:foldr(
    fun(X, Result) ->
        case Result of
          [] ->
            [X];
          _ ->
            [X|[0|Result]]
        end
    end, [], List),
  list_to_binary(L).

%% internal functions
random_int() ->
  A = erlang:phash2(erlang:now()),
  B = erlang:phash2(crypto:strong_rand_bytes(64)),
  A * B.
